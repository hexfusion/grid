//! Manages grid enrollment from the command line.
//!
//! Two audiences. A provider joining a grid runs `enroll`, which generates a key
//! that never leaves the machine, submits a request for it, and collects the
//! certificate once an operator has approved. An operator runs `requests` to see
//! what is waiting and to decide.

#![expect(clippy::print_stdout, reason = "gridctl is a CLI; its output is the product")]

use std::{
    path::{Path, PathBuf},
    time::Duration,
};

use base64::Engine as _;
use clap::{Parser, Subcommand};
use http_body_util::{BodyExt as _, Full};
use hyper::{Method, Request, body::Bytes};
use hyper_util::{client::legacy::Client as HyperClient, rt::TokioExecutor};
use serde_json::{Map, Value, json};

/// Anything that went wrong, reported to the operator as text.
type Failure = Box<dyn std::error::Error>;

/// Manage enrollment in an AI grid.
#[derive(Debug, Parser)]
#[command(name = "gridctl", version, about = "Manage enrollment in an AI grid")]
struct Cli {
    /// Base URL of the grid's enrollment service.
    #[arg(
        long,
        global = true,
        default_value = "http://127.0.0.1:8080",
        env = "GRID_ENROLLMENT_URL"
    )]
    server: String,

    /// Operator token, needed to list or decide.
    #[arg(long, global = true, env = "GRID_OPERATOR_TOKEN")]
    token: Option<String>,

    /// What to do.
    #[command(subcommand)]
    command: Command,
}

/// What gridctl was asked to do.
#[derive(Debug, Subcommand)]
enum Command {
    /// Ask to join a grid, and collect the certificate once approved.
    Enroll(EnrollArgs),

    /// Inspect and decide on enrollments. Requires an operator token.
    ///
    /// Aliased to `csr` for anyone reaching for `kubectl get csr`, though an
    /// enrollment carries more than the request it was asked with.
    #[command(subcommand, alias = "csr", alias = "enrollments")]
    Enrollment(EnrollmentCommand),
}

/// How to ask for membership.
#[derive(Debug, Parser)]
struct EnrollArgs {
    /// The name to ask for.
    #[arg(long)]
    site: String,

    /// The grid being joined.
    #[arg(long)]
    grid: String,

    /// Host and port peers should reach this provider on.
    #[arg(long)]
    address: String,

    /// A model this provider serves, as `name=path`. Repeatable.
    #[arg(long = "model", value_parser = parse_model)]
    models: Vec<(String, String)>,

    /// Request and response shape the models speak.
    #[arg(long, default_value = "openai-chat")]
    api_format: String,

    /// Where to write the key, and the certificate once issued.
    #[arg(long, default_value = ".")]
    out: PathBuf,

    /// Wait for a decision rather than returning after submitting.
    #[arg(long)]
    wait: bool,

    /// How long to wait for a decision.
    #[arg(long, default_value = "300", requires = "wait")]
    timeout_secs: u64,
}

/// Operator actions on enrollments.
#[derive(Debug, Subcommand)]
enum EnrollmentCommand {
    /// List enrollments, newest first.
    List {
        /// Show only requests in this phase.
        #[arg(long)]
        phase: Option<String>,
    },

    /// Approve an enrollment and issue its certificate.
    Approve {
        /// The enrollment to approve.
        request_id: String,
    },

    /// Deny an enrollment.
    Deny {
        /// The enrollment to deny.
        request_id: String,

        /// Recorded as the reason.
        #[arg(long)]
        reason: Option<String>,
    },
}

/// A string field of a record, or a dash when it is absent.
fn field<'record>(record: &'record Value, name: &str) -> &'record str {
    record.get(name).and_then(Value::as_str).unwrap_or("-")
}

/// Parse a `name=path` model argument.
fn parse_model(raw: &str) -> Result<(String, String), String> {
    raw.split_once('=')
        .map(|(name, path)| (name.to_owned(), path.to_owned()))
        .ok_or_else(|| format!("expected name=path, got {raw}"))
}

#[tokio::main]
async fn main() -> Result<(), Failure> {
    let cli = Cli::parse();
    let client = Client::new(&cli.server, cli.token);

    match cli.command {
        Command::Enroll(args) => enroll(&client, &args).await,
        Command::Enrollment(EnrollmentCommand::List { phase }) => list(&client, phase.as_deref()).await,
        Command::Enrollment(EnrollmentCommand::Approve { request_id }) => approve(&client, &request_id).await,
        Command::Enrollment(EnrollmentCommand::Deny { request_id, reason }) => {
            deny(&client, &request_id, reason.as_deref()).await
        },
    }
}

/// Generate a key, ask for a name, and collect the certificate.
async fn enroll(client: &Client, args: &EnrollArgs) -> Result<(), Failure> {
    let key = rcgen::KeyPair::generate()?;
    let key_path = args.out.join(format!("{}-key.pem", args.site));
    write_private(&key_path, &key.serialize_pem())?;
    println!("wrote {} (keep this; it is never sent)", key_path.display());

    let body = submission(args, &key)?;
    let created = client.send(Method::POST, "/v1/requests", Some(body), false).await?;
    let request_id = field(&created, "requestId").to_owned();
    println!("submitted request {request_id} for site {}", args.site);

    if !args.wait {
        println!("an operator must approve it: gridctl enrollment approve {request_id}");
        return Ok(());
    }

    let timeout = Duration::from_secs(args.timeout_secs);
    let record = poll_until_decided(client, &request_id, timeout).await?;
    collect(client, &record, args, &key).await
}

/// Build the submission body.
///
/// The names in the request are advisory. The grid rebuilds every name on the
/// certificate from what it decided to grant, so this asks rather than asserts.
fn submission(args: &EnrollArgs, key: &rcgen::KeyPair) -> Result<Value, Failure> {
    let mut params = rcgen::CertificateParams::default();
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, args.site.clone());
    let csr = params.serialize_request(key)?.pem()?;

    let mut body = Map::new();
    body.insert("siteName".to_owned(), json!(args.site));
    body.insert("gridNetworkRef".to_owned(), json!(args.grid));
    body.insert("csr".to_owned(), json!(csr));
    body.insert("egress".to_owned(), json!({"address": args.address}));

    if !args.models.is_empty() {
        let models: Vec<Value> = args
            .models
            .iter()
            .map(|(name, path)| json!({"name": name, "path": path, "apiFormat": args.api_format}))
            .collect();
        body.insert("capabilities".to_owned(), json!({"inference": {"models": models}}));
    }

    Ok(Value::Object(body))
}

/// Collect the joining kit, or explain why there is none.
async fn collect(client: &Client, record: &Value, args: &EnrollArgs, key: &rcgen::KeyPair) -> Result<(), Failure> {
    match record.get("phase").and_then(Value::as_str) {
        Some("issued") => {},
        Some("denied") => return Err(format!("denied: {}", field(record, "reason")).into()),
        other => return Err(format!("request ended in phase {}", other.unwrap_or("unknown")).into()),
    }

    println!("approved as {}", field(record, "spiffeId"));

    let request_id = field(record, "requestId");
    let kit = client.collect_kit(request_id, key).await?;
    write_kit(&kit, args)
}

/// Write the joining kit where a proxy can be pointed at it.
///
/// A certificate on its own is not enough to join: a member also has to verify
/// its peers and reach the mesh. All of it lands in one directory so the next
/// step is configuration rather than assembly.
fn write_kit(kit: &Value, args: &EnrollArgs) -> Result<(), Failure> {
    let dir = args.out.join(format!("{}-grid", args.site));
    std::fs::create_dir_all(&dir)?;

    std::fs::write(dir.join("tls.crt"), field(kit, "certificate"))?;
    std::fs::write(dir.join("ca.crt"), field(kit, "caBundle"))?;

    let seeds: Vec<&str> = kit
        .get("seeds")
        .and_then(Value::as_array)
        .map(|seeds| seeds.iter().filter_map(Value::as_str).collect())
        .unwrap_or_default();
    std::fs::write(dir.join("seeds"), format!("{}\n", seeds.join("\n")))?;

    match kit.get("gossipKey").and_then(Value::as_str) {
        Some(gossip_key) => write_private(&dir.join("gossip.key"), gossip_key)?,
        None => println!("warning: the grid returned no gossip key, so this site cannot join the mesh"),
    }

    println!("wrote {}/", dir.display());
    println!("  tls.crt     this site's certificate");
    println!("  ca.crt      the grid CA, for verifying peers");
    println!("  gossip.key  shared transport key");
    println!("  seeds       peers to announce to");
    println!();
    println!("point the grid operator at it:");
    println!("  GRID_SITE_CERT_PATH={}/tls.crt", dir.display());
    println!("  GRID_CA_CERT_PATH={}/ca.crt", dir.display());
    println!(
        "  GRID_SITE_KEY_PATH={}",
        args.out.join(format!("{}-key.pem", args.site)).display()
    );
    Ok(())
}

/// Poll one request until it leaves the pending phase.
async fn poll_until_decided(client: &Client, request_id: &str, timeout: Duration) -> Result<Value, Failure> {
    let path = format!("/v1/requests/{request_id}");
    let started = tokio::time::Instant::now();
    println!("waiting for a decision");

    loop {
        let record = client.send(Method::GET, &path, None, false).await?;
        if record.get("phase").and_then(Value::as_str) != Some("pending") {
            return Ok(record);
        }
        if started.elapsed() >= timeout {
            return Err("timed out waiting for a decision; the request is still pending".into());
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

/// Print requests as a short table.
async fn list(client: &Client, phase: Option<&str>) -> Result<(), Failure> {
    let path = phase.map_or_else(
        || "/v1/requests".to_owned(),
        |phase| format!("/v1/requests?phase={phase}"),
    );
    let rows = client.send(Method::GET, &path, None, true).await?;

    let Some(rows) = rows.as_array() else {
        return Err("the service did not return a list".into());
    };
    if rows.is_empty() {
        println!("no enrollments");
        return Ok(());
    }

    println!("{:<38} {:<14} {:<10} {:<12}", "REQUEST", "SITE", "PHASE", "DECIDED BY");
    for row in rows {
        println!(
            "{:<38} {:<14} {:<10} {}",
            field(row, "requestId"),
            field(row, "siteName"),
            field(row, "phase"),
            field(row, "decidedBy"),
        );
    }
    Ok(())
}

/// Approve one request.
async fn approve(client: &Client, request_id: &str) -> Result<(), Failure> {
    let path = format!("/v1/requests/{request_id}/approve");
    let record = client.send(Method::POST, &path, None, true).await?;
    println!(
        "approved {} as {}",
        field(&record, "siteName"),
        field(&record, "spiffeId")
    );
    Ok(())
}

/// Deny one request.
async fn deny(client: &Client, request_id: &str, reason: Option<&str>) -> Result<(), Failure> {
    let path = format!("/v1/requests/{request_id}/deny");
    let body = reason.map(|reason| json!({"reason": reason}));
    let record = client.send(Method::POST, &path, body, true).await?;
    println!("denied {}", field(&record, "siteName"));
    Ok(())
}

/// The DER body of a PEM document.
fn pem_to_der(pem_text: &str) -> Result<Vec<u8>, Failure> {
    Ok(pem::parse(pem_text)?.contents().to_vec())
}

/// Write a file only the owner can read.
fn write_private(path: &Path, contents: &str) -> std::io::Result<()> {
    std::fs::write(path, contents)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
}

/// Talks to one enrollment service.
struct Client {
    /// Base URL, without a trailing slash.
    server: String,

    /// Operator token, when one was given.
    token: Option<String>,
}

impl Client {
    /// A client for one server.
    fn new(server: &str, token: Option<String>) -> Self {
        Self {
            server: server.trim_end_matches('/').to_owned(),
            token,
        }
    }

    /// Collect the joining kit for one request.
    ///
    /// The kit carries the gossip key, so the grid will not hand it over without
    /// proof that the caller is the provider that asked. The proof is a signature
    /// over the request identifier, made with the key this process generated and
    /// never sent.
    async fn collect_kit(&self, request_id: &str, key: &rcgen::KeyPair) -> Result<Value, Failure> {
        let id = uuid::Uuid::parse_str(request_id)?;
        let signing = ring::signature::EcdsaKeyPair::from_pkcs8(
            &ring::signature::ECDSA_P256_SHA256_ASN1_SIGNING,
            &pem_to_der(&key.serialize_pem())?,
            &ring::rand::SystemRandom::new(),
        )
        .map_err(|_bad| "the generated key cannot sign")?;

        let rng = ring::rand::SystemRandom::new();
        let signature = signing
            .sign(&rng, id.as_bytes())
            .map_err(|_bad| "could not sign the request identifier")?;

        let proof = json!({
            "signature": base64::engine::general_purpose::STANDARD.encode(signature.as_ref())
        });
        self.send(
            Method::POST,
            &format!("/v1/requests/{request_id}/join"),
            Some(proof),
            false,
        )
        .await
    }

    /// Make one request and return the decoded body.
    ///
    /// `needs_token` reports whether the route requires an operator, so a missing
    /// token is explained before a round trip rather than after a 401.
    async fn send(&self, method: Method, path: &str, body: Option<Value>, needs_token: bool) -> Result<Value, Failure> {
        if needs_token && self.token.is_none() {
            return Err("this command needs an operator token: pass --token or set GRID_OPERATOR_TOKEN".into());
        }

        let response = self.round_trip(method, path, body).await?;
        let status = response.status();
        let bytes = response.into_body().collect().await?.to_bytes();
        let decoded: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);

        if status.is_success() {
            Ok(decoded)
        } else {
            Err(format!("{status}: {}", field(&decoded, "message")).into())
        }
    }

    /// Send one request and hand back the response.
    async fn round_trip(
        &self,
        method: Method,
        path: &str,
        body: Option<Value>,
    ) -> Result<hyper::Response<hyper::body::Incoming>, Failure> {
        let connector = hyper_rustls::HttpsConnectorBuilder::new()
            .with_native_roots()?
            .https_or_http()
            .enable_http1()
            .build();
        let client: HyperClient<_, Full<Bytes>> = HyperClient::builder(TokioExecutor::new()).build(connector);

        let mut builder = Request::builder()
            .method(method)
            .uri(format!("{}{path}", self.server))
            .header("content-type", "application/json");
        if let Some(token) = self.token.as_deref() {
            builder = builder.header("authorization", format!("Bearer {token}"));
        }

        let payload = body.map(|value| value.to_string()).unwrap_or_default();
        Ok(client.request(builder.body(Full::new(Bytes::from(payload)))?).await?)
    }
}
