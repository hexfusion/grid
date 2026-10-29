//! Grid-level routing for namespace sites sharing a Kubernetes cluster.

use super::*;
use crate::{
    crd::grid_network::{GridNetwork, SiteRouter, TlsConfig},
    resources::{consumer_config::render_consumer_config_with_projected, routing_overlay},
};

#[test]
fn namespace_sites_keep_candidates_and_gateway_endpoints_distinct() {
    let topology = NamespaceSites::new();
    let (overlay, resolution) = topology.project("consumer");
    let candidates: BTreeSet<_> = overlay
        .candidates
        .iter()
        .map(|candidate| {
            (
                candidate.name.as_str(),
                candidate.site.as_str(),
                candidate.cluster.as_str(),
            )
        })
        .collect();
    assert_eq!(
        candidates,
        BTreeSet::from([
            ("model-x", "cluster-a-team-a", "prov-a"),
            ("model-x", "cluster-a-team-b", "prov-b"),
            ("team-a-only", "cluster-a-team-a", "prov-a"),
            ("team-b-only", "cluster-a-team-b", "prov-b"),
        ]),
        "shared model names must preserve both provider/site pairs without crossing namespace ownership"
    );
    let identities: BTreeSet<_> = overlay
        .candidates
        .iter()
        .map(|candidate| candidate.stable_id.as_deref().expect("candidate identity"))
        .collect();
    assert_eq!(
        identities.len(),
        4,
        "namespace sites must not collapse candidate identities"
    );
    assert_eq!(
        rendered_backends(&overlay, &resolution),
        BTreeMap::from([
            (
                "prov-a".to_owned(),
                vec!["maas-gateway.team-a.svc.cluster.local:443".to_owned()]
            ),
            (
                "prov-b".to_owned(),
                vec!["maas-gateway.team-b.svc.cluster.local:443".to_owned()]
            ),
        ]),
        "the grid router must dial each GridSite gateway, never either InferenceProvider endpoint"
    );
    for (provider, site_name) in [("prov-a", "cluster-a-team-a"), ("prov-b", "cluster-a-team-b")] {
        let transport = resolution[provider]
            .endpoint
            .transport
            .as_ref()
            .expect("site transport");
        assert_eq!(transport.mode, TransportMode::MutualTls);
        assert_eq!(
            transport.sni.as_deref(),
            Some(format!("{site_name}.grid.internal").as_str())
        );
    }
}

#[test]
fn namespace_sites_follow_gateway_updates_and_withdraw_only_the_removed_site() {
    let mut topology = NamespaceSites::new();
    let (original, _) = topology.project("consumer");
    topology.sites[0]
        .spec
        .gateway_endpoint
        .as_mut()
        .expect("site gateway")
        .address = "replacement.team-a.svc.cluster.local:443".to_owned();
    topology.sites[0].metadata.generation = Some(1);
    topology.sites[0]
        .status
        .as_mut()
        .expect("probed site")
        .observed_generation = 1;
    let (updated, resolution) = topology.project("consumer");
    assert_eq!(
        rendered_backends(&updated, &resolution)["prov-a"],
        vec!["replacement.team-a.svc.cluster.local:443"],
        "a verified GridSite endpoint change must update the grid dial table"
    );
    let identities = |overlay: &routing_overlay::RoutingOverlay| -> BTreeSet<String> {
        overlay
            .candidates
            .iter()
            .filter_map(|candidate| candidate.stable_id.clone())
            .collect()
    };
    assert_eq!(
        identities(&original),
        identities(&updated),
        "gateway rotation must preserve candidate identities"
    );
    topology.sites.remove(0);
    let (withdrawn, withdrawn_resolution) = topology.project("consumer");
    assert_eq!(withdrawn.candidates.len(), 2);
    assert!(
        withdrawn
            .candidates
            .iter()
            .all(|candidate| candidate.site == "cluster-a-team-b")
    );
    assert_eq!(
        rendered_backends(&withdrawn, &withdrawn_resolution),
        BTreeMap::from([(
            "prov-b".to_owned(),
            vec!["maas-gateway.team-b.svc.cluster.local:443".to_owned()]
        )]),
        "retaining the provider object must not bypass withdrawal of its GridSite"
    );
}

#[test]
fn namespace_sites_use_site_gateways_even_when_a_site_is_local_to_the_grid_router() {
    let topology = NamespaceSites::new();
    for local_site in ["cluster-a-team-a", "cluster-a-team-b"] {
        let (overlay, resolution) = topology.project(local_site);
        assert_eq!(
            rendered_backends(&overlay, &resolution),
            BTreeMap::from([
                (
                    "prov-a".to_owned(),
                    vec!["maas-gateway.team-a.svc.cluster.local:443".to_owned()]
                ),
                (
                    "prov-b".to_owned(),
                    vec!["maas-gateway.team-b.svc.cluster.local:443".to_owned()]
                ),
            ]),
            "locality {local_site} must not let a grid-level router bypass the namespace's GridSite gateway"
        );
    }
}

/// Distinct namespace gateways advertise a shared model and one model unique to each site.
struct NamespaceSites {
    /// Both namespace sites participate in one grid.
    network: GridNetwork,
    /// Cluster-scoped provider declarations with explicit namespace-site selectors.
    providers: Vec<InferenceProvider>,
    /// Two independently addressed gateways on the same physical cluster.
    sites: Vec<GridSite>,
}

impl NamespaceSites {
    /// All provider and site objects are visible through the same cluster's API.
    fn new() -> Self {
        let mut providers = Vec::new();
        let mut sites = Vec::new();
        for (name, namespace) in [("prov-a", "team-a"), ("prov-b", "team-b")] {
            let mut provider = provider(
                name,
                &format!("https://model-backend.{namespace}.svc.cluster.local:8000"),
                None,
            );
            provider
                .spec
                .models
                .push(serde_json::from_value(serde_json::json!({"name": format!("{namespace}-only")})).expect("model"));
            provider.spec.site_selector.match_labels =
                BTreeMap::from([("example.org/inference-namespace".to_owned(), namespace.to_owned())]);
            let site_name = format!("cluster-a-{namespace}");
            let mut site = site(
                &site_name,
                Some(mutual_egress(
                    &format!("maas-gateway.{namespace}.svc.cluster.local:443"),
                    &format!("{site_name}.grid.internal"),
                )),
            );
            site.metadata.labels = Some(BTreeMap::from([
                ("example.org/cluster".to_owned(), "cluster-a".to_owned()),
                ("example.org/inference-namespace".to_owned(), namespace.to_owned()),
            ]));
            providers.push(provider);
            sites.push(site);
        }
        Self {
            network: serde_json::from_value(serde_json::json!({
                "apiVersion": "grid.praxis.fast/v1alpha1", "kind": "GridNetwork",
                "metadata": {"name": "net"}, "spec": {"membership": {"mode": "Local"}, "signalTransport": {"mode": "poll"}}
            })).expect("network"),
            providers,
            sites,
        }
    }

    /// Exercise candidate projection and endpoint resolution together before rendering the dial table.
    fn project(&self, local_site: &str) -> (routing_overlay::RoutingOverlay, Resolution) {
        let overlay = routing_overlay::render_routing_overlay(
            &self.network,
            &self.sites,
            &self.providers,
            &[],
            &[],
            local_site,
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
        .expect("overlay");
        let resolution = resolve(
            &overlay.candidates,
            &[],
            &Declarations {
                signal: SignalMode::Poll,
                ..decl(&self.providers, &self.sites, local_site)
            },
        );
        assert!(resolution.refused.is_empty(), "{:?}", resolution.refused);
        (overlay, resolution)
    }
}

/// Read the actual load-balancer table emitted for `grid_site_route`.
fn rendered_backends(
    overlay: &routing_overlay::RoutingOverlay,
    resolution: &Resolution,
) -> BTreeMap<String, Vec<String>> {
    let endpoints: Vec<_> = resolution.values().map(|resolved| resolved.endpoint.clone()).collect();
    let render = render_consumer_config_with_projected(
        overlay,
        "/run/secrets/grid-credentials",
        &endpoints,
        "/etc/praxis/tls",
        8080,
        &TlsConfig::default(),
        "grid-router",
        "grid-system",
        None,
        false,
        false,
        SiteRouter::GridSiteRoute,
    )
    .expect("consumer render");
    let config: serde_yaml::Value = serde_yaml::from_str(&render.config_yaml).expect("rendered config");
    let filters = config["filter_chains"][0]["filters"].as_sequence().expect("filters");
    let balancer = filters
        .iter()
        .find(|filter| filter["filter"].as_str() == Some("load_balancer"))
        .expect("load balancer");
    balancer["clusters"]
        .as_sequence()
        .expect("clusters")
        .iter()
        .map(|cluster| {
            let name = cluster["name"].as_str().expect("cluster name").to_owned();
            let addresses = cluster["endpoints"]
                .as_sequence()
                .expect("endpoints")
                .iter()
                .map(|address| address.as_str().expect("endpoint address").to_owned())
                .collect();
            (name, addresses)
        })
        .collect()
}
