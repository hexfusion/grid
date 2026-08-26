//! Mock AI provider servers for integration testing.
//!
//! Each module exposes a `router()` function returning an
//! [`axum::Router`] that simulates a specific provider's API.
//! Pass an [`AppState`] to inject the `X-Grid-Demo-Provider`
//! response header for demo attribution.

#![deny(unsafe_code)]

use std::sync::Arc;

/// Mock Anthropic Messages API.
pub mod anthropic;
/// Mock AWS Bedrock Converse API.
pub mod bedrock;
/// Shared HTTP response utilities.
mod common;
/// Mock llm-d inference pool, fronted by an endpoint picker.
pub mod llmd;
/// Serving capacity and the queue depth it produces.
pub mod load;
/// Mock `OpenAI` chat completions and Responses API.
pub mod openai;
/// Mock Google Vertex AI `generateContent` API.
pub mod vertex;

/// Shared application state injected into every provider router.
#[derive(Clone, Debug)]
pub struct AppState {
    /// Site identity for the `X-Grid-Demo-Provider` response header.
    pub provider_site: Arc<str>,

    /// Normalized queue depth exported by the demo metrics endpoint.
    ///
    /// A fixed setting, kept for callers that want a site pinned to a value.
    /// The vLLM-shaped series alongside it are measured instead.
    pub queue_depth: f64,

    /// Serving capacity, and the backlog that offered load produces against it.
    pub load: Arc<load::Load>,
}
