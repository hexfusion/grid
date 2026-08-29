//! Enrollment for the AI grid.
//!
//! A provider that wants to join sends a certificate signing request over HTTP,
//! an operator approves it, and the provider collects a certificate carrying the
//! name the grid granted. The provider needs no credentials on the grid's
//! cluster to ask, and the grid never sees the provider's private key.
//!
//! Storage is a backend enum, so a MaaS deployment can point this at the
//! Postgres it already runs while a standalone grid brings its own.

pub mod api;
pub mod auth;
pub mod model;
pub mod store;

pub use api::{AppState, router};
pub use auth::Operators;
pub use store::Store;
