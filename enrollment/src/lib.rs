//! Enrollment for the AI grid.
//!
//! A provider that wants to join presents a one-shot invite an operator minted
//! and sends a certificate signing request over HTTP; redeeming the invite
//! issues the certificate directly, carrying the name the grid granted, with no
//! separate approve step. The provider needs no credentials on the grid's
//! cluster to ask, and the grid never sees the provider's private key.
//!
//! Storage is a backend enum, so a MaaS deployment can point this at the
//! Postgres it already runs while a standalone grid brings its own.

pub mod api;
pub mod auth;
pub mod authz;
pub mod metrics;
pub mod model;
pub mod store;

pub use api::{AppState, JoiningConfig, router};
pub use auth::Operators;
pub use store::{Invite, Issued, NewInvite, NewRequest, Store, StoreError};
