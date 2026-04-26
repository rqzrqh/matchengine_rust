//! Engine HTTP: Axum serves REST JSON under `/markets/...` (`server`) with json builders (`rest`).

mod rest;
mod server;

pub use rest::handle_rest_request;
pub use server::{serve_engine_http, EngineHttpState};
