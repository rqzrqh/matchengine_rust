//! Engine HTTP: Axum 在 `server` 里挂路由；撮合线程在 `handlers` 里根据 `HttpOp` 拼状态码与 JSON 响应体。

mod handlers;
mod server;

pub use handlers::handle_http_request;
pub use server::{EngineHttpState, serve_engine_http};
