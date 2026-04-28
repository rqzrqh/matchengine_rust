use crate::publish::PublishBacklog;
use crate::task::{self, HttpOp, HttpRequestTask, HttpResponse};
use axum::body::Body;
use axum::extract::{Path, Query, Request, State};
use axum::http::{header, Method, StatusCode};
use axum::response::Response;
use axum::routing::get;
use axum::Router;
use json::JsonValue;
use serde::Deserialize;
use std::sync::{mpsc, Arc};
use tower_http::cors::CorsLayer;

#[derive(Clone)]
pub struct EngineHttpState {
    pub main_routine_sender: mpsc::Sender<task::Task>,
    pub market_name: String,
    pub publish_backlog: Arc<PublishBacklog>,
}

#[derive(Deserialize)]
struct MarketPath {
    market: String,
}

#[derive(Deserialize)]
struct MarketOrderPath {
    market: String,
    order_id: u64,
}

#[derive(Deserialize)]
struct MarketUserPath {
    market: String,
    user_id: u32,
}

#[derive(Deserialize)]
struct OrderBookQuery {
    side: u32,
    #[serde(default)]
    offset: u32,
    #[serde(default = "default_ob_limit")]
    limit: u32,
}

fn default_ob_limit() -> u32 {
    12
}

#[derive(Deserialize)]
struct UserOrdersQuery {
    #[serde(default)]
    offset: u32,
    #[serde(default = "default_uo_limit")]
    limit: u32,
}

fn default_uo_limit() -> u32 {
    50
}

pub async fn serve_engine_http(addr: std::net::SocketAddr, state: EngineHttpState) {
    let app = Router::new()
        .route("/markets/:market/summary", get(get_market_summary))
        .route("/markets/:market/status", get(get_market_status))
        .route("/markets/:market/publish-pending", get(get_publish_pending))
        .route(
            "/markets/:market/orders/:order_id",
            get(get_order_detail_http),
        )
        .route("/markets/:market/order-book", get(get_order_book_http))
        .route(
            "/markets/:market/users/:user_id/orders",
            get(get_user_orders_http),
        )
        .fallback(fallback_unmatched)
        .layer(CorsLayer::permissive())
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("engine http bind");
    if let Err(e) = axum::serve(listener, app).await {
        error!("engine http server: {}", e);
    }
}

async fn get_market_summary(
    State(st): State<EngineHttpState>,
    Path(MarketPath { market }): Path<MarketPath>,
) -> Response {
    let res = forward_http_request(st, HttpOp::MarketSummary { market }).await;
    http_response_to_axum(res)
}

async fn get_market_status(
    State(st): State<EngineHttpState>,
    Path(MarketPath { market }): Path<MarketPath>,
) -> Response {
    let res = forward_http_request(st, HttpOp::MarketStatus { market }).await;
    http_response_to_axum(res)
}

async fn get_publish_pending(
    State(st): State<EngineHttpState>,
    Path(MarketPath { market }): Path<MarketPath>,
) -> Response {
    if st.market_name != market {
        return http_response_to_axum(HttpResponse {
            status: 404,
            body: "{\"error\":\"unknown market\"}".to_string(),
        });
    }

    let snapshot = st.publish_backlog.snapshot();
    let mut body = JsonValue::new_object();
    body["market"] = market.into();
    body["quote_pending"] = snapshot.quote_pending.into();
    body["settle_pending"] = snapshot.settle_pending.into();
    body["total_pending"] = snapshot.total_pending.into();

    let mut settle_group_pending = JsonValue::new_array();
    for (idx, pending) in snapshot.settle_group_pending.into_iter().enumerate() {
        let mut item = JsonValue::new_object();
        item["group_id"] = (idx as u32).into();
        item["pending"] = pending.into();
        settle_group_pending[idx] = item;
    }
    body["settle_group_pending"] = settle_group_pending;

    http_response_to_axum(HttpResponse {
        status: 200,
        body: body.dump(),
    })
}

async fn get_order_detail_http(
    State(st): State<EngineHttpState>,
    Path(MarketOrderPath { market, order_id }): Path<MarketOrderPath>,
) -> Response {
    let res = forward_http_request(
        st,
        HttpOp::OrderDetail {
            market,
            order_id,
        },
    )
    .await;
    http_response_to_axum(res)
}

async fn get_order_book_http(
    State(st): State<EngineHttpState>,
    Path(MarketPath { market }): Path<MarketPath>,
    Query(q): Query<OrderBookQuery>,
) -> Response {
    let res = forward_http_request(
        st,
        HttpOp::OrderBook {
            market,
            side: q.side,
            offset: q.offset,
            limit: q.limit.clamp(1, 500),
        },
    )
    .await;
    http_response_to_axum(res)
}

async fn get_user_orders_http(
    State(st): State<EngineHttpState>,
    Path(MarketUserPath { market, user_id }): Path<MarketUserPath>,
    Query(q): Query<UserOrdersQuery>,
) -> Response {
    let res = forward_http_request(
        st,
        HttpOp::UserOrdersPending {
            market,
            user_id,
            offset: q.offset,
            limit: q.limit.clamp(1, 500),
        },
    )
    .await;
    http_response_to_axum(res)
}

async fn forward_http_request(st: EngineHttpState, op: HttpOp) -> HttpResponse {
    let (tx, rx) = tokio::sync::oneshot::channel();
    st.main_routine_sender
        .send(task::Task::HttpRequest(HttpRequestTask { op, rsp: tx }))
        .expect("send http task failed");
    rx.await.unwrap()
}

fn http_response_to_axum(r: HttpResponse) -> Response {
    let status = StatusCode::from_u16(r.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::ACCESS_CONTROL_ALLOW_ORIGIN, "*")
        .body(Body::from(r.body))
        .unwrap()
}

async fn fallback_unmatched(req: Request) -> Response {
    match *req.method() {
        Method::GET => text_response(StatusCode::NOT_FOUND, "not found"),
        _ => text_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed"),
    }
}

fn text_response(status: StatusCode, msg: &str) -> Response {
    Response::builder()
        .status(status)
        .header(header::ACCESS_CONTROL_ALLOW_ORIGIN, "*")
        .body(Body::from(msg.to_string()))
        .unwrap()
}
