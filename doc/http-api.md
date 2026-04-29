# 撮合引擎 HTTP 协议

本文档描述进程内只读查询接口。每个引擎进程只服务一个市场（`config.yaml` 中的 `market.name`），请求路径里的 `:market` 必须与该市场名一致，否则返回 **404** 及 `{"error":"unknown market"}`。

**说明**：单进程、单市场；监听地址在 `src/main.rs` 中写死为 `127.0.0.1:8080`（可据此推断基础 URL；若你本地改过源码，以实际运行为准）。

## 通用约定

| 项 | 说明 |
|----|------|
| 方法 | 仅 **GET**；其它方法对未定义路径返回 **405**（纯文本 `method not allowed`） |
| 内容类型 | 成功时响应体为 **JSON**，`Content-Type: application/json` |
| CORS | 使用宽松策略（`CorsLayer::permissive`），成功响应与部分错误会带 `Access-Control-Allow-Origin: *` |
| 未匹配 GET | **404**（纯文本 `not found`） |
| 市场不存在 | **404**，JSON：`{"error":"unknown market"}` |
| 业务错误体 | 形如 `{"error":"..."} ` |

请求由 Axum 转发到主线程的撮合状态上执行，为同步阻塞查询。

---

## 1. 市场汇总

`GET /markets/{market}/summary`

| HTTP 状态 | 含义 |
|-----------|------|
| 200 | 成功 |
| 404 | 市场名不匹配 |

**成功响应字段**

| 字段 | 类型 | 说明 |
|------|------|------|
| `name` | string | 市场名 |
| `ask_count` | number | 卖单（asks）条数 |
| `bid_count` | number | 买单（bids）条数 |
| `ask_stock_amount` | string | 总卖 stock 的量，即所有卖单 `left` 之和 |
| `ask_money_amount` | string | 总卖 token 量乘以价格的总金额，即所有卖单 `price * left` 之和 |
| `bid_stock_amount` | string | 总买 stock 的量，即所有买单 `left` 之和 |
| `bid_money_amount` | string | 总买 stock 的量乘以价格的总金额，即所有买单 `price * left` 之和 |

---

## 2. 市场内部状态/游标

`GET /markets/{market}/status`

| HTTP 状态 | 含义 |
|-----------|------|
| 200 | 成功 |
| 404 | 市场名不匹配 |

**成功响应字段**

| 字段 | 类型 | 说明 |
|------|------|------|
| `oper_id` | number | 操作 id |
| `order_id` | number | 当前订单 id 游标相关 |
| `deals_id` | number | 成交 id 相关 |
| `message_id` | number | 消息 id 相关 |
| `input_offset` | number | 输入游标（Kafka 等） |
| `input_sequence_id` | number | 输入序列 id |

---

## 3. 订单详情

`GET /markets/{market}/orders/{order_id}`

路径参数 `order_id` 为无符号 64 位整数。

| HTTP 状态 | 含义 |
|-----------|------|
| 200 | 找到订单 |
| 404 | 市场不存在，或 `order not found` |

**订单对象**（`200` 时根对象即为单条订单，字段均来自 `Order::to_json`）

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | number | 订单 id |
| `type` | number | 订单类型：`1` 限价、`2` 市价（与 `market` 中常量一致） |
| `side` | number | `1` 卖（ask）、`2` 买（bid） |
| `create_time` | number | 创建时间 |
| `update_time` | number | 更新时间 |
| `user_id` | number | 用户 id |
| `price` | string | 价格（定标到该市场的报价小数位） |
| `amount` | string | 数量（`stock_prec`） |
| `taker_fee_rate` | string | Taker 费率（`fee_rate_prec`） |
| `maker_fee_rate` | string | Maker 费率（`fee_rate_prec`） |
| `left` | string | 剩余量；市价买单按 **金额** 精度（`money_prec`），否则为 `stock_prec` |
| `deal_stock` | string | 已成交数量（`stock_prec`） |
| `deal_money` | string | 已成交金额（`money_prec`） |
| `deal_fee` | string | 已付手续费（`money_prec`） |

---

## 4. 订单簿（深度）

`GET /markets/{market}/order-book?side={side}&offset={offset}&limit={limit}`

| 参数 | 必填 | 默认 | 限制 | 说明 |
|------|------|------|------|------|
| `side` | 是 | — | 必须为 `1` 或 `2` | `1` = 卖盘（asks），`2` = 买盘（bids） |
| `offset` | 否 | `0` | — | 跳过的档位数（从 0 起算） |
| `limit` | 否 | `12` | 服务端夹紧到 **1～500** | 本页条数 |

| HTTP 状态 | 含义 |
|-----------|------|
| 200 | 成功（含 `side` 非法时不会到这里） |
| 400 | `side` 不是 1/2，JSON：`{"error":"invalid side (use 1=ask, 2=bid)"}` |
| 404 | 市场名不匹配 |

**成功响应**

| 字段 | 类型 | 说明 |
|------|------|------|
| `limit` | number | 本请求的 limit（夹紧后） |
| `offset` | number | 本请求的 offset |
| `total` | number | 该侧订单总数 |
| `orders` | array | 订单对象列表，元素结构同 [§3 订单对象](#3-订单详情) |
| `stock_amount` | string | 仅当 `side=1`（卖）：市场侧累计数量相关（`stock_prec`） |
| `money_amount` | string | 仅当 `side=2`（买）：市场侧累计金额相关（`money_prec`） |

卖盘、买盘在实现中按各自顺序取 `offset` 起的 `limit` 条（卖/买簿内的排序与撮合树一致，价格/时间等见引擎实现，此处不展开）。

---

## 5. 用户未完结订单

`GET /markets/{market}/users/{user_id}/orders?offset={offset}&limit={limit}`

| 参数 | 必填 | 默认 | 限制 |
|------|------|------|------|
| `offset` | 否 | `0` | — |
| `limit` | 否 | `50` | 夹紧到 **1～500** |

| HTTP 状态 | 含义 |
|-----------|------|
| 200 | 成功（用户无订单时 `total=0`，`orders=[]`） |
| 404 | 市场名不匹配 |

**成功响应**

| 字段 | 类型 | 说明 |
|------|------|------|
| `limit` | number | 本请求的 limit（夹紧后） |
| `offset` | number | 本请求的 offset |
| `total` | number | 该用户未完结订单总数 |
| `orders` | array | 订单对象列表，结构同 [§3](#3-订单详情) |

---

## 示例

基础路径（以默认端口为例）：

```http
GET http://127.0.0.1:8080/markets/BTCUSDT/summary
GET http://127.0.0.1:8080/markets/BTCUSDT/status
GET http://127.0.0.1:8080/markets/BTCUSDT/orders/1001
GET http://127.0.0.1:8080/markets/BTCUSDT/order-book?side=1&offset=0&limit=20
GET http://127.0.0.1:8080/markets/BTCUSDT/users/42/orders?offset=0&limit=50
```

将 `BTCUSDT` 换成你 `config.yaml` 里实际的市场名。

---

## 与源码的对应关系

| 文档章节 | 实现位置 |
|----------|----------|
| 路由与 query 默认/夹紧 | `src/http/server.rs` |
| 状态码与 JSON 体 | `src/http/handlers.rs`（`handle_http_request`） |
| 订单 JSON 各字段定标 | `src/market.rs` 中 `Order::to_json` |
| 查询枚举 | `src/task.rs` 中 `HttpOp` |
