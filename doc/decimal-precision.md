# `rust_decimal` 与 libmpdecimal 参考行为的差异与应对（不换精度库）

撮合核心继续使用 [`rust_decimal::Decimal`](https://docs.rs/rust_decimal)，不嵌入 libmpdecimal。与常见基于 mpdecimal 的参考实现的数值差异主要来自 **类型能力** 与 **舍入规则**，而不是撮合循环结构（已与 `match.c` 对齐）。

## 差异摘要

| 方面 | 参考实现（mpdecimal 2.4.x） | 本仓库（rust_decimal） |
|------|------------------------------|-------------------------|
| 系数/指数范围 | IEEE decimal128 等，更宽 | 96-bit 系数，scale ≤ 28 |
| 默认舍入 | 上下文里 `MPD_ROUND_DOWN`（对 mpdecimal 即**向 0 截断**） | `rescale` / 部分运算为 **MidpointAwayFromZero** 等 |
| `rescale` 语义 | `mpd_rescale` 的指数与 Rust 的「小数位数」需对照理解 | `Decimal::rescale(dp)` 以 **dp 位小数** 重标 |

## 不换库时的可行办法

1. **配置层收紧（已有 `validate_config`）**  
   保证 `stock_prec` / `money_prec` / `fee_prec` ≤ 28，`min_amount` 解析后在 `stock_prec` 下仍 &gt; 0，避免静默变成 0 或溢出。

2. **业务上控制输入规模**  
   价格、数量、金额保持在 `rust_decimal` 可表示范围内；超大中间值在进引擎前由网关或清算层先量化。

3. **已实现：与参考引擎同位置的量化**  
   `src/align_decimal.rs` 中的 **`rescale_down`**：当当前 scale **大于** 目标 `dp` 时用 **`RoundingStrategy::ToZero`**（向 0 截断，贴近 `MPD_ROUND_DOWN`）；当 scale **小于等于** `dp` 时仍用 **`Decimal::rescale`** 做补零。市价买单可成交量由 **`market_buy_base_amount`**（与 `match.c` 中 `mpd_rescale` 路径一致）计算。  
   **仅**在参考实现也会做 rescale 的路径使用 `rescale_down`：**`load`**（MySQL 快照字段，对应 `decimal(row, prec)`）、**`mainprocess`**（Kafka 指令入参，对应 `server.c` 里 `decimal(..., prec)`）、**`engine`**（经由 `market_buy_base_amount`）。对外 JSON/Kafka 消息体与 **`get_order_info` / `publish_deal_message` / `mpd_to_sci`** 一样，**不再**在输出前二次按精度钉死。

4. **验收**  
   对关键市场用同一批订单在参考引擎与 Rust 引擎跑 **对比用例**（成交价、成交量、账户累计字段），差异可接受即视为等效；若单点不一致再针对该运算点改舍入策略。

5. **仍不够时**  
   仅在 **边界层**（HTTP/Kafka 入参）用 C/libmpdecimal 做一次性量化，再传入本引擎的 `String`/`Decimal`，**不**把 mpdecimal 铺进撮合主路径。

## 延伸阅读

- `rust_decimal` 的 `rescale` 行为见 crate 文档（整体重标，含整数部分进位）。
- mpdecimal 的 `mpd_rescale` 与指数约定见上游 `doc/context.html` / `mpdecimal.h`。
