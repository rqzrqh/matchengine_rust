### Overview

It's a memory-matchengine, very simple. Written in rust, **it's used for examing my rust-study .**
It referes to a c-language matchengine which runs long-term.

more detail see [design.md](https://github.com/rqzrqh/matchengine_rust/blob/master/design.md)



### Usage

cd deploy

```
./db_market.sh localhost $dbuser $dbpasswd eth_btc
```

```
./kafka_market.sh eth_btc
```

```
./kafka_settle.sh
```

```
RUST_LOG=info cargo run --release ./deploy/eth_btc.conf
```

```
python3 ./tests/order_generator.py
```

### FileList

**console** query matchengine info

**src** all code

**deploy**  scripts of init database and messagequeue, config file

**tests** very useful test scripts, generate various orders



### TODO

1.Replay from a specificed offset. But the rust-rdkafka seems not work well now.

2.The input data contain oper_id and the engine record it and do continuity check.

3.Use orderly batch send for higher data-push performance

4.Use a better decimal library for rescale a value. The rust_decimal's function of rescale only support decimal part, not support integer part.

5.Automatically deletes old snapshot













