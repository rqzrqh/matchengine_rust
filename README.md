### Overview

It's a memory-matchengine, very simple. Written in rust, **it's used for examing my rust-study .**
It referes to a c-language matchengine which runs long-term.

more detail see [design.md](https://github.com/rqzrqh/matchengine_rust/blob/master/design.md)



### Usage

```
./deploy/db_market.sh localhost $dbuser $dbpasswd eth_btc
```

```
./deploy/kafka_market.sh eth_btc
```

```
./deploy/kafka_settle.sh
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

1.Replay from a specificed offset. But the rust-library of kafka and rdkafka aren't yet support this feature, the rocketmq also is incomplete.

2.Use a better decimal library for rescale a value. The rust_decimal's function of rescale only support decimal part, not support integer part.

3.Performance test and optimize

4.add process lock

5.Automatically deletes old snapshot

6.use rocketmq, kafka is not a suitable message queue for more than 64 topics











