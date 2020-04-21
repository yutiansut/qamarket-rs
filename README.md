# qamarket-rs 高性能行情分发中间件
rust version for qamarketcollector, for a high performance with limited resources


![Rust](https://github.com/yutiansut/qamarket-rs/workflows/Rust/badge.svg)

分发的单个行情格式为

```rust
pub struct QAKlineBase{
    pub datetime: String,
    pub updatetime: String,
    pub code: String,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub frequence: String
}
```

订阅：

- model: direct
- exchange:  realtime_{code}   eg.  realtime_rb2010
- routing_key: 1min/ 5min/ 15min/ 30min/ 60min

本项目通过docker部署直接拉起后自动会实时重采样所有主力连续合约,并实时重采样成bar数据主动推送出来