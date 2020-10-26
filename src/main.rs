extern crate crossbeam_utils;
extern crate ndarray;

use std::thread;

use amiquip::Delivery;
use amiquip::{
    Connection, ConsumerMessage, ConsumerOptions, Exchange, ExchangeDeclareOptions, ExchangeType,
    FieldTable, Publish, QueueDeclareOptions, Result,
};
use crossbeam_channel::bounded;
use crossbeam_channel::{Receiver, Sender};
use ndarray::array;
use serde::{Deserialize, Serialize};
use serde_json::json;
use serde_json::value::Value;

use crate::dataframe::DataCell;
use crate::config::CONFIG;

mod dataframe;
mod config;

pub struct QAEventMQ {
    pub amqp: String,
    pub exchange: String,
    pub model: String,
    pub routing_key: String,
    // connection:
}

impl QAEventMQ {
    pub fn consume(eventmq: QAEventMQ, ws_event_tx: Sender<String>) -> Result<()> {
        let client = eventmq;
        let mut connection = Connection::insecure_open(&client.amqp)?;
        let channel = connection.open_channel(None)?;
        let exchange = channel.exchange_declare(
            ExchangeType::Direct,
            &client.exchange,
            ExchangeDeclareOptions::default(),
        )?;
        let queue = channel.queue_declare(
            "",
            QueueDeclareOptions {
                exclusive: true,
                ..QueueDeclareOptions::default()
            },
        )?;
        println!("created exclusive queue {}", queue.name());

        queue.bind(&exchange, client.routing_key.clone(), FieldTable::new())?;

        let consumer = queue.consume(ConsumerOptions {
            no_ack: true,
            ..ConsumerOptions::default()
        })?;

        for (_i, message) in consumer.receiver().iter().enumerate() {
            match message {
                ConsumerMessage::Delivery(delivery) => {
                    let body = String::from_utf8_lossy(&delivery.body);
                    QAEventMQ::callback(&client, &delivery, &ws_event_tx);
                }
                other => {
                    println!("Consumer ended: {:?}", other);
                    break;
                }
            }
        }
        connection.close()
    }

    pub fn publish_fanout(
        amqp: String,
        exchange_name: String,
        context: String,
        routing_key: String,
    ) {
        let mut connection = Connection::insecure_open(&amqp).unwrap();
        let channel = connection.open_channel(None).unwrap();
        let exchange = channel
            .exchange_declare(
                ExchangeType::Fanout,
                &exchange_name,
                ExchangeDeclareOptions::default(),
            )
            .unwrap();

        exchange
            .publish(Publish::new(context.as_bytes(), routing_key.as_str()))
            .unwrap();
        //connection.close();
    }
    pub fn publish_direct(
        amqp: String,
        exchange_name: String,
        context: String,
        routing_key: String,
    ) {
        let mut connection = Connection::insecure_open(&amqp).unwrap();
        let channel = connection.open_channel(None).unwrap();
        let exchange = channel
            .exchange_declare(
                ExchangeType::Direct,
                &exchange_name,
                ExchangeDeclareOptions::default(),
            )
            .unwrap();

        exchange
            .publish(Publish::new(context.as_bytes(), routing_key.as_str()))
            .unwrap();
        //connection.close();
    }
    pub fn callback(eventmq: &QAEventMQ, message: &Delivery, ws_event_tx: &Sender<String>) {
        let msg = message.body.clone();
        let foo = String::from_utf8(msg).unwrap();
        let data = foo.to_string();

        //println!("{:?}",data);
        ws_event_tx.send(data).unwrap();
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QAKlineBase {
    pub datetime: String,
    pub updatetime: String,
    pub code: String,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub frequence: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QASeries {
    //pub tick: Vec<QAKlineBase>,
    pub min1: Vec<QAKlineBase>,
    pub min5: Vec<QAKlineBase>,
    pub min15: Vec<QAKlineBase>,
    pub min30: Vec<QAKlineBase>,
    pub min60: Vec<QAKlineBase>,
    dtmin: String, //dtmin是用于控制分钟线生成的
}

fn main() {
    // let code = "au2002".to_string();

    // let subscribe_list = vec![
    //     "IF2005", "IH2005", "IC2005", "TF2006", "T2006", "cu2006", "au2006", "ag2006", "zn2007",
    //     "al2007", "ru2009", "rb2010", "fu2009", "hc2010", "bu2006", "pb2006", "ni2007", "sn2007",
    //     "wr2010", "sc2006", "a2009", "b2006", "bb2006", "c2009", "cs2009", "fb2009", "i2009",
    //     "j2009", "jd2006", "jm2009", "l2009", "m2009", "p2009", "pp2009", "v2009", "y2009",
    //     "WH009", "PM007", "CF009", "CY009", "SR009", "TA009", "OI009", "RI009", "MA009", "FG009",
    //     "RS009", "RM009", "ZC009", "JR007", "LR007", "SF009", "SM009", "AP010", "TS2006", "sp2009",
    //     "eg2009", "CJ009", "nr2007", "rr2009", "UR009", "ss2007", "eb2009", "SA009", "pg2011",
    // ];
    let subscribe_list:Vec<String> = CONFIG.common.subscribe_list.clone();

    let sub_iter = subscribe_list.iter().cloned();
    for code in sub_iter {
        thread::spawn(move || {
            subscribe_code(code.parse().unwrap());
        });
    }
    loop {}
}

fn subscribe_code(code: String) {
    let (s1, r1) = bounded(0);
    let route_key = code.clone();
    thread::spawn(move || {
        let mut client = QAEventMQ {
            amqp: "amqp://admin:admin@qaeventmq:5672/".to_string(),
            exchange: "CTPX".to_string(),
            model: "direct".to_string(),
            routing_key: route_key,
        };
        println!("xxx");
        QAEventMQ::consume(client, s1).unwrap();
    });

    let mut kline = QASeries::init();

    let mut connection = Connection::insecure_open("amqp://admin:admin@qaeventmq:5672/").unwrap();
    let channel = connection.open_channel(None).unwrap();
    let exchange = channel
        .exchange_declare(
            ExchangeType::Direct,
            format!("realtime_{}", code.clone()).as_str(),
            ExchangeDeclareOptions::default(),
        )
        .unwrap();

    loop {
        let data = r1.recv().unwrap();
        let resx: Value = serde_json::from_str(&data).unwrap();

        kline.update(resx);
        //kline.print();
        //        let js = kline.to_json();

        exchange
            .publish(Publish::new(kline.to_1min_json().as_bytes(), "1min"))
            .unwrap();
        exchange
            .publish(Publish::new(kline.to_5min_json().as_bytes(), "5min"))
            .unwrap();
        exchange
            .publish(Publish::new(kline.to_15min_json().as_bytes(), "15min"))
            .unwrap();
        exchange
            .publish(Publish::new(kline.to_30min_json().as_bytes(), "30min"))
            .unwrap();
        exchange
            .publish(Publish::new(kline.to_60min_json().as_bytes(), "60min"))
            .unwrap();
    }
    connection.close();
}

impl QAKlineBase {
    fn init(frequence: String) -> QAKlineBase {
        QAKlineBase {
            datetime: "".to_string(),
            updatetime: "".to_string(),
            code: "".to_string(),
            open: 0.0,
            high: 0.0,
            low: 0.0,
            close: 0.0,
            volume: 0.0,
            frequence,
        }
    }

    fn update(&mut self, data: Value) {
        if self.open == 0.0 {
            self.init_data(data.clone());
        }
        let new_price = data["last_price"].as_f64().unwrap();
        if (self.high < new_price) {
            self.high = new_price;
        }
        if (self.low > new_price) {
            self.low = new_price;
        }
        self.close = new_price;
        let cur_datetime: String = data["datetime"].as_str().unwrap().parse().unwrap();
        self.updatetime = cur_datetime.clone();
    }

    fn init_data(&mut self, data: Value) {
        self.datetime = data["datetime"].as_str().unwrap().parse().unwrap();
        self.updatetime = data["datetime"].as_str().unwrap().parse().unwrap();
        self.code = data["symbol"].as_str().unwrap().parse().unwrap();
        self.open = data["last_price"].as_f64().unwrap();
        self.high = data["last_price"].as_f64().unwrap();
        self.low = data["last_price"].as_f64().unwrap();
        self.close = data["last_price"].as_f64().unwrap();
        self.volume = data["volume"].as_f64().unwrap();
    }

    fn print(&mut self) {
        println!(
            "\n\r {:?}\n\r",
            row![
                self.datetime.clone(),
                self.code.clone(),
                self.open,
                self.high,
                self.low,
                self.close,
                self.volume
            ]
        );
    }

    fn new(&mut self, data: Value) -> QAKlineBase {
        let data = QAKlineBase {
            datetime: data["datetime"].as_str().unwrap().parse().unwrap(),
            updatetime: data["datetime"].as_str().unwrap().parse().unwrap(),
            code: data["symbol"].as_str().unwrap().parse().unwrap(),
            open: data["last_price"].as_f64().unwrap(),
            high: data["last_price"].as_f64().unwrap(),
            low: data["last_price"].as_f64().unwrap(),
            close: data["last_price"].as_f64().unwrap(),
            volume: data["volume"].as_f64().unwrap(),
            frequence: self.frequence.clone(),
        };
        data
    }
    fn to_json(&mut self) -> String {
        let jdata = serde_json::to_string(&self).unwrap();
        //println!("this is json{:#?}", jdata);
        jdata
    }
}

impl QASeries {
    fn init() -> QASeries {
        QASeries {
            //            tick:vec![],
            min1: vec![],
            min5: vec![],
            min15: vec![],
            min30: vec![],
            min60: vec![],
            dtmin: "99".to_string(),
        }
    }
    fn update(&mut self, data: Value) {
        let cur_data = data.clone();
        let cur_datetime: String = cur_data["datetime"].as_str().unwrap().parse().unwrap();
        if self.dtmin == "99".to_string() {
            self.update_1(cur_data.clone(), cur_datetime.clone());
            self.update_5(cur_data.clone(), cur_datetime.clone());
            self.update_15(cur_data.clone(), cur_datetime.clone());
            self.update_30(cur_data.clone(), cur_datetime.clone());
            self.update_60(cur_data.clone(), cur_datetime.clone());
            self.dtmin = cur_datetime[14..16].parse().unwrap();
        }
        if &cur_datetime[14..16] != self.dtmin {
            let min_f = &cur_datetime[14..16];
            match min_f {
                "00" => {
                    self.update_1(cur_data.clone(), cur_datetime.clone());
                    self.update_5(cur_data.clone(), cur_datetime.clone());
                    self.update_15(cur_data.clone(), cur_datetime.clone());
                    self.update_30(cur_data.clone(), cur_datetime.clone());
                    self.update_60(cur_data.clone(), cur_datetime.clone());
                }
                "30" => {
                    self.update_1(cur_data.clone(), cur_datetime.clone());
                    self.update_5(cur_data.clone(), cur_datetime.clone());
                    self.update_15(cur_data.clone(), cur_datetime.clone());
                    self.update_30(cur_data.clone(), cur_datetime.clone());
                }
                "15" | "45" => {
                    self.update_1(cur_data.clone(), cur_datetime.clone());
                    self.update_5(cur_data.clone(), cur_datetime.clone());
                    self.update_15(cur_data.clone(), cur_datetime.clone());
                }
                "05" | "10" | "20" | "25" | "35" | "40" | "50" | "55" => {
                    self.update_1(cur_data.clone(), cur_datetime.clone());
                    self.update_5(cur_data.clone(), cur_datetime.clone());
                }
                _ => {
                    self.update_1(cur_data.clone(), cur_datetime.clone());
                }
            }
        } else {
            let mut lastdata: QAKlineBase = self.min1.pop().unwrap();
            lastdata.update(data.clone());
            self.min1.push(lastdata);
            let mut lastdata: QAKlineBase = self.min5.pop().unwrap();
            lastdata.update(data.clone());
            self.min5.push(lastdata);
            let mut lastdata: QAKlineBase = self.min15.pop().unwrap();
            lastdata.update(data.clone());
            self.min15.push(lastdata);
            let mut lastdata: QAKlineBase = self.min30.pop().unwrap();
            lastdata.update(data.clone());
            self.min30.push(lastdata);
            let mut lastdata: QAKlineBase = self.min60.pop().unwrap();
            lastdata.update(data.clone());
            self.min60.push(lastdata);
        }
        self.dtmin = cur_datetime[14..16].parse().unwrap();
        println!("dtmin {:?}", self.dtmin);
    }

    fn update_1(&mut self, data: Value, cur_datetime: String) {
        if cur_datetime.len() == 19 {
            println!("create new bar !!");
            //self.min1.pop().unwrap();
            let lastdata = QAKlineBase::init("1min".to_string()).new(data.clone());
            self.min1.push(lastdata);
        } else {
            let mut lastdata: QAKlineBase = self.min1.pop().unwrap();
            lastdata.update(data.clone());
            self.min1.push(lastdata);
        }
    }

    fn update_5(&mut self, data: Value, cur_datetime: String) {
        if cur_datetime.len() == 19 {
            println!("create new bar !!");
            //self.min5.pop().unwrap();
            let lastdata = QAKlineBase::init("5min".to_string()).new(data.clone());
            self.min5.push(lastdata);
        } else {
            let mut lastdata: QAKlineBase = self.min5.pop().unwrap();
            lastdata.update(data.clone());
            self.min5.push(lastdata);
        }
    }
    fn update_15(&mut self, data: Value, cur_datetime: String) {
        if cur_datetime.len() == 19 {
            println!("create new bar !!");
            //self.min15.pop().unwrap();
            let lastdata = QAKlineBase::init("15min".to_string()).new(data.clone());
            self.min15.push(lastdata);
        } else {
            let mut lastdata: QAKlineBase = self.min15.pop().unwrap();
            lastdata.update(data.clone());
            self.min15.push(lastdata);
        }
    }
    fn update_30(&mut self, data: Value, cur_datetime: String) {
        if cur_datetime.len() == 19 {
            println!("create new bar !!");
            //self.min30.pop().unwrap();
            let lastdata = QAKlineBase::init("30min".to_string()).new(data.clone());
            self.min30.push(lastdata);
        } else {
            let mut lastdata: QAKlineBase = self.min30.pop().unwrap();
            lastdata.update(data.clone());
            self.min30.push(lastdata);
        }
    }
    fn update_60(&mut self, data: Value, cur_datetime: String) {
        if cur_datetime.len() == 19 {
            println!("create new bar !!");
            //self.min60.pop().unwrap();
            let lastdata = QAKlineBase::init("60min".to_string()).new(data.clone());
            self.min60.push(lastdata);
        } else {
            let mut lastdata: QAKlineBase = self.min60.pop().unwrap();
            lastdata.update(data.clone());
            self.min60.push(lastdata);
        }
    }
    fn print(&mut self) {
        print!("MIN1 \n\r{:?}\n\r", self.min1);
        print!("MIN5 \n\r{:?}\n\r", self.min5);
        print!("MIN15 \n\r{:?}\n\r", self.min15);
        print!("MIN30 \n\r{:?}\n\r", self.min30);
        print!("MIN60 \n\r{:?}\n\r", self.min60);
    }
    fn to_json(&mut self) -> String {
        let jdata = serde_json::to_string(&self).unwrap();
        //println!("\nthis is json{:#?}\n", jdata);
        jdata
    }
    fn to_1min_json(&mut self) -> String {
        let mut lastdata: QAKlineBase = self.min1.pop().unwrap();
        let jdata = serde_json::to_string(&lastdata).unwrap();
        self.min1.push(lastdata);

        //println!("\nthis is json{:#?}\n", jdata);
        jdata
    }
    fn to_5min_json(&mut self) -> String {
        let mut lastdata: QAKlineBase = self.min5.pop().unwrap();
        let jdata = serde_json::to_string(&lastdata).unwrap();
        self.min5.push(lastdata);

        jdata
    }
    fn to_15min_json(&mut self) -> String {
        let mut lastdata: QAKlineBase = self.min15.pop().unwrap();
        let jdata = serde_json::to_string(&lastdata).unwrap();
        self.min15.push(lastdata);
        jdata
    }
    fn to_30min_json(&mut self) -> String {
        let mut lastdata: QAKlineBase = self.min30.pop().unwrap();
        let jdata = serde_json::to_string(&lastdata).unwrap();
        self.min30.push(lastdata);

        jdata
    }
    fn to_60min_json(&mut self) -> String {
        let mut lastdata: QAKlineBase = self.min60.pop().unwrap();
        let jdata = serde_json::to_string(&lastdata).unwrap();
        self.min60.push(lastdata);

        jdata
    }
}
