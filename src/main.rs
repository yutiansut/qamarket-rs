
use amiquip::{
    Connection, ConsumerMessage, ConsumerOptions, ExchangeDeclareOptions, ExchangeType, FieldTable,
    QueueDeclareOptions, Result, Publish
};
use amiquip::Delivery;
use std::thread;
extern crate crossbeam_utils;

use crossbeam_channel::bounded;
use crossbeam_channel::Sender;
use serde_json::value::Value;
extern crate ndarray;
use ndarray::{array};
mod dataframe;
use crate::dataframe::DataCell;
use serde_json::json;
use serde::{Deserialize, Serialize};

pub struct QAEventMQ{
    pub amqp : String,
    pub exchange: String,
    pub model: String,
    pub routing_key: String
}



impl QAEventMQ{
    pub fn consume(
        eventmq: QAEventMQ,
        ws_event_tx: Sender<String>
    ) -> Result<()>{
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


    pub fn callback(eventmq: &QAEventMQ,
                    message: &Delivery,
                    ws_event_tx:  &Sender<String>
    ){
        let msg  = message.body.clone();
        let foo = String::from_utf8(msg).unwrap();
        let data = foo.to_string();

        //println!("{:?}",data);
        ws_event_tx.send(data).unwrap();

    }

}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QAKlineBase{
    pub datetime: String,
    pub updatetime: String,
    pub code: String,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,


}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QASeries{
    pub tick: Vec<QAKlineBase>,
    dtmin: String //dtmin是用于控制分钟线生成的
}

fn main() {
    let code = "au2002".to_string();
    let (s1, r1) = bounded(0);
    thread::spawn(move || {
        let mut client = QAEventMQ{
            amqp: "amqp://admin:admin@127.0.0.1:5672/".to_string(),
            exchange: "CTPX".to_string(),
            model: "direct".to_string(),
            routing_key: code,
        };
        println!("xxx");
        QAEventMQ::consume(client, s1).unwrap();
    });

    let mut index: Vec<String> = vec![];
    let mut values:  Vec<f64> = vec![];


    let mut kline = QASeries::init();

    loop{
        let data = r1.recv().unwrap();
        let resx:Value = serde_json::from_str(&data).unwrap();

        kline.update(resx);
        kline.print();
        kline.to_json();
    }
}



impl QAKlineBase{
    fn init() -> QAKlineBase {
        QAKlineBase{
            datetime: "".to_string(),
            updatetime: "".to_string(),
            code: "".to_string(),
            open: 0.0,
            high: 0.0,
            low: 0.0,
            close: 0.0,
            volume: 0.0,
        }
    }

    fn update(&mut self, data:Value) {

        let p1 = "datetime";
        let p2 = "last_price";
        let p3 = "volume";

        if self.open == 0.0{
            self.init_data(data.clone());
        }

        let new_price = data["last_price"].as_f64().unwrap();
        if (self.high < new_price){
            self.high = new_price;
        }
        if (self.low > new_price){
            self.low = new_price;
        }
        self.close = new_price;
        let cur_datetime:String = data["datetime"].as_str().unwrap().parse().unwrap();
        self.updatetime = cur_datetime.clone();

    }

    fn init_data(&mut self, data:Value) {


        self.datetime= data["datetime"].as_str().unwrap().parse().unwrap();
        self.updatetime=data["datetime"].as_str().unwrap().parse().unwrap();
        self.code=data["symbol"].as_str().unwrap().parse().unwrap();
        self.open=  data["last_price"].as_f64().unwrap();
        self.high= data["last_price"].as_f64().unwrap();
        self.low= data["last_price"].as_f64().unwrap();
        self.close= data["last_price"].as_f64().unwrap();
        self.volume=data["volume"].as_f64().unwrap();

    }

    fn print(&mut self){
        println!("\n\r {:?}\n\r", row![self.datetime.clone(), self.code.clone(), self.open, self.high, self.low, self.close, self.volume]);
    }


    fn new(&mut self, data:Value) -> QAKlineBase{

        let data = QAKlineBase{
            datetime: data["datetime"].as_str().unwrap().parse().unwrap(),
            updatetime:data["datetime"].as_str().unwrap().parse().unwrap(),
            code: data["datetime"].as_str().unwrap().parse().unwrap(),
            open:  data["last_price"].as_f64().unwrap(),
            high: data["last_price"].as_f64().unwrap(),
            low: data["last_price"].as_f64().unwrap(),
            close: data["last_price"].as_f64().unwrap(),
            volume:data["volume"].as_f64().unwrap(),
        };
        data
    }
    fn to_json(&mut self) {
        let jdata= serde_json::to_string(&self).unwrap();
        println!("this is json{:#?}", jdata);

    }

}

impl QASeries{
    fn init() -> QASeries {
        QASeries{
            tick:vec![],
            dtmin: "99".to_string(),
        }
    }
    fn update(&mut self, data:Value) {
        let cur_data = data.clone();
        let cur_datetime:String = cur_data["datetime"].as_str().unwrap().parse().unwrap();
        if &cur_datetime[14..16] != self.dtmin{
            if cur_datetime.len() == 19 {
                println!("create new bar !!");
                let lastdata = QAKlineBase::init().new(data.clone());
                self.tick.push(lastdata);
            }else{
                let mut lastdata:QAKlineBase = self.tick.pop().unwrap();
                lastdata.update(data.clone());
                self.tick.push(lastdata);
            }
        } else{
            let mut lastdata:QAKlineBase = self.tick.pop().unwrap();
            lastdata.update(data.clone());
            self.tick.push(lastdata);
        }
        self.dtmin = cur_datetime[14..16].parse().unwrap();
        println!("dtmin {:?}", self.dtmin);


    }
    fn print(&mut self){
        print!("\n\r{:?}\n\r", self.tick);
    }
    fn to_json(&mut self){
        let jdata= serde_json::to_string(&self).unwrap();
        println!("\nthis is json{:#?}\n", jdata);
    }
}