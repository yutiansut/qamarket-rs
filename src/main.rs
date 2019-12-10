
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

        println!("{:?}",data);

        ws_event_tx.send(data).unwrap();

    }

}


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


pub struct QASeires{
    pub tick: Vec<QAKlineBase>
}

fn main() {
    let code = "IF1912".to_string();
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

    loop{
        let data = r1.recv().unwrap();
        let resx:Value = serde_json::from_str(&data).unwrap();
        println!("receive !!{:?}",resx);
        index.push(resx["datetime"].as_str().unwrap().parse().unwrap());
        values.push(resx["last_price"].as_f64().unwrap());
        println!("{:#?}", index);
        println!("{:#?}", values);
        

    }
}



impl QAKlineBase{
    fn update(&mut self, data:Value) {
        let new_price = data["last_price"].as_f64().unwrap();
        if (self.high < new_price){
            self.high = new_price;
        }
        if (self.low > new_price){
            self.low = new_price;
        }
        self.close = new_price;
        self.datetime = data["datetime"].as_str().unwrap().parse().unwrap();
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


}