
use amiquip::{
    Connection, ConsumerMessage, ConsumerOptions, ExchangeDeclareOptions, ExchangeType, FieldTable,
    QueueDeclareOptions, Result, Publish
};
use amiquip::Delivery;

use crossbeam_channel::Sender;
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
        // Start a consumer. Use no_ack: true so the server doesn't wait for us to ack
        // the messages it sends us.
        let consumer = queue.consume(ConsumerOptions {
            no_ack: true,
            ..ConsumerOptions::default()
        })?;

        for (_i, message) in consumer.receiver().iter().enumerate() {
            match message {
                ConsumerMessage::Delivery(delivery) => {
                    let body = String::from_utf8_lossy(&delivery.body);
//                    println!("({:>3}) {}:{}", i, delivery.routing_key, body);body.to_string()
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


fn main() {
    let code = "rb2001".to_string();

    thread::spawn(move || {
        let mut client = QAEventMQ{
            amqp: "amqp://admin:admin@127.0.0.1:5672/".to_string(),
            exchange: "CTPX".to_string(),
            model: "direct".to_string(),
            routing_key: code,
        };
        println!("xxx");
//        QAEventMQ::consume(client, s1).unwrap();
        client.consume();
    });

}