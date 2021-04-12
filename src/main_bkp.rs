#[macro_use]
extern crate log;
extern crate lapin;
extern crate async_global_executor;
extern crate futures_lite;

use dotenv::dotenv;
use std::env;
use lapin::{Connection, ConnectionProperties, BasicProperties, types::FieldTable};
use lapin::options::{QueueDeclareOptions, BasicConsumeOptions, BasicPublishOptions, BasicAckOptions};
use futures_lite::{StreamExt};
use std::{thread, time};

fn main() {
    env_logger::init();

    async_global_executor::block_on(async {
        let conn = Connection::connect("amqp://rmq:rmq@127.0.0.1:5672/%2f", ConnectionProperties::default()).await.expect("Error when connecting to RabbitMQ");
        if let Ok(channel) = conn.create_channel().await {
            if let Ok(queue) = channel.queue_declare(
                "TEST_MESSAGE",
                QueueDeclareOptions::default(),
                FieldTable::default()).await {
                info!("Declared queue {:?}", queue);

                if let Ok(mut consumer) = channel.basic_consume(
                    "TEST_MESSAGE",
                    "test_message_consumer",
                    BasicConsumeOptions::default(),
                    FieldTable::default(),
                ).await {
                    async_global_executor::spawn(async move {
                        info!("Will consume");
                        while let Some(delivery) = consumer.next().await {
                            info!("Message received.");
                            let (_, delivery) = delivery.expect("error in mail consumer");
                            let body = String::from_utf8_lossy(&*delivery.data);
                            println!("Received [{}]", body);
                            delivery
                                .ack(BasicAckOptions::default())
                                .await
                                .expect("ack");
                        }
                        info!("Stopped consume");
                    }).detach();
                }
            }
        }
    });

    loop {
        async_global_executor::block_on(async {
            let conn = Connection::connect("amqp://rmq:rmq@127.0.0.1:5672/%2f", ConnectionProperties::default()).await.expect("Error when connecting to RabbitMQ");
            if let Ok(channel) = conn.create_channel().await {

                match channel
                    .basic_publish(
                        "",
                        "TEST_MESSAGE",
                        BasicPublishOptions::default(),
                        Vec::from(String::from("Test...")),
                        BasicProperties::default(),
                    )
                    .await {
                    Ok(pub_conf) => {
                        match pub_conf.await {
                            Ok(_) => {
                                info!("PubConf OK");
                            }
                            Err(e) => {
                                error!("Error when publising: {}", e);
                            }
                        }
                    }
                    Err(e) => {
                        error!("Error when publising: {}", e);
                    }
                }

                info!("OK -> notify complete");
            }
        });

        let ten_millis = time::Duration::from_secs(10);
        thread::sleep(ten_millis);
    }
}
