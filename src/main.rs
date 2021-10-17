use main_error::MainError;
use rand::{distributions::Alphanumeric, Rng};
use rumqttc::{AsyncClient, ClientError, ConnectionError, Event, MqttOptions, Packet, QoS};
use rust_decimal::prelude::*;
use sqlx::postgres::PgPool;
use std::time::Duration;

#[derive(sqlx::FromRow, Debug)]
struct Row {
    price: Decimal,
}

async fn msg_heaters(client: AsyncClient, status: bool) -> Result<(), ClientError> {
    let heaters = vec!["0xb4e3f9fffe19faf7", "0xb4e3f9fffe206084"];

    let toggle = match status {
        true => "ON",
        false => "OFF",
    };

    let state = format!("{{\"state\":\"{}\"}}", toggle);

    for i in heaters {
        let h = format!("zigbee2mqtt/{}/set", i);
        client
            .publish(h, QoS::ExactlyOnce, false, state.as_bytes())
            .await
            .unwrap();
    }
    //
    tokio::time::sleep(Duration::from_secs(1)).await;

    client.cancel().await
}

#[tokio::main]
async fn main() -> Result<(), MainError> {
    let pgsql_url = match std::env::var("POSTGRESQL_URL") {
        Ok(val) => val,
        Err(_e) => {
            let v = "postgresql:/sensors";
            println!("POSTGRESQL_URL not defined, using default: '{}", v);
            v.to_string()
        }
    };
    let mqtt_url = match std::env::var("MQTT_URL") {
        Ok(val) => val,
        Err(_e) => {
            let v = "localhost";
            println!("MQTT_URL not defined, using default: '{}'", v);
            v.to_string()
        }
    };
    let mqtt_port: u16 = match std::env::var("MQTT_PORT") {
        Ok(val) => val.parse().unwrap_or(1883),
        Err(_e) => {
            let v = 1883;
            println!("MQTT_PORT not defined, using default: '{}'", v);
            v
        }
    };
    let s: String = rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(7)
        .map(char::from)
        .collect();

    let pool = PgPool::connect(&pgsql_url).await?;

    // Look up current price
    let price_lookup = sqlx::query_as::<_, Row>(
        "
        SELECT
            price * 1.2 AS price
        FROM nordpool_price
        WHERE \"time\" @> now()::timestamp AND region = $1",
    )
    .bind("ee".to_string())
    .fetch_one(&pool)
    .await
    .unwrap();

    let price = price_lookup.price;

    // Fancy AI-based (hah??) decision tree
    let status: bool = if price > rust_decimal::Decimal::from_str("100.0").unwrap() {
        false
    } else {
        true
    };

    println!("Current price: {:?}, heater status: {:?}", price, status);

    let mut mqttoptions = MqttOptions::new(
        ["home-faerie", "heating-manager", &s].join("-"),
        mqtt_url,
        mqtt_port,
    );
    mqttoptions.set_keep_alive(5);
    mqttoptions.set_max_packet_size(512 * 1024, 512 * 1024);

    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);

    tokio::task::spawn(async move {
        msg_heaters(client, status).await.unwrap();
        tokio::time::sleep(Duration::from_secs(2)).await;
    });

    loop {
        let event = eventloop.poll().await;
        if event.is_err() {
            println!("{:?}", event);
            break;
        }
        println!("{:?}", event.unwrap());
    }

    Ok(())
}
