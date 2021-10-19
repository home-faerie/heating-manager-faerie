use log::{debug, error, info, trace, warn};
use main_error::MainError;
use rand::{distributions::Alphanumeric, Rng};
use rumqttc::{AsyncClient, ClientError, MqttOptions, QoS};
use rust_decimal::prelude::*;
use sqlx::postgres::PgPool;
use std::io::Write;

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
    client.cancel().await
}

#[tokio::main]
async fn main() -> Result<(), MainError> {
    match std::env::var("RUST_LOG_STYLE").as_deref() {
        Ok(s) if s == "SYSTEMD" => env_logger::builder()
            .format(|buf, record| {
                writeln!(
                    buf,
                    "<{}>{}: {}",
                    match record.level() {
                        log::Level::Error => 3,
                        log::Level::Warn => 4,
                        log::Level::Info => 6,
                        log::Level::Debug => 7,
                        log::Level::Trace => 7,
                    },
                    record.target(),
                    record.args()
                )
            })
            .init(),
        _ => env_logger::init(),
    };

    let pgsql_url = match std::env::var("POSTGRESQL_URL") {
        Ok(val) => val,
        Err(_e) => {
            let v = "postgresql:/sensors";
            warn!("POSTGRESQL_URL not defined, using default: '{}", v);
            v.to_string()
        }
    };
    let mqtt_url = match std::env::var("MQTT_URL") {
        Ok(val) => val,
        Err(_e) => {
            let v = "localhost";
            warn!("MQTT_URL not defined, using default: '{}'", v);
            v.to_string()
        }
    };
    let mqtt_port: u16 = match std::env::var("MQTT_PORT") {
        Ok(val) => val.parse().unwrap_or(1883),
        Err(_e) => {
            let v = 1883;
            warn!("MQTT_PORT not defined, using default: '{}'", v);
            v
        }
    };
    let s: String = rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(7)
        .map(char::from)
        .collect();

    let pool = match PgPool::connect(&pgsql_url).await {
        Ok(r) => r,
        Err(e) => {
            error!("Unable to connect to database: {}", e);
            std::process::exit(1);
        }
    };

    // Look up current price
    let result = sqlx::query_as::<_, Row>(
        "
        SELECT
            price * 1.2 AS price
        FROM nordpool_price
        WHERE \"time\" @> now()::timestamp AND region = $1",
    )
    .bind("ee".to_string())
    .fetch_one(&pool)
    .await;

    let price = match result {
        Ok(row) => row.price,
        Err(e) => {
            error!("Unable to look up price: {}", e);
            std::process::exit(1);
        }
    };

    // Fancy AI-based (hah??) decision tree
    let status: bool = price <= rust_decimal::Decimal::from_str("100.0").unwrap();

    info!("Current price: {:?}, heater status: {:?}", price, status);

    let mut mqttoptions = MqttOptions::new(
        ["home-faerie", "heating-manager", &s].join("-"),
        mqtt_url,
        mqtt_port,
    );
    mqttoptions.set_max_packet_size(512 * 1024, 512 * 1024);

    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);

    tokio::task::spawn(async move {
        msg_heaters(client, status).await.unwrap();
    });

    loop {
        let event = eventloop.poll().await;
        match event {
            Ok(e) => {
                trace!("Received event: {:?}", e);
            },
            Err(rumqttc::ConnectionError::Cancel) => {
                debug!("Received Cancel Event, exiting...");
                break;
            },
            Err(_) => {
                error!("Error: {:?}", event);
                break;
            }
        };
    }

    Ok(())
}
