use std::collections::HashMap;

use testcontainers::{core::WaitFor, Image, ImageArgs};

const NAME: &str = "apachepulsar/pulsar";
const TAG: &str = "3.2.0";

pub const PULSAR_PORT: u16 = 6650;
pub const ZOOKEEPER_PORT: u16 = 2181;
pub const BROKER_HTTP_PORT: u16 = 8080;
pub const BOOKKEEPER_PORT: u16 = 3181;

/// Module to work with [`Pulsar`] inside of tests.
///
/// Starts an instance of Pulsar.
/// This module is based on the official [`Pulsar docker image`].
///
/// Default db name, user and password is `postgres`.
///
/// [`Pulsar`]: https://pulsar.apache.org
/// [`Pulsar docker image`]: https://hub.docker.com/r/apachepulsar/pulsar

#[derive(Debug, Default, Clone)]
pub struct PulsarArgs;

impl ImageArgs for PulsarArgs {
    fn into_iterator(self) -> Box<dyn Iterator<Item = String>> {
        Box::new(
            vec![
                "/bin/sh".to_owned(),
                "-c".to_owned(),
                "/pulsar/bin/apply-config-from-env.py /pulsar/conf/standalone.conf && bin/pulsar standalone".to_owned()
            ]
            .into_iter(),
        )
    }
}

#[derive(Debug)]
pub struct Pulsar;

impl Image for Pulsar {
    type Args = PulsarArgs;

    fn name(&self) -> String {
        NAME.to_owned()
    }

    fn tag(&self) -> String {
        TAG.to_owned()
    }

    // WaitFor::Healthcheck not possible because no healthcheck in Docker Pulsar image
    // WaitFor::Http does not exist in testcontainers-rs
    // Healthcheck on the last log line for now
    fn ready_conditions(&self) -> Vec<WaitFor> {
        vec![WaitFor::message_on_stdout(
            "org.apache.pulsar.functions.worker.SchedulerManager - Schedule summary - execution time"
        )]
    }

    fn expose_ports(&self) -> Vec<u16> {
        vec![PULSAR_PORT, ZOOKEEPER_PORT, BROKER_HTTP_PORT, BOOKKEEPER_PORT]
    }
}

#[cfg(test)]
mod tests {
    use testcontainers::clients;

    use super::*;

    use pulsar::{
        message::proto, producer, Error as PulsarError, Pulsar as PulsarClient, SerializeMessage, TokioExecutor,
    };

    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize)]
    struct TestData {
        data: String,
    }

    impl SerializeMessage for TestData {
        fn serialize_message(input: Self) -> Result<producer::Message, PulsarError> {
            let payload = serde_json::to_vec(&input).map_err(|e| PulsarError::Custom(e.to_string()))?;
            Ok(producer::Message {
                payload,
                ..Default::default()
            })
        }
    }

    #[tokio::test]
    async fn pulsar_producer() -> Result<(), pulsar::Error> {
        let docker = clients::Cli::default();
        let node = docker.run(Pulsar);

        let broker_url = &format!(
            "pulsar://localhost:{}",
            node.get_host_port_ipv4(PULSAR_PORT)
        );

        let pulsar: PulsarClient<_> = PulsarClient::builder(broker_url, TokioExecutor)
            .build()
            .await?;

        let mut producer = pulsar
            .producer()
            .with_topic("non-persistent://public/default/my-topic")
            .with_name("my-producer")
            .with_options(producer::ProducerOptions {
                schema: Some(proto::Schema {
                    r#type: proto::schema::Type::String as i32,
                    ..Default::default()
                }),
                ..Default::default()
            })
            .build()
            .await?;

        producer
            .send(TestData {
                data: "Hello world!".to_string(),
            })
            .await?;

        Ok(())
    }
}
