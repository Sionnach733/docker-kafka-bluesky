# docker-kafka-bluesky
Sending a live social media stream across Kafka in a Docker environment.

Originally this streamed tweets via twitter4j, but the Twitter v1.1 streaming API
has been retired. The producer now reads public Bluesky posts from
[Jetstream](https://github.com/bluesky-social/jetstream), which is free and needs no account or keys.

## Requirements
- Docker with Docker Compose v2 (Docker Desktop is fine). The Java code is built
  inside Docker, so you don't need a local JDK or Maven.

## Run
From `Docker-dir/`:

```sh
docker compose up --build
```

This starts:
- a single-node Kafka broker (KRaft mode, no ZooKeeper)
- a one-shot job that creates the `HelloKafkaTopic` topic
- `java-producer` (`MyBlueskyStream`): sends Bluesky posts matching your keywords to Kafka
- `java-consumer` (`MyKafkaConsumer`): prints every message it receives

Follow just the consumer output with `docker compose logs -f java-consumer`.

### Choosing keywords
Set `KEYWORDS` (comma-separated, case-insensitive) in `Docker-dir/.env`
(copy `.env.example`) or inline:

```sh
KEYWORDS="kafka,docker" docker compose up --build
```

An empty `KEYWORDS` sends every post, which is many per second.

### Send messages from the console
In a second terminal, from `Docker-dir/`:

```sh
docker compose run --rm console-producer
```

Type lines; each one appears in the consumer's output.

## Configuration
| Variable | Default | Purpose |
| --- | --- | --- |
| `KEYWORDS` | `north korea` | Comma-separated keywords to match in post text |
| `KAFKA_TOPIC` | `HelloKafkaTopic` | Topic to produce to and consume from |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9094` (`kafka:9092` in compose) | Broker address |
| `JETSTREAM_URL` | `wss://jetstream2.us-east.bsky.network/subscribe` | Jetstream instance to connect to |

## Running from the host / IDE
The broker is exposed on `localhost:9094`, so you can run any of the classes from
your IDE (in IntelliJ, open `pom.xml` as a Maven project) or with `mvn package && java -cp target/DockerMultiContainerKafka.jar MyBlueskyStream`
while the `kafka` container is running.
