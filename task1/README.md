# Task 1 — Local Kafka & Redpanda Multi-Broker Clusters

## Directory layout

```
task1/
├── kafka/
│   └── docker-compose.yml   # 3-broker Apache Kafka KRaft cluster + kafka-ui
└── redpanda/
    └── docker-compose.yml   # 3-broker Redpanda cluster + Redpanda Console
```

---

## Part 1 — Apache Kafka (KRaft, 3 brokers)

### Architecture

```mermaid
graph TD
  subgraph KRaft Cluster
    K0[kafka-0<br/>broker + controller<br/>:9092]
    K1[kafka-1<br/>broker + controller<br/>:9093]
    K2[kafka-2<br/>broker + controller<br/>:9094]
  end
  UI[kafka-ui<br/>:8080] --> K0
  UI --> K1
  UI --> K2
  K0 <-->|Raft consensus| K1
  K1 <-->|Raft consensus| K2
```

**KRaft mode** (Kafka 3.x): no ZooKeeper — each broker also participates in the Raft-based controller quorum.  
Replication factor 3 → every partition survives loss of 1 broker.

### Start the cluster

```bash
cd task1/kafka
docker compose up -d
docker ps
```
![Docker](screenshots/img.png)


### Kafka CLI — all commands run via `docker exec`

#### 1. Create a topic

```bash
docker exec kafka-0 kafka-topics \
  --bootstrap-server kafka-0:9092,kafka-1:9092,kafka-2:9092 \
  --create \
  --topic demo.messages.sent \
  --partitions 3 \
  --replication-factor 3
```
![Create topic](screenshots/img_1.png)

#### 2. List topics

```bash
docker exec kafka-0 kafka-topics \
  --bootstrap-server kafka-0:9092,kafka-1:9092,kafka-2:9092 \
  --list
```
![Topics list](screenshots/img_2.png)

#### 3. Describe the topic (bonus — shows partition leaders and replicas)

```bash
docker exec kafka-0 kafka-topics \
  --bootstrap-server kafka-0:9092,kafka-1:9092,kafka-2:9092 \
  --describe \
  --topic demo.messages.sent
```
![Topic describe](screenshots/img_3.png)

#### 4. Produce at least 10 messages (console producer)

Type each line and press Enter; press `Ctrl+C` to stop.

```bash
docker exec -it kafka-0 kafka-console-producer \
  --bootstrap-server kafka-0:9092,kafka-1:9092,kafka-2:9092 \
  --topic demo.messages.sent
```
![Console producer](screenshots/img_17.png)

#### 5. Consume messages (console consumer)

Open a second terminal. `--from-beginning` replays all stored messages.

```bash
docker exec -it kafka-0 kafka-console-consumer \
  --bootstrap-server kafka-0:9092,kafka-1:9092,kafka-2:9092 \
  --topic demo.messages.sent \
  --from-beginning
```
![Console consumer](screenshots/img_18.png)


#### 6. Delete the topic

```bash
docker exec kafka-0 kafka-topics \
  --bootstrap-server kafka-0:9092,kafka-1:9092,kafka-2:9092 \
  --delete \
  --topic demo.messages.sent
```
![Delete topic](screenshots/img_6.png)

```bash
# Show KRaft quorum leader
docker exec kafka-0 kafka-metadata-quorum \
  --bootstrap-server kafka-0:9092 \
  describe --status
```
![Quorum leader](screenshots/img_7.png)

### Stop and clean up

```bash
docker compose down -v   # removes containers AND volumes
```

---

## Part 2 — Redpanda (3 brokers)

### Architecture

```mermaid
graph TD
  subgraph Redpanda Raft Cluster
    R0[redpanda-0<br/>seed node<br/>kafka:19092 admin:9644]
    R1[redpanda-1<br/>kafka:19093]
    R2[redpanda-2<br/>kafka:19094]
  end
  CON[redpanda-console<br/>:8081] --> R0
  R1 -->|joins via seed| R0
  R2 -->|joins via seed| R0
```

Redpanda uses its own Raft-based consensus — no ZooKeeper, no KRaft.  
It is Kafka-API compatible: standard Kafka clients work without modification.

### Start the cluster

```bash
cd task1/redpanda
docker compose up -d
docker ps
```
![Redpanda cluster](screenshots/img_8.png)

#### 1. Check cluster health

```bash
docker exec redpanda-0 rpk cluster health \
  --api-urls redpanda-0:9644
```
![Cluster health](screenshots/img_9.png)

#### 2. List brokers

```bash
docker exec redpanda-0 rpk cluster info \
  --brokers redpanda-0:9092,redpanda-1:9092,redpanda-2:9092
```
![Cluster info](screenshots/img_10.png)

#### 3. Create a topic

```bash
docker exec redpanda-0 rpk topic create demo.messages.sent \
  --brokers redpanda-0:9092,redpanda-1:9092,redpanda-2:9092 \
  --partitions 3 \
  --replicas 3
```
![Create topic](screenshots/img_11.png)

#### 4. List topics

```bash
docker exec redpanda-0 rpk topic list \
  --brokers redpanda-0:9092,redpanda-1:9092,redpanda-2:9092
```
![List topics](screenshots/img_12.png)

#### 5. Describe the topic

```bash
docker exec redpanda-0 rpk topic describe demo.messages.sent \
  --brokers redpanda-0:9092,redpanda-1:9092,redpanda-2:9092
```
![Describe topic](screenshots/img_13.png)

#### 6. Produce at least 10 messages
```bash
docker exec -it redpanda-0 rpk topic produce demo.messages.sent \
  --brokers redpanda-0:9092,redpanda-1:9092,redpanda-2:9092
```
![Produce messages](screenshots/img_15.png)

#### 7. Consume messages

```bash
docker exec -it redpanda-0 rpk topic consume demo.messages.sent \
  --brokers redpanda-0:9092,redpanda-1:9092,redpanda-2:9092 \
  --offset start
```
![Consume messages](screenshots/img_14.png)

#### 8. Delete the topic

```bash
docker exec redpanda-0 rpk topic delete demo.messages.sent \
  --brokers redpanda-0:9092,redpanda-1:9092,redpanda-2:9092
```
![Delete topic](screenshots/img_16.png)
---

## Key differences: Kafka vs Redpanda

| Feature | Apache Kafka | Redpanda |
|---|---|---|
| Consensus | KRaft (Raft in JVM) | Native Raft (C++) |
| Runtime | JVM | Native binary |
| ZooKeeper | Not needed (3.x+) | Never needed |
| CLI | `kafka-*.sh` scripts | `rpk` single binary |
| Schema Registry | Separate service | Built-in |
| HTTP Proxy | Separate service (REST Proxy) | Built-in (PandaProxy) |
| Throughput | High | Higher (lower latency) |
| Kafka API compat | 100% (it is Kafka) | ~100% (compatible) |

---
## Conclusion
- Both clusters are up and running with 3 brokers each, using Raft-based consensus without ZooKeeper.
- We successfully created topics, produced and consumed messages, and managed the clusters using their respective CLIs.
- Redpanda offers a more streamlined experience with built-in features and better performance, while Kafka has a more mature ecosystem and wider adoption.