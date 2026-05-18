# IoT Traffic Speed Alerting Pipeline — Architecture

## System Overview

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                              IoT TRAFFIC SPEED ALERTING PIPELINE                         │
└─────────────────────────────────────────────────────────────────────────────────────────┘

┌──────────────┐     ┌──────────────────────────────────────────────────────────────────┐
│              │     │                     KAFKA CLUSTER                                │
│  IoT         │     │                                                                  │
│  Cameras     │     │  ┌────────────────────┐  ┌─────────────────┐  ┌──────────────┐  │
│  (Producers) │────▶│  │ iot.camera-        │  │ iot.traffic.    │  │ iot.camera-  │  │
│              │     │  │ events.raw         │  │ alerts          │  │ registry.    │  │
│  {camera_id, │     │  │                    │  │                 │  │ changes      │  │
│   event_time,│     │  │ 16 partitions      │  │ 4 partitions    │  │              │  │
│   plate_text,│     │  │ Partitioned by     │  │ 30-day retention│  │ 1 partition  │  │
│   confidence}│     │  │ plate_text         │  │                 │  │ Compact      │  │
│              │     │  │ 7-day retention    │  │                 │  │              │  │
└──────────────┘     │  └─────────┬──────────┘  └────────┬────────┘  └──────┬───────┘  │
                     └────────────┼──────────────────────┼──────────────────┼──────────┘
                                  │                      │                  │
     ┌────────────────────────────┼──────────────────────┼──────────────────┼──────────┐
     │                            │     FLINK CLUSTER    │                  │          │
     │                            │                      │                  │          │
     │  ┌─────────────────────────▼──────────────────────▼──────────────────▼────────┐ │
     │  │                         TRAFFIC SPEED JOB                                 │ │
     │  │                                                                            │ │
     │  │                                                                            │ │
     │  │   ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────────────┐    │ │
     │  │   │ Stage 1  │    │ Stage 2  │    │ Stage 3  │    │    Stage 4       │    │ │
     │  │   │          │    │          │    │          │    │                  │    │ │
     │  │   │  Source  │───▶│  Filter  │───▶│  Dedup   │───▶│  Speed           │    │ │
     │  │   │          │    │          │    │          │    │  Calculation     │    │ │
     │  │   │  Kafka   │    │ conf >=  │    │ 5s tumbl │    │                  │    │ │
     │  │   │  Consumer│    │  0.80    │    │ window   │    │  KeyedProcess    │    │ │
     │  │   │          │    │          │    │          │    │  (per plate)     │    │ │
     │  │   └──────────┘    └──────────┘    └──────────┘    │                  │    │ │
     │  │                                                   │  State:          │    │ │
     │  │                                                   │  PlateState      │    │ │
     │  │                                                   │  {lastCamera,    │    │ │
     │  │                                                   │   lastTime,      │    │ │
     │  │                                                   │   lastLat/Lon}   │    │ │
     │  │                                                   └────────┬─────────┘    │ │
     │  │                                                            │              │ │
     │  │                                                            ▼              │ │
     │  │                                                   ┌──────────────────┐    │ │
     │  │                                                   │    Stage 5       │    │ │
     │  │                                                   │                  │    │ │
     │  │                                                   │  Windowed Agg    │    │ │
     │  │                                                   │  + Congestion    │    │ │
     │  │                                                   │    Detection     │    │ │
     │  │                                                   │                  │    │ │
     │  │                                                   │  60min sliding   │    │ │
     │  │                                                   │  1min slide      │    │ │
     │  │                                                   │  Key: road_seg   │    │ │
     │  │                                                   │                  │    │ │
     │  │                                                   │  Logic:          │    │ │
     │  │                                                   │  1. AVG(speed)   │    │ │
     │  │                                                   │  2. threshold =  │    │ │
     │  │                                                   │     avg * 0.80   │    │ │
     │  │                                                   │  3. COUNT(slow)  │    │ │
     │  │                                                   │  4. IF slow >=   │    │ │
     │  │                                                   │     10 → ALERT   │    │ │
     │  │                                                   └────────┬─────────┘    │ │
     │  │                                                            │              │ │
     │  │                           ┌────────────────────────────────┤              │ │
     │  │                           │                                │              │ │
     │  │                           ▼                                ▼              │ │
     │  │                  ┌────────────────┐              ┌─────────────────┐       │ │
     │  │                  │  Camera Loc   │              │  Side Output    │       │ │
     │  │                  │  Broadcast    │              │  (Late Events)  │       │ │
     │  │                  │  State        │              │                 │       │ │
     │  │                  │               │              │  Events > 30s   │       │ │
     │  │                  │  Replicated   │              │  late → DLQ     │       │ │
     │  │                  │  to all tasks │              │                 │       │ │
     │  │                  └───────────────┘              └─────────────────┘       │ │
     │  │                                                                            │ │
     │  └──────────────────────────────────────────────┬─────────────────────────────┘ │
     └─────────────────────────────────────────────────┼───────────────────────────────┘
                                                       │
                          ┌────────────────────────────┼────────────────────────────┐
                          │                            │                            │
                          ▼                            ▼                            │
                 ┌────────────────┐          ┌──────────────────┐                   │
                 │     MinIO      │          │   Kafka Connect  │                   │
                 │    (S3)        │          │   (HTTP Sink)    │                   │
                 │                │          │                  │                   │
                 │  s3://raw/     │          │  Retry: 3x       │                   │
                 │    camera-     │          │  Exponential     │                   │
                 │    events/     │          │  Backoff         │                   │
                 │                │          │                  │                   │
                 │  s3://enriched/│          │  DLQ on failure  │                   │
                 │    speed-      │          │                  │                   │
                 │    calculations│          └────────┬─────────┘                   │
                 │                │                   │                             │
                 │  s3://enriched/│                   ▼                             │
                 │    alerts/     │          ┌──────────────────┐                   │
                 │                │          │   Alert Service  │                   │
                 │  Format:       │          │   (External)     │                   │
                 │  Parquet       │          │                  │                   │
                 │  Partitioned   │          │   POST /alerts   │                   │
                 │  by date       │          │   REST API       │                   │
                 └────────────────┘          └──────────────────┘                   │
                                                                                    │
                                                                                    │
                 ┌────────────────┐                                                   │
                 │  Dead Letter   │◀──────────────────────────────────────────────────┘
                 │  Queue         │
                 │                │
                 │  iot.camera-   │
                 │  events.late   │
                 │                │
                 │  4 partitions  │
                 │  30-day retain │
                 └────────────────┘
```

## Data Flow — End to End

```
                    TIME ──────────────────────────────────────────────────────────▶

Camera A                    Camera B
  │                           │
  │  {cam_001, ABC1234,       │  {cam_002, ABC1234,
  │   10:30:00, 0.95}         │   10:32:30, 0.92}
  │                           │
  ▼                           ▼
┌─────────────────────────────────────────────┐
│              KAFKA                          │
│  Topic: iot.camera-events.raw               │
│  Partition: hash(ABC1234) % 16              │
└─────────────────┬───────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────┐
│           FLINK: STAGE 1 — SOURCE           │
│  Deserialize JSON → CameraEvent POJO        │
│  Watermark = max_event_time - 30s           │
└─────────────────┬───────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────┐
│           FLINK: STAGE 2 — FILTER           │
│  confidence >= 0.80?                        │
│  0.95 ✓  │  0.92 ✓                         │
│  (0.60 would be dropped)                    │
└─────────────────┬───────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────┐
│           FLINK: STAGE 3 — DEDUP            │
│  Key: (plate_text, camera_id)               │
│  5-second tumbling window, keep first       │
│  (eliminates duplicate camera triggers)     │
└─────────────────┬───────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────┐
│      FLINK: STAGE 4 — SPEED CALCULATION     │
│                                             │
│  Key: plate_text                            │
│                                             │
│  Event 1 (cam_001): No prior state          │
│    → Store PlateState{cam_001, 10:30:00}    │
│    → Wait for next camera...                │
│                                             │
│  Event 2 (cam_002): Prior state exists      │
│    → cam_001 ≠ cam_002 ✓                    │
│    → Lookup: cam_001 → (40.71, -74.01)      │
│              cam_002 → (40.73, -74.02)      │
│    → Distance: 1.7 miles (Haversine)        │
│    → Time: 2.5 min = 0.0417 hr              │
│    → Speed: 1.7 / 0.0417 = 40.8 mph         │
│    → Emit SpeedEvent{ABC1234, seg_42, 40.8} │
│    → Update state: {cam_002, 10:32:30}      │
│                                             │
│  Side Input: Camera Registry (broadcast)    │
│  {camera_id → (lat, lon, road_segment_id)}  │
└─────────────────┬───────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────┐
│  FLINK: STAGE 5 — WINDOWED AGG & DETECTION  │
│                                             │
│  Key: road_segment_id                       │
│  Window: 60-min sliding, 1-min slide        │
│                                             │
│  Window [10:00 - 11:00] for seg_42:         │
│    50 cars detected                         │
│    avg_speed = 55.2 mph                     │
│    threshold = 55.2 * 0.80 = 44.16 mph      │
│    slow_cars (speed < 44.16) = 14           │
│    14 >= 10 → 🚨 ALERT                      │
│                                             │
│  Output:                                    │
│  {alert_id, seg_42, 10:00, 11:00,           │
│   avg: 55.2, slow: 14, threshold: 44.16}    │
└────────┬──────────────────────┬─────────────┘
         │                      │
         ▼                      ▼
┌─────────────────┐    ┌──────────────────────┐
│  KAFKA TOPIC    │    │  MINIO (S3)          │
│  iot.traffic.   │    │                      │
│  alerts         │    │  s3://raw/           │
│                 │    │    camera-events/    │
│                 │    │  s3://enriched/      │
│                 │    │    speed-calcs/      │
│                 │    │    alerts/           │
└────────┬────────┘    └──────────────────────┘
         │
         ▼
┌─────────────────────┐
│   KAFKA CONNECT     │
│   HTTP Sink         │
│                     │
│   POST /alerts      │
│   Retry: 3x exp.    │
│   backoff           │
└────────┬────────────┘
         │
         ▼
┌─────────────────────┐
│   ALERT SERVICE     │
│   (External REST)   │
└─────────────────────┘
```

## Flink Job Internal Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          FLINK TRAFFIC SPEED JOB                            │
│                                                                             │
│  PARALLELISM: 8-16 task slots                                              │
│  STATE BACKEND: RocksDB (disk-backed)                                      │
│  CHECKPOINTING: 60s interval, incremental, to MinIO                        │
│  SEMANTICS: Exactly-once                                                   │
│                                                                             │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │                        OPERATOR CHAIN                                │  │
│  │                                                                       │  │
│  │                                                                       │  │
│  │   ┌─────────┐   ┌─────────┐   ┌─────────┐   ┌────────────────────┐  │  │
│  │   │ Source  │   │ Filter  │   │ Dedup   │   │ Speed Calculation  │  │  │
│  │   │         │   │         │   │         │   │                    │  │  │
│  │   │ Kafka   │──▶│ conf >= │──▶│ 5s tumbl│──▶│ KeyedProcessFunc   │  │  │
│  │   │ Consumer│   │  0.80   │   │ window  │   │                    │  │  │
│  │   │         │   │         │   │         │   │ Key: plate_text    │  │  │
│  │   │ 16      │   │         │   │         │   │                    │  │  │
│  │   │ readers │   │         │   │         │   │ State:             │  │  │
│  │   │         │   │         │   │         │   │  PlateState        │  │  │
│  │   │         │   │         │   │         │   │  {lastCameraId,    │  │  │
│  │   │         │   │         │   │         │   │   lastEventTime,   │  │  │
│  │   │         │   │         │   │         │   │   lastLat,         │  │  │
│  │   │         │   │         │   │         │   │   lastLon}         │  │  │
│  │   │         │   │         │   │         │   │                    │  │  │
│  │   │         │   │         │   │         │   │ TTL: 2 hours       │  │  │
│  │   └─────────┘   └─────────┘   └─────────┘   └─────────┬──────────┘  │  │
│  │                                                        │             │  │
│  │                        ┌───────────────────────────────┤             │  │
│  │                        │                               │             │  │
│  │                        │  ┌────────────────────────────▼──────────┐  │  │
│  │                        │  │  Camera Registry Broadcast State      │  │  │
│  │                        │  │                                       │  │  │
│  │                        │  │  Source: iot.camera-registry.changes   │  │  │
│  │                        │  │  Refresh: every 5 minutes             │  │  │
│  │                        │  │  Replicated to ALL task slots         │  │  │
│  │                        │  │                                       │  │  │
│  │                        │  │  Map<camera_id, (lat, lon, segment)>  │  │  │
│  │                        │  └───────────────────────────────────────┘  │  │
│  │                        │                                             │  │
│  │                        ▼                                             │  │
│  │   ┌──────────────────────────────────────────────────────────────┐   │  │
│  │   │           Windowed Aggregation + Congestion Detection        │   │  │
│  │   │                                                              │   │  │
│  │   │  Key: road_segment_id                                       │   │  │
│  │   │  Window: Sliding(60 min, 1 min)                             │   │  │
│  │   │                                                              │   │  │
│  │   │  ProcessFunction (per window trigger):                       │   │  │
│  │   │    1. avg_speed = AVG(all speed_mph in window)              │   │  │
│  │   │    2. threshold  = avg_speed * 0.80                         │   │  │
│  │   │    3. slow_cars  = COUNT(speed_mph < threshold)             │   │  │
│  │   │    4. IF slow_cars >= 10 → emit Alert                       │   │  │
│  │   │                                                              │   │  │
│  │   └──────────────────────────┬───────────────────────────────────┘   │  │
│  │                              │                                       │  │
│  │            ┌─────────────────┼─────────────────┐                     │  │
│  │            │                 │                 │                     │  │
│  │            ▼                 ▼                 ▼                     │  │
│  │   ┌──────────────┐  ┌──────────────┐  ┌──────────────┐              │  │
│  │   │ Alert Sink   │  │ Speed Data   │  │ Late Event   │              │  │
│  │   │              │  │ Lake Sink    │  │ Side Output  │              │  │
│  │   │ KafkaProducer│  │              │  │              │              │  │
│  │   │              │  │ S3Sink       │  │ KafkaProducer│              │  │
│  │   │ → iot.traffic│  │ (Parquet)    │  │              │              │  │
│  │   │   .alerts    │  │              │  │ → iot.camera │              │  │
│  │   │              │  │ → s3://      │  │   .events.   │              │  │
│  │   │              │  │   enriched/  │  │   late       │              │  │
│  │   │              │  │   speed-     │  │              │              │  │
│  │   │              │  │   calculations│ │              │              │  │
│  │   │              │  │   /alerts/   │  │              │              │  │
│  │   └──────────────┘  └──────────────┘  └──────────────┘              │  │
│  │                                                                      │  │
│  └──────────────────────────────────────────────────────────────────────┘  │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Kafka Topics

| Topic | Partitions | Retention | Partition Key | Purpose |
|-------|-----------|-----------|---------------|---------|
| `iot.camera-events.raw` | 16 | 7 days | `plate_text` | Raw camera reads from IoT devices |
| `iot.camera-events.late` | 4 | 30 days | `plate_text` | Late-arriving events (>30s watermark) |
| `iot.traffic.alerts` | 4 | 30 days | `road_segment_id` | Congestion alerts for downstream consumers |
| `iot.camera-registry.changes` | 1 | Compact | `camera_id` | Camera location updates (broadcast) |

## State Management

```
┌─────────────────────────────────────────────────────────────────┐
│                     FLINK STATE ARCHITECTURE                    │
│                                                                 │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │  KEYED STATE (per plate_text)                             │  │
│  │  Backend: RocksDB (on-disk, spills from heap)             │  │
│  │                                                           │  │
│  │  ┌─────────────────────────────────────────────────────┐  │  │
│  │  │  PlateState                                         │  │  │
│  │  │  ├── lastCameraId: String                           │  │  │
│  │  │  ├── lastEventTime: Instant                         │  │  │
│  │  │  ├── lastLat: double                                │  │  │
│  │  │  └── lastLon: double                                │  │  │
│  │  │                                                     │  │  │
│  │  │  TTL: 2 hours (auto-cleanup of stale plates)        │  │  │
│  │  │  Estimated size: ~500 MB                            │  │  │
│  │  └─────────────────────────────────────────────────────┘  │  │
│  └───────────────────────────────────────────────────────────┘  │
│                                                                 │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │  BROADCAST STATE (replicated to all tasks)                │  │
│  │                                                           │  │
│  │  ┌─────────────────────────────────────────────────────┐  │  │
│  │  │  Camera Registry                                    │  │  │
│  │  │  Map<camera_id, CameraLocation>                     │  │  │
│  │  │                                                     │  │  │
│  │  │  CameraLocation:                                    │  │  │
│  │  │  ├── latitude: double                               │  │  │
│  │  │  ├── longitude: double                              │  │  │
│  │  │  └── road_segment_id: String                        │  │  │
│  │  │                                                     │  │  │
│  │  │  Refresh: every 5 minutes from Kafka                │  │  │
│  │  │  Estimated size: ~50 MB                             │  │  │
│  │  └─────────────────────────────────────────────────────┘  │  │
│  └───────────────────────────────────────────────────────────┘  │
│                                                                 │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │  WINDOW STATE (per road_segment_id + window)              │  │
│  │                                                           │  │
│  │  ┌─────────────────────────────────────────────────────┐  │  │
│  │  │  WindowState                                        │  │  │
│  │  │  ├── speeds: List<Double>                           │  │  │
│  │  │  ├── count: Long                                    │  │  │
│  │  │  └── sum: Double                                    │  │  │
│  │  │                                                     │  │  │
│  │  │  Scope: 60-min window, cleared after evaluation     │  │  │
│  │  │  Estimated size: ~50 MB                             │  │  │
│  │  └─────────────────────────────────────────────────────┘  │  │
│  └───────────────────────────────────────────────────────────┘  │
│                                                                 │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │  CHECKPOINT STATE (persisted to MinIO)                    │  │
│  │                                                           │  │
│  │  ├── Full snapshot of all keyed + window state            │  │
│  │  ├── Kafka consumer offsets                               │  │
│  │  ├── Incremental diffs (only changes since last checkpoint)│ │
│  │  └── Interval: 60 seconds                                 │  │
│  └───────────────────────────────────────────────────────────┘  │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## Watermark & Late Data Handling

```
  Event Time Axis ──────────────────────────────────────────────▶

  ─────┬─────────┬─────────┬─────────┬─────────┬─────────┬─────
       │         │         │         │         │         │
    Event A    Event B   Event C   Event D   Event E   Event F
    t=10:30    t=10:31   t=10:32   t=10:33   t=10:34   t=10:28
                                                      ↑
                                                   LATE!

  Watermark = max(event_times) - 30 seconds

  At processing time 10:35:
    max_event_time = 10:34
    watermark = 10:34 - 30s = 10:33:30

  Event F (t=10:28) arrives:
    10:28 < 10:33:30 → Event is LATE (>30s beyond watermark)
    → Routed to side output: iot.camera-events.late topic
    → NOT included in window calculations

  Normal case (event within 30s):
    → Processed normally in window aggregation
```

## Error Handling

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          ERROR HANDLING MATRIX                              │
├──────────────────────────────┬──────────────────────────────────────────────┤
│ Scenario                     │ Handling                                     │
├──────────────────────────────┼──────────────────────────────────────────────┤
│ Low confidence (< 0.80)      │ Filtered at Stage 2; metric counter          │
├──────────────────────────────┼──────────────────────────────────────────────┤
│ Duplicate plate+camera read  │ Deduplicated at Stage 3 (5s window)          │
├──────────────────────────────┼──────────────────────────────────────────────┤
│ Camera location not found    │ Routed to dead-letter queue; ops alert      │
├──────────────────────────────┼──────────────────────────────────────────────┤
│ Unpaired plate (1 camera)    │ State TTL expires after 2 hours; no output  │
├──────────────────────────────┼──────────────────────────────────────────────┤
│ Unreasonable speed (>200mph) │ Discarded as noise; metric counter           │
├──────────────────────────────┼──────────────────────────────────────────────┤
│ Late event (>30s)            │ Side output → iot.camera-events.late topic  │
├──────────────────────────────┼──────────────────────────────────────────────┤
│ Alert service unavailable    │ Kafka Connect retry (3x exp. backoff); DLQ  │
├──────────────────────────────┼──────────────────────────────────────────────┤
│ Flink task failure           │ Auto-restart from last checkpoint           │
├──────────────────────────────┼──────────────────────────────────────────────┤
│ Flink job failure            │ JobManager restarts; exactly-once recovery  │
├──────────────────────────────┼──────────────────────────────────────────────┤
│ Kafka broker failure         │ Replication factor handles broker loss      │
├──────────────────────────────┼──────────────────────────────────────────────┤
│ Checkpoint failure           │ Retry; if persistent, job fails + alert     │
└──────────────────────────────┴──────────────────────────────────────────────┘
```

## Capacity Planning

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         CAPACITY ESTIMATES                                  │
├──────────────────────────────┬──────────────────────────────────────────────┤
│ Metric                       │ Estimate                                     │
├──────────────────────────────┼──────────────────────────────────────────────┤
│ Cameras                      │ 1,000                                        │
│ Raw events/sec               │ 10,000 (10 reads/camera/sec avg)            │
│ After filter + dedup         │ ~7,000 events/sec                            │
│ Speed calculations/sec       │ ~3,500 (50% pair successfully)              │
│ Road segments                │ ~100                                         │
│ Window evaluations/min       │ ~100 (one per segment)                       │
│ Alerts/min (typical)         │ 0-5                                          │
│ Alerts/min (rush hour)       │ 20-50                                        │
├──────────────────────────────┼──────────────────────────────────────────────┤
│ Flink task slots             │ 8-16                                         │
│ Flink heap memory            │ 4 GB per task manager                        │
│ RocksDB disk                 │ 2 GB (state + checkpoints)                   │
│ Kafka brokers                │ 3                                            │
│ Kafka partitions (raw)       │ 16                                           │
├──────────────────────────────┼──────────────────────────────────────────────┤
│ State size (plate)           │ ~500 MB                                      │
│ State size (window)          │ ~50 MB                                       │
│ State size (broadcast)       │ ~50 MB                                       │
│ Checkpoint size              │ ~600 MB (full), ~10 MB (incremental)         │
├──────────────────────────────┼──────────────────────────────────────────────┤
│ Kafka throughput             │ ~100 MB/s inbound, ~50 MB/s outbound         │
│ MinIO storage (daily)        │ ~50 GB (raw) + ~10 GB (enriched)             │
└──────────────────────────────┴──────────────────────────────────────────────┘
```

## Deployment Topology

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        DOCKER COMPOSE TOPOLOGY                              │
│                                                                             │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                        Kafka Cluster                                 │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                 │   │
│  │  │  Broker 1   │  │  Broker 2   │  │  Broker 3   │                 │   │
│  │  └─────────────┘  └─────────────┘  └─────────────┘                 │   │
│  │         ▲                                                           │   │
│  │         │         ┌─────────────┐                                   │   │
│  │         │         │  ZooKeeper  │                                   │   │
│  │         │         └─────────────┘                                   │   │
│  └─────────┼───────────────────────────────────────────────────────────┘   │
│            │                                                               │
│  ┌─────────┼───────────────────────────────────────────────────────────┐   │
│  │         │           Flink Cluster                                   │   │
│  │  ┌──────▼──────┐                                                    │   │
│  │  │ JobManager  │                                                    │   │
│  │  │             │─── Submits jobs                                    │   │
│  │  │ - Scheduling│                                                    │   │
│  │  │ - Checkpoint│                                                    │   │
│  │  │ - Recovery  │                                                    │   │
│  │  └──────┬──────┘                                                    │   │
│  │         │                                                           │   │
│  │  ┌──────▼──────┐  ┌──────────────┐  ┌──────────────┐               │   │
│  │  │ TaskManager │  │ TaskManager  │  │ TaskManager  │               │   │
│  │  │  (4 slots)  │  │  (4 slots)   │  │  (4 slots)   │               │   │
│  │  │             │  │              │  │              │               │   │
│  │  │ - Source    │  │ - Filter     │  │ - Windowed   │               │   │
│  │  │ - Dedup     │  │ - Speed Calc │  │   Agg        │               │   │
│  │  │ - Broadcast │  │ - State Mgmt │  │ - Sinks      │               │   │
│  │  └─────────────┘  └──────────────┘  └──────────────┘               │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│            │                                                      
│  ┌─────────┼───────────────────────────────────────────────────────────┐   │
│  │         │           Storage Layer                                  │   │
│  │  ┌──────▼──────┐  ┌──────────────┐  ┌──────────────┐               │   │
│  │  │    MinIO    │  │Kafka Connect │  │   Alert      │               │   │
│  │  │    (S3)     │  │  (HTTP Sink) │  │   Service    │               │   │
│  │  │             │  │              │  │   (External) │               │   │
│  │  │ - Raw data  │  │ - Alerts →   │  │              │               │   │
│  │  │ - Enriched  │  │   REST API   │  │ - POST       │               │   │
│  │  │ - Checkpts  │  │ - Retry logic│  │   /alerts    │               │   │
│  │  └─────────────┘  └──────────────┘  └──────────────┘               │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```
