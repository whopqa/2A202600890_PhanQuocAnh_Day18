# Architecture Decision: LLM Observability at 1B Requests/Day

This document defines the storage and processing architecture for our high-throughput LLM observability platform, designed to capture, process, and analyze 1 Billion requests per day under a hard budget cap of **$5,000/month**.

---

## 1. Problem Statement

We are logging every API request/response for a foundation-model suite. 
- **Scale:** 1 Billion requests/day × 5 KB/request ≈ **5 TB/day raw** (150 TB/month).
- **SLAs & Requirements:**
  1. Real-time dashboards (cost & latency per tenant) refreshed every 5 minutes.
  2. Full prompt/response text retained for 7 days for incident review.
  3. Aggregated metrics retained for 1 year.
  4. PII (API keys, credentials, phone numbers, emails) redacted/tokenized at ingestion before human viewing.
  5. Hard FinOps budget constraint: **Total storage + compute spend ≤ $5,000/month**.

**Why it is hard:** Ingestion write rates average **11,574 writes/sec** (peaking at ~25,000 writes/sec). Processing this volume in real-time, performing regex-based PII masking, updating dashboards every 5 minutes, and storing 150 TB of data monthly would easily cost $20,000+/month on standard cloud architectures if not optimized aggressively.

---

## 2. Architecture Diagram

```mermaid
graph TD
    %% Ingestion Flow
    LLMGate[LLM Gateways] -->|API Logs: JSON| Kafka[Apache Kafka / MSK]
    
    %% Compute & Processing Layers
    subgraph Ingestion & Processing (Spark Structured Streaming)
        Kafka -->|Stream| SparkIngest[Spark Streaming Job]
        SparkIngest -->|1. Tokenize & Mask PII| BronzeStore
    end
    
    %% Storage Tier (Object Storage)
    subgraph Storage Layer (Delta Lake on AWS S3)
        BronzeStore[(Bronze Table: raw_calls_masked)]
        SilverStore[(Silver Table: curated_calls)]
        GoldStore[(Gold Table: daily_tenant_metrics)]
    end
    
    %% ETL & Aggregation Flow
    SparkIngest -->|2. Micro-batch write| BronzeStore
    SparkSilver[Spark Silver Refiner] -->|Dedup & Clean| SilverStore
    SparkGold[Spark Gold Aggregator] -->|Roll up every 5m| GoldStore
    
    %% Lifecycle Archive
    SilverStore -->|Retention: 7 days| S3Glacier[(S3 Glacier Deep Archive)]
    BronzeStore -->|Retention: 7 days| S3Glacier
    
    %% Query & Consumer Layer
    subgraph Query & Analytics Path
        GoldStore -->|Fast Dashboards| DuckDB[DuckDB / Trino]
        SilverStore -->|Incident Review| Trino[Trino / Athena]
        DuckDB --> Dashboards[5-min Tenant Dashboards]
        Trino --> AdminUI[Internal Support Console]
    end

    classDef storage fill:#2A5C91,stroke:#fff,stroke-width:2px,color:#fff;
    classDef compute fill:#D95F02,stroke:#fff,stroke-width:2px,color:#fff;
    classDef source fill:#7570B3,stroke:#fff,stroke-width:2px,color:#fff;
    
    class BronzeStore,SilverStore,GoldStore,S3Glacier storage;
    class SparkIngest,SparkSilver,SparkGold,DuckDB,Trino compute;
    class LLMGate,Kafka source;
```

---

## 3. Key Decisions & Rejected Alternatives

### Decision 1: Table Format
*   **Selected:** **Delta Lake (delta-rs / PySpark)**.
*   **Rejected Alternative A (Apache Iceberg):** While Iceberg has superior catalog independence, Delta Lake offers faster write paths, tighter integration with Spark Structured Streaming via native ACID checkpoints, and superior compaction performance under heavy concurrent writes.
*   **Rejected Alternative B (Apache Hudi):** Hudi’s Merge-on-Read format is ideal for highly mutable CDC pipelines but introduces unnecessary query-read latency overhead for our write-heavy, read-light time-series workload.

### Decision 2: Storage Partitioning & Clustering Layout
*   **Selected:** Partition by `date` (UTC), Z-order (clustered) by `tenant_id` and `model`.
*   **Rejected Alternative A (Partition by `tenant_id`):** Creating partitions for thousands of tenants results in the "small file problem" magnified by 1,000×, causing S3 request costs and query planning times to explode.
*   **Rejected Alternative B (Partition by `hour`):** Hourly partitioning creates too many metadata directories per week. A daily partition combined with Z-ordering on `tenant_id` gives sub-second file-skipping for tenant-specific dashboard queries.

### Decision 3: PII Masking Location
*   **Selected:** In-line tokenization inside the Spark Ingestion stream before writing to the **Bronze** table.
*   **Rejected Alternative A (Post-hoc Silver processing):** Masking PII during the Silver ETL stage means raw PII sits in the Bronze table. Even if restricted, having raw PII on disk creates a major compliance liability (Decree 13 / GDPR) and requires complex column-level ACLs.
*   **Rejected Alternative B (Client-side gateway redaction):** Redacting at the LLM Gateway places CPU load on critical user-facing paths, increasing API latency. Ingress stream tokenization decouples this latency.

### Decision 4: Compute Engine for Dashboards
*   **Selected:** **DuckDB** reading Delta metadata directly for the 5-minute dashboards.
*   **Rejected Alternative A (Athena/Trino):** Querying Athena every 5 minutes for thousands of concurrent dashboard users incurs high serverless query fees. DuckDB can run locally on the dashboard server web-tier, cache read blocks, and process the Gold table (which is pre-aggregated and small) at zero extra infrastructure cost.
*   **Rejected Alternative B (Redshift/Snowflake):** Maintaining an active data warehouse cluster just to update tenant latency/cost graphs would breach our $5,000/month budget within the first week.

### Decision 5: File Retention & Storage Tiering
*   **Selected:** Keep Bronze and Silver in S3 Standard for exactly 7 days, then automatically transition files to **S3 Glacier Deep Archive** using S3 Lifecycle policies, deleting them after 30 days. Gold aggregates remain in S3 Standard for 1 year.
*   **Rejected Alternative A (S3 Intelligent-Tiering only):** S3 Intelligent-Tiering takes 30-90 days to transition inactive objects, costing us thousands in standard storage fees for raw logs that we know will never be read after 7 days.

---

## 4. Failure Modes

### Failure Mode 1: Poison Pill Schema Changes (JSON Drift)
*   **Scenario:** A upstream LLM model change updates the JSON format of `usage` from nested integers `{"input": 100, "output": 50}` to a list or string, causing Spark's JSON parser to throw exceptions or output `NULL` values.
*   **Detection:** Spark streaming job monitors a `corrupted_records` count. If it exceeds 0.1% of throughput, an alert fires.
*   **Rollback/Mitigation:** Write unparseable records to a `dead_letter_queue` Delta table without crashing the stream. We then use Delta Lake's **schema evolution** (`mergeSchema`) or rerun the parser over the dead-letter table using Time Travel once the mapping code is updated.

### Failure Mode 2: S3 Rate Limiting (HTTP 503 Slow Down)
*   **Scenario:** High-throughput streaming writes attempt to upload parquet files concurrently, hitting S3's limit of 3,500 PUT requests/sec per prefix.
*   **Detection:** Spark streaming tasks fail with `AmazonS3Exception: Slow Down`.
*   **Mitigation:** Configure Spark writes to use a partition structure containing a hash prefix (e.g., `_lakehouse/bronze/hash=ab/date=2026-04-01/`). This spreads writes across multiple S3 partitions, bypassing prefix limits.

### Failure Mode 3: PII Redaction Regex failure (Token leakage)
*   **Scenario:** An edge case in user prompts bypasses our regex patterns, leaking phone numbers or credentials into the Silver table.
*   **Detection:** Regular automated auditing runs against random samples in Silver using advanced NLP models (e.g., Presidio).
*   **Mitigation:** Utilize Delta Lake **Time Travel** and **ACID transactions** to overwrite the corrupted rows. Since Delta supports `DELETE WHERE`, we can remove the leaked PII in a single transaction, ensuring any query running after the commit sees the cleaned version instantly.

---

## 5. Cost Back-of-Envelope (Math & Estimates)

Our hard limit is **$5,000/month**.

### A. Storage Cost Calculations
- **Bronze Table (7 days retention):** 5 TB/day × 7 days = 35 TB.
  - S3 Standard: 35 TB × $23/TB-month = **$805/mo**.
- **Silver Table (7 days retention, compressed):** 5 TB/day raw reduces to ~2.5 TB/day when converted to highly compressed columnar Parquet (ZSTD).
  - 2.5 TB/day × 7 days = 17.5 TB.
  - S3 Standard: 17.5 TB × $23/TB-month = **$402.50/mo**.
- **Gold Table (1 year retention, pre-aggregated daily):** ~50 MB/day (daily tenant/model metrics).
  - 50 MB × 365 days = 18.25 GB.
  - S3 Standard: 18.25 GB × $0.023/GB = **$0.42/mo** (negligible).
- **Archive Storage (30 days of compressed Bronze in Glacier Deep Archive):**
  - 2.5 TB/day × 30 days = 75 TB.
  - S3 Glacier Deep Archive: 75 TB × $0.99/TB-month = **$74.25/mo**.
- **Total Storage Cost = ~$1,282.17/month**

### B. Compute Cost Calculations
We deploy Spark Structured Streaming on Spot Instances in AWS EMR (or EKS).
- **Ingestion Stream (24/7):** 2 × `m6g.xlarge` Spot instances ($0.068/hr each) + EMR fee ($0.03/hr).
  - Cost per hour = 2 × $0.068 + $0.03 = $0.166/hr.
  - Cost per month = $0.166 × 24 × 30 = **$119.52/mo**.
- **Silver Refiner & Compaction (Runs hourly for 10 min):** 4 × `c6g.2xlarge` Spot instances ($0.136/hr each).
  - Cost per month = (4 × $0.136 + $0.06) × 4 hours/day × 30 days = **$72.48/mo**.
- **Gold Rollups (Every 5 minutes, runs for 15s):** 1 × `m6g.large` Spot instance.
  - Cost per month = $0.034/hr × 24 × 30 = **$24.48/mo**.
- **S3 API requests (GET/PUT charges):**
  - ~1.2M PUT requests/month = **$6.00/mo**.
- **Kafka / MSK (3 nodes, 2xlarge):**
  - Cost per month = 3 × $300 = **$900.00/mo**.
- **Total Compute & Networking Cost = ~$1,122.48/month**

**Grand Total Estimated Spend = $2,404.65/month** (leaving a 50% buffer under our $5,000 ceiling for peak traffic spikes).

---

## 6. What You Would Build First (1-Week MVP)

The MVP aims to prove we can ingest, redact PII, and generate 5-minute dashboards at zero-copy speed using DuckDB.

### Scope of MVP:
1. **Mock Data Generator:** A python script simulating LLM logging requests containing mock PII (emails/phone numbers).
2. **Ingestion Pipeline:** A lightweight script writing raw records directly to a partitioned Delta table (`bronze/llm_calls_raw`).
3. **Tokenization Job:** A python function performing regex masking and saving the clean dataset to `silver/llm_calls`.
4. **Dashboard Query:** A DuckDB sql script that scans the Silver table directly using `delta_scan` and rolls up latency/cost metrics into `gold/llm_daily_metrics` to update the dashboard.
5. **Validation:** Assert that no raw PII exists in Silver and that DuckDB executes the aggregation in < 200 ms.
