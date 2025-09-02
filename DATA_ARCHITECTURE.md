
1. [Complete Data Platform Architecture](https://github.com/brendajanuario/credix-data-pipeline/blob/main/DATA_ARCHITECTURE.md#data-architecture-a-lambda-approach)
2. [Case-Specific Solution (only PostgreSQL transactional replication)](https://github.com/brendajanuario/credix-data-pipeline/blob/main/DATA_ARCHITECTURE.md#case-specific-solution-only-postgresql-transactional-replication)

## **Data Architecture: A Lambda Approach**

This project adopts a Lambda architecture to handle both **batch** and **streaming** data. This design meets two core needs in enterprise data environments: historical storage for analytics and real-time processing for event-driven use cases, such as on-demand credit score calculation.

The architecture is composed of distinct, yet interconnected, layers: the **Ingestion Layer**, the **Transformation Layer**, and the **Serving Layer**, each with a specific role in the data's journey.

<img width="2804" height="1788" alt="image" src="https://github.com/user-attachments/assets/448901ac-5fc5-4c5d-b199-734d3a6e19b9" />

---

### **Ingestion Layer**

The data journey begins with diverse sources, such as transactional databases like **PostgreSQL**, but can be easily extended to include **SaaS applications** (like Salesforce), **external APIs**, or **IoT devices**.

**Collection**
* **Cloud Solution:** Use **Datastream** for continuous ingestion. Its native **Change Data Capture (CDC)** provides a highly efficient and reliable way to replicate data in real time, with the flexibility to also handle batch operations. **Dataflow** serves as a cost-effective alternative for scenarios that don't require native CDC.
* **Open Source Solution:** **Airbyte** is a primary option for a low-cost, open-source approach, offering a wide array of pre-built connectors. However, for a robust production setup, it typically requires a **Kubernetes** cluster, which can increase operational complexity.

**Initial Storage**
All collected data is stored in **Google Cloud Storage (GCS)** using the **Parquet** format. Its columnar structure is optimized for read efficiency and compression, which reduces storage costs and accelerates analytical queries.

---

### **Transformation Layer**

**Streaming**
* **Dataflow** handles real-time transformations, ensuring scalability and seamless integration with Pub/Sub for low-latency processing.

**Batch**
* Transformations are performed directly in **BigQuery** using **dbt**, which provides versioning and reproducibility for all queries. **Dagster** orchestrates this entire process.

**Dagster vs. Airflow for Orchestration**
The choice to use Dagster over Airflow was based on several key advantages. While Airflow is a mature, task-based orchestrator, **Dagster** is **asset-oriented**, treating each dataset or model as a traceable and versioned asset. This provides a clear, integrated view of the data lineage, allowing us to **understand dependencies from source to final consumption**. Dagster also offers a more declarative experience with built-in data typing, **native dbt integration**, and comprehensive observability metrics, which significantly reduces the need for additional layers of monitoring.

---

### **Data Quality & Observability**

**Elementary** is used to monitor data quality. It integrates directly with **dbt**, leveraging the tests and metadata already defined within your dbt project. This provides a centralized and automated way to monitor data freshness, detect anomalies, and verify expected values. By integrating with dbt, it acts as a single source of truth for pipeline health, generating alerts and detailed reports in case of failures.

---

### **Serving Layer**

The serving layer is designed around the **needs of the business** and the required **consumption frequency** of the data.

* **Real-Time:** Data is exposed via **Bigtable** for low-latency access by dashboards, internal systems, APIs, and machine learning models.
* **Streaming:** Processed data is made available through **Pub/Sub**, enabling different services to consume data asynchronously from a topic.
* **Batch:** Transformed data in the Bronze, Silver, and Gold layers of **BigQuery** is ready for analysis by BI teams, management reports, and data scientists.

---

### **A Hybrid Approach: GCP + Dagster**

An effective strategy is to leverage GCP's native services for the core data pipeline while using Dagster as a pure **orchestrator**. This allows the benefit of the elasticity, scalability, and ease of use of managed GCP services (**Datastream, Dataflow, BigQuery**) while maintaining the robust traceability, governance, and superior orchestration capabilities of Dagster.

---
### Data Tool Comparison -> Use Cases and Estimated Cost

#### Data Ingestion Tools
These tools move data from sources to a destination. Costs vary widely with data volume. The "Ideal Situation" column highlights the primary use case for each.
The estimates below assume a moderate daily data volume (e.g., 100-200 GB).

<img width="1502" height="668" alt="image" src="https://github.com/user-attachments/assets/6c741cb7-0b08-4bfb-9258-b959ea0e87b9" />

#### Orchestration Tools
These tools manage and schedule data pipelines. The main cost is for the underlying compute resources.
For a light workload like 15 daily jobs, the cost is relatively fixed.

<img width="1496" height="658" alt="image" src="https://github.com/user-attachments/assets/668d8211-00e5-42b5-8e11-dda86042e468" />

---

## Case-Specific Solution (only PostgreSQL transactional replication)

For the PostgreSQL use case, the recommended design follows a simplified Medallion architecture on GCP:

### Hypothetical scenario considered

- **Volume:** Hundreds of MB per month, must be scalable.  
- **Team:** Small team; rely on managed services.  
- **Raw Access SLA:** Users mostly care about transformed data; raw is accessed only for audits or troubleshooting.  

### Chosen Architecture

- **Ingestion:** Datastream in batch CDC mode.  
- **Typical dbt + External Tables Flow:**  
  - **Bronze:** External tables in BigQuery pointing to raw data in **GCS**.  
  - **Silver:** dbt transformations reading from external tables and writing to native BigQuery tables.  
  - **Gold:** Final consumption layer in BigQuery, modeled via dbt.  

### Cost & Storage Optimization

- Partition tables in BigQuery to move data from active to long-term storage after 90 days.  
- Archive raw landing data in GCS after 90 days to reduce storage costs.
