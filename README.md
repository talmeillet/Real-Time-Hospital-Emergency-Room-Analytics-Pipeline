# Real-Time Hospital ER Analytics Pipeline 🏥⚡

### **Project Overview**
This project implements a scalable, **end-to-end Big Data pipeline** for real-time monitoring and analysis of Hospital Emergency Room (ER) operations. By simulating continuous patient arrivals, the system demonstrates how distributed infrastructures can handle high-velocity clinical data to provide immediate operational insights.

---

### **System Architecture & Data Flow**
The system is fully containerized using **Docker Compose**, ensuring a seamless orchestration of the following components:

1.  **Ingestion Layer (Producer):** A Python script streams patient arrival data from a CSV source to a **Kafka** topic (`er_arrivals`), simulating a live EMR feed.
2.  **Streaming Broker:** **Apache Kafka** manages the real-time data flow with high throughput.
3.  **Persistence Layer (Consumer):** A dedicated consumer reads from Kafka and persists the raw records into **MongoDB** for long-term storage and historical auditing.
4.  **Analytics Layer (Spark):** **PySpark** connects to MongoDB to perform complex batch and stream processing, calculating KPIs like hourly arrival trends and department-wise patient distribution.
5.  **Caching Layer:** Processed metrics are pushed to **Redis** for sub-millisecond retrieval by the visualization layer.
6.  **Visualization (Dashboard):** A **Streamlit** dashboard fetches real-time KPIs from Redis, providing hospital administrators with interactive charts and live status updates.

---

### **Key Technical Implementations**
* **Scalability:** The architecture is designed to scale horizontally by adding more Kafka partitions or Spark workers.
* **Real-Time KPIs:** Automated calculation of total arrivals, average patient age, and arrival frequency trends.
* **Infrastructure as Code:** The entire environment (Kafka, Zookeeper, Mongo, Redis) is managed via `docker-compose.yml` for easy deployment and testing.

---

### **Tech Stack**
* **Languages:** Python, PySpark 
* **Message Broker:** Apache Kafka
* **Data Processing:** Apache Spark
* **NoSQL Databases:** MongoDB (Document Store), Redis (Key-Value Cache) 
* **Visualization:** Streamlit
* **DevOps:** Docker, Docker Compose

