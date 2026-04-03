# ER-Pulse: Real-Time Hospital Emergency Room Analytics Pipeline

**ER-Pulse** is an end-to-end Big Data system designed to simulate, store, and analyze patient flow in a hospital emergency room (ER) in real-time. The system handles high-velocity data streaming, ensures fault-tolerant storage, and provides immediate operational insights through a live dashboard.

## 🏗 System Architecture
The system is built using a microservices architecture, fully containerized with **Docker** for portability and consistent deployment.

### Key Components:
* **Data Producer (Python):** Simulates real-time patient arrivals by streaming records from a historical dataset every **10 seconds**.
* **Message Broker (Apache Kafka):** Acts as the "ingestion pipeline," decoupling the data source from the processing units and ensuring no data loss even during database downtime.
* **Storage Layer:**
    * **MongoDB (NoSQL):** Serves as the primary "Document Store" for long-term historical records and semi-structured clinical data.
    * **Redis (In-Memory):** Used for ultra-fast, real-time caching of KPIs to power the live dashboard.
* **Analytical Engine (Apache Spark):** The "brain" of the system, performing distributed processing to calculate complex metrics such as average wait times and department loads.
* **Visualization (Streamlit):** A dynamic dashboard that provides hospital administrators with real-time visual insights.

## 🧠 The DIKW Journey
Our pipeline follows the **Data-Information-Knowledge-Wisdom** hierarchy to transform raw facts into actionable insights:
1.  **Data:** Raw CSV records of patient arrivals.
2.  **Information:** Organized data within **MongoDB** and **Redis** providing context.
3.  **Knowledge:** Patterns and correlations identified by **Spark** analytics.
4.  **Wisdom:** Real-time decision-making support via the **Dashboard** (e.g., reallocating staff to high-load departments).

## 🛠 Tech Stack
| Category | Technology |
| :--- | :--- |
| **Streaming** | Apache Kafka |
| **Databases** | MongoDB (NoSQL), Redis (In-Memory) |
| **Analytics** | Apache Spark (PySpark) |
| **Orchestration** | Docker & Docker Compose |
| **Development** | Python 3.9 |
| **UI** | Streamlit & Plotly |


## 📊 Analytics & KPIs
The system calculates critical metrics in real-time:
* **Total Arrivals:** Continuous count of incoming patients.
* **Average Wait Time:** Calculated dynamically to monitor ER efficiency.
* **Department Load:** Distribution of patients across Orthopedics, Cardiology, Neurology, and Nephrology.
