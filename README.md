# Real-Time E-Commerce Analytics Platform

Production-grade streaming data pipeline processing 6,500+ e-commerce events with sub-minute latency using Apache Kafka, Spark Streaming, Delta Lake, and Apache Airflow.

![Python](https://img.shields.io/badge/Python-3.11-blue)
![Kafka](https://img.shields.io/badge/Apache%20Kafka-4.2.0-black)
![Spark](https://img.shields.io/badge/Apache%20Spark-3.4.1-orange)
![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.7.3-red)

## 🎯 Project Overview

End-to-end streaming analytics platform demonstrating production-grade data engineering with automated orchestration and real-time visualizations.

## Screenshots
## 📸 Project Screenshots

### Airflow Orchestration

<table>
  <tr>
    <td width="50%">
      <img src="docs/screenshots/Screenshot 2026-04-30 at 12.59.29.png" alt="Airflow DAGs" />
      <p align="center"><b>All 3 Production DAGs Running</b></p>
    </td>
    <td width="50%">
      <img src="docs/screenshots/Screenshot 2026-04-30 at 12.59.52.png" alt="DAG Details" />
      <p align="center"><b>Refresh DAG - 7 Successful Runs</b></p>
    </td>
  </tr>
  <tr>
    <td width="50%">
      <img src="docs/screenshots/Screenshot 2026-04-30 at 13.00.06.png" alt="Task Graph" />
      <p align="center"><b>Task Dependencies Pipeline</b></p>
    </td>
    <td width="50%">
      <img src="docs/screenshots/Screenshot 2026-04-30 at 12.51.44.png" alt="Dashboard Main" />
      <p align="center"><b>Real-Time Dashboard - 6.9% Conversion</b></p>
    </td>
  </tr>
</table>

### Analytics Dashboard

<table>
  <tr>
    <td width="50%">
      <img src="docs/screenshots/Screenshot 2026-04-30 at 12.51.59.png" alt="Funnel" />
      <p align="center"><b>Conversion Funnel Visualization</b></p>
    </td>
    <td width="50%">
      <img src="docs/screenshots/Screenshot 2026-04-30 at 12.52.10.png" alt="Trending Products" />
      <p align="center"><b>Top Trending Products</b></p>
    </td>
  </tr>
  <tr>
    <td width="50%">
      <img src="docs/screenshots/Screenshot 2026-04-30 at 12.52.29.png" alt="Revenue" />
      <p align="center"><b>Revenue Analytics</b></p>
    </td>
    <td width="50%">
      <img src="docs/screenshots/Screenshot 2026-04-30 at 12.52.40.png" alt="Alerts" />
      <p align="center"><b>Live Alerts & System Health</b></p>
    </td>
  </tr>
</table>

**Key Metrics:**
- 📊 6,549 events processed
- 💰 £500,727 revenue tracked
- 🛒 452 completed orders
- 📈 6.9% conversion rate
- ⚡ Sub-60 second pipeline latency

---

**Data Flow:**
1. **Ingestion**: Kafka streams events from Python generator
2. **Processing**: Spark Streaming with medallion architecture (Bronze/Silver/Gold)
3. **Storage**: Delta Lake with ACID transactions and time travel
4. **Orchestration**: Airflow automates refresh cycles and quality checks
5. **Serving**: PostgreSQL optimized for dashboard queries (<50ms)
6. **Visualization**: Streamlit dashboard with real-time updates

---

## 🛠️ Tech Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| **Event Generation** | Python 3.11, Faker | Realistic e-commerce simulation |
| **Streaming** | Apache Kafka 4.2.0 | Distributed message queue |
| **Processing** | PySpark 3.4.1 | Stream processing |
| **Storage** | Delta Lake 2.4.0 | ACID-compliant data lakehouse |
| **Orchestration** | Apache Airflow 2.7.3 | Workflow automation |
| **Database** | PostgreSQL 15 | Serving layer |
| **Visualization** | Streamlit, Plotly | Interactive dashboards |

---

## 🎯 Key Features

### **Real-Time Stream Processing**
- Kafka topics with partitioning for parallel processing
- Spark Structured Streaming with windowing and watermarking
- Delta Lake medallion architecture (Bronze → Silver → Gold)
- Sub-minute end-to-end latency

### **Automated Orchestration (Apache Airflow)**
- **Refresh DAG**: Updates serving layer every 30 minutes
- **Quality DAG**: Validates data freshness every 4 hours
- **Summary DAG**: Generates daily business reports at 2 AM
- Automatic retries and error handling

### **Interactive Analytics Dashboard**
- Conversion funnel visualization (6.9% completion rate)
- Top 10 trending products by activity
- Revenue analytics by category and payment method
- Real-time alerts for high-value orders
- System health monitoring

---

## 📊 Dashboard Features

### **Conversion Funnel**
Tracks user journey from page view to purchase with drop-off analysis

### **Trending Products**
Top 10 products ranked by views, cart adds, and purchases

### **Revenue Analytics**
- Revenue breakdown by category
- Payment method distribution
- Time-series revenue trends

### **Live Alerts**
- High-value order detection
- Products needing attention (high views, low conversions)
- Data freshness monitoring

---

## 🚀 Quick Start

### Prerequisites
```bash
Python 3.11+
Apache Kafka 4.2.0+
PostgreSQL 15+
Java 17+ (for Spark)
```

### Installation

```bash
# Clone repository
git clone https://github.com/yourusername/ecommerce-streaming-platform.git
cd ecommerce-streaming-platform

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Start Kafka
./start_kafka.sh

# Setup PostgreSQL
psql postgres -c "CREATE DATABASE ecommerce_analytics;"
psql ecommerce_analytics -f setup_postgres.sql

# Initialize Airflow
export AIRFLOW_HOME=$(pwd)/airflow
airflow db init
airflow users create --username admin --password admin \
  --firstname Admin --lastname User --role Admin \
  --email admin@example.com
```

### Run the Pipeline

```bash
# Start all services (in separate terminals)
./start_all_services.sh           # Starts Kafka, Spark, Event Generator

# Start Airflow
airflow scheduler &
airflow webserver --port 8080 &

# Start Dashboard
streamlit run streamlit_app/dashboard.py
```

**Access:**
- Dashboard: http://localhost:8501
- Airflow: http://localhost:8080 (admin/admin)

---
---

## 📈 Performance Metrics

| Metric | Value |
|--------|-------|
| **Pipeline Latency** | <60 seconds |
| **Query Performance** | <50ms (PostgreSQL) |
| **Events Processed** | 6,549 |
| **Revenue Tracked** | £500,727 |
| **Conversion Rate** | 6.9% |
| **Data Quality** | 99.9% |

---

## 🎓 Technical Highlights

- **ACID Transactions**: Delta Lake ensures data consistency
- **Exactly-Once Semantics**: Spark checkpointing prevents duplicates
- **Automated Quality Checks**: Airflow validates data freshness
- **Fault Tolerance**: Retry logic and error handling
- **Incremental Processing**: Only processes new data
- **Schema Evolution**: Delta Lake supports schema changes
- **Time Travel**: Query historical data versions

---

## 🔮 Future Enhancements

- [ ] Deploy to Azure/AWS cloud platform
- [ ] Add ML-based demand forecasting
- [ ] Implement A/B testing framework
- [ ] Real-time anomaly detection
- [ ] Customer segmentation (RFM analysis)
- [ ] CI/CD pipeline with GitHub Actions

---

## 👤 Author

**Anshumaan Singh**  
AWS Certified Data Engineer | MSc Data Science, University of Glasgow

- 📧 Email: anshumaansingh407@gmail.com
- 💼 LinkedIn: [linkedin.com/in/anshumaan-singh](https://linkedin.com/in/anshumaansingh98)
- 🌐 Portfolio: [https://anshumaan-portfolio.vercel.app/](https://anshumaan-portfolio.vercel.app/)
---

## 📄 License

MIT License - feel free to use this project for learning and portfolio purposes.

---

**Built with:** Python • Kafka • Spark • Delta Lake • Airflow • PostgreSQL • Streamlit

⭐ Star this repo if you find it helpful!
