# 🚍 TransJakarta Data Pipeline Project

## 📘 Overview
This project demonstrates an **end-to-end data pipeline** for analyzing **TransJakarta public transportation** data.  
The pipeline automates the **Extract–Transform–Load (ETL)** process to generate insights into passenger demographics, travel behavior, and route performance.

---
## 📚 Data Source

Dataset: [Transjakarta - Public Transportation Transaction](https://www.kaggle.com/datasets/dikisahkan/transjakarta-transportation-transaction)  
License: [CC0: Public Domain](https://creativecommons.org/publicdomain/zero/1.0/)  
The dataset is freely available for public use. Attribution is appreciated but not required.

---
## ⚙️ Tools & Technologies
| Category | Tools |
|-----------|--------|
| Orchestration | Apache Airflow |
| Transformation | DBT (Data Build Tool) |
| Data Warehouse | PostgreSQL |
| Processing | Apache Spark |
| Visualization | Power BI |
| Containerization | Docker |

---

## 🧩 Data Pipeline Flow
### 1. Data Extraction
- Source data obtained from TransJakarta open datasets.
- Raw files include transaction, route, and passenger information.

### 2. Data Transformation (DBT)
- Cleans and models raw data into **dimensional tables**:
  - `dim_time`, `dim_corridor`, `dim_stop`, `dim_card_holder`, and `fact_transactions`.
- Uses **incremental load** to improve performance on large data.

### 3. Data Orchestration (Airflow)
- DAG automates `dbt run` and `dbt test` commands.
- Ensures data validation and reliable refresh cycles.

### 4. Data Visualization (Power BI)
- Presents metrics on passenger age groups, gender, corridor traffic, and peak travel times.

---

## 📊 Key Insights
- The majority of passengers are aged **36–45**, followed by **26–35**.
- **53.35%** of passengers are **female**, and **46.65%** are **male**.
- The highest number of trips occur during **weekdays**.
- **Peak hours** are at **6 AM** and **5 PM**.
- The busiest corridors are **Matraman Baru–Ancol** and **Blok M–Kota**.

---

## 💡 Business Recommendations
- **Optimize bus frequency** during morning (6 AM) and evening (5 PM) rush hours.  
- **Enhance service quality** in high-demand corridors such as Matraman Baru–Ancol and Blok M–Kota.  
- **Target customer engagement** for age groups between 26–45 years old.

---

## 🚀 Future Improvements
- Integrate **real-time streaming** using Kafka or Spark Structured Streaming.  
- Migrate the warehouse to **cloud platforms (GCP/AWS)** for scalability.  
- Implement **predictive models** for passenger demand forecasting.

---

## 🧠 Key Learnings
- Hands-on experience with **Airflow orchestration and dbt transformations**.  
- Improved understanding of **incremental model optimization** in PostgreSQL.  
- Demonstrated **data engineering workflow automation** from raw data to dashboard.

---

## 👤 Author
**Winardi**  
Data Engineer | Data Enthusiast  
📧 awinardi1004@gmail.com  
🔗 [LinkedIn Profile](https://www.linkedin.com/in/winardi-/)

---

## 🗂️ Project Structure
```bash
├── dags/
│   ├── dag_csv_to_postgres_dag.py
│   └── dag_dbt_pipeline.py
├── include/
│   ├── dbt/
│   │    └── dbt_project/
│   │        ├── models/
│   │        │   └── datamart/
│   │        └── dbt_project.yml
│   └── scripts/
│       └── load_csv_to_postgres.py
├── docker-compose.yml
├── dashboard.pbix
└── README.md
