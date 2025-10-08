# 🚀 Adventure Works End-to-End Data Engineering Project

![Azure](https://img.shields.io/badge/Azure-Cloud-blue?logo=microsoft-azure&style=flat-square)
![PySpark](https://img.shields.io/badge/PySpark-Big%20Data-orange?logo=apache-spark&style=flat-square)
![Azure Data Factory](https://img.shields.io/badge/Azure-Data%20Factory-blue?logo=microsoft-azure&style=flat-square)
![Azure Synapse](https://img.shields.io/badge/Azure-Synapse%20Analytics-blue?logo=microsoft-azure&style=flat-square)
![Databricks](https://img.shields.io/badge/Databricks-Delta%20Lake-red?logo=databricks&style=flat-square)
![SQL](https://img.shields.io/badge/SQL-Data%20Warehouse-yellow?logo=database&style=flat-square)
![PowerBI](https://img.shields.io/badge/Power%20BI-Dashboard-orange?logo=power-bi&style=flat-square)
![Git](https://img.shields.io/badge/Git-Version%20Control-green?logo=git&style=flat-square)

---

## 📌 Project Overview
**Adventure Works End-to-End Data Engineering Project** demonstrates a full-scale **Azure cloud data pipeline**.  
Data is ingested via **Azure Data Factory**, processed in **Databricks (PySpark + Delta Lake)**, stored in **ADLS**, and loaded into **Synapse SQL Pool** for analytics with **Power BI**.  
The project implements **Medallion Architecture** and **Star Schema design** for enterprise-ready analytics.

---

## Pipeline

<img width="1472" height="704" alt="Gemini_Generated_Image_4czv2w4czv2w4czv" src="https://github.com/user-attachments/assets/3476e337-1338-4af9-887a-9d2e29652b78" />


## 🎯 Objectives
- Implement ETL pipelines from raw data to analytics-ready tables.  
- Process Adventure Works data through **Bronze → Silver → Gold** layers.  
- Ensure **schema evolution** and **data quality** using Delta Lake.  
- Enable **interactive dashboards** via Power BI.  
- Maintain code with **Git version control**.

---

## 📂 Project Structure
```plaintext
Adventure-Works-Analytics/
│
├── Data/                      # Raw and curated data
│   ├── Bronze/
│   ├── Silver/
│   └── Gold/
│
├── Reference Script/           # ADF pipelines, Databricks notebooks, SQL queries
│   ├── ADF_Pipelines.json
│   ├── Databricks_Notebooks.py
│   └── Synapse_Queries.sql
│
└── README.md                   # Project documentation
