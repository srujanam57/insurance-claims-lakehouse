Insurance Claims Lakehouse
Project Overview
Built an Insurance Claims Lakehouse using Databricks, PySpark, and Delta Lake following the Medallion Architecture.

Architecture
Claims Data
     |
     v
Bronze Layer
     |
     v
Silver Layer
     |
     +--------------+
     |              |
     v              v
Claim History   Gold Marts
(SCD Type 2)
     |
     v
Databricks Workflow
     |
     v
OPTIMIZE + ZORDER
Technologies Used
Databricks
PySpark
Delta Lake
SQL
GitHub
Features
Bronze Layer (Raw Data)
Silver Layer (Cleaned Data)
Gold Layer (Business Reports)
SCD Type 2
Fraud Claims Mart
Databricks Workflows
Delta Time Travel
OPTIMIZE
Z-ORDER
Tables Created
bronze_claims
silver_claims
claim_history
gold_claim_status
gold_claim_type
gold_fraud_claims
Workflow
Bronze_Load → Silver_Transform → SCD2_History → Gold_Marts

Business Use Case
The project processes insurance claim data, maintains claim history using SCD Type 2, and creates analytical marts for reporting and fraud analysis.

Author
Srujana Mallikarjuna