# Databricks notebook source
print("Spark version:", spark.version)

# COMMAND ----------

data = [
    (101, "POL101", "Collision", "Open", 25000),
    (102, "POL102", "Theft", "Approved", 45000),
    (103, "POL103", "Damage", "Closed", 15000),
    (104, "POL104", "Fire", "Rejected", 35000),
    (105, "POL105", "Collision", "Approved", 55000)
]

columns = [
    "claim_id",
    "policy_id",
    "claim_type",
    "claim_status",
    "claim_amount"
]

claims_df = spark.createDataFrame(data, columns)

display(claims_df)

# COMMAND ----------

# Check total number of records
print("Total Records:", claims_df.count())

# Check total number of columns
print("Total Columns:", len(claims_df.columns))

# Check column names and data types
claims_df.printSchema()

# COMMAND ----------

from pyspark.sql import functions as F

# Number of claims to generate
total_records = 10000

# Generate claims
claims = (
    spark.range(1, total_records + 1)

    .withColumnRenamed("id", "claim_number")

    .withColumn(
        "claim_id",
        F.concat(F.lit("CLM"), F.col("claim_number"))
    )

    .withColumn(
        "policy_id",
        F.concat(F.lit("POL"), F.col("claim_number"))
    )

    .withColumn(
        "customer_id",
        F.concat(F.lit("CUS"), F.col("claim_number"))
    )

    .withColumn(
        "claim_type",
        F.when(F.col("claim_number") % 4 == 0, "Collision")
         .when(F.col("claim_number") % 4 == 1, "Theft")
         .when(F.col("claim_number") % 4 == 2, "Damage")
         .otherwise("Fire")
    )

    .withColumn(
        "claim_status",
        F.when(F.col("claim_number") % 4 == 0, "Open")
         .when(F.col("claim_number") % 4 == 1, "Approved")
         .when(F.col("claim_number") % 4 == 2, "Rejected")
         .otherwise("Closed")
    )

    .withColumn(
        "claim_amount",
        (F.rand(seed=42) * 100000 + 1000).cast("decimal(18,2)")
    )

    .withColumn(
        "claim_date",
        F.date_sub(
            F.current_date(),
            (F.col("claim_number") % 365).cast("int")
        )
    )
)

display(claims.limit(10))

# COMMAND ----------

# Count total claims
total_claims = claims.count()

print("Total Claims Generated:", total_claims)

# Validate record count
if total_claims == 10000:
    print("Validation Successful!")
else:
    print("Validation Failed!")
    

# COMMAND ----------

# Display claims column names and data types

claims.printSchema()

# COMMAND ----------

# Display first 20 claims

display(claims.limit(20))


# COMMAND ----------

claims.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze_claims")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM bronze_claims;

# COMMAND ----------

from pyspark.sql import functions as F

bronze_df = spark.table("bronze_claims")

silver_df = (
    bronze_df
    .filter(F.col("claim_amount") > 0)
    .dropDuplicates(["claim_id"])
    .withColumn("load_date", F.current_date())
)

# COMMAND ----------

silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_claims")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM silver_claims;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM silver_claims;

# COMMAND ----------

gold_status = (
    silver_df
    .groupBy("claim_status")
    .count()
)

# COMMAND ----------

gold_status = (
    silver_df
    .groupBy("claim_status")
    .count()
)

# COMMAND ----------

display(gold_status)

# COMMAND ----------

gold_status.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_claim_status")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold_claim_status;

# COMMAND ----------

gold_type = (
    silver_df
    .groupBy("claim_type")
    .count()
)

display(gold_type)

# COMMAND ----------

gold_type.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_claim_type")

# COMMAND ----------

spark.sql("SHOW TABLES").show(truncate=False)

# COMMAND ----------

from pyspark.sql import functions as F

history_df = (
    silver_df
    .select(
        "claim_id",
        "claim_status"
    )
    .withColumn("effective_date", F.current_date())
    .withColumn("end_date", F.lit(None).cast("date"))
    .withColumn("is_current", F.lit("Y"))
)

history_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("claim_history")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM claim_history LIMIT 10;

# COMMAND ----------

updated_claims = (
    silver_df
    .withColumn(
        "claim_status",
        F.when(
            F.col("claim_id").isin("CLM1","CLM2","CLM3"),
            "Approved"
        ).otherwise(F.col("claim_status"))
    )
)

display(updated_claims.filter(F.col("claim_id").isin("CLM1","CLM2","CLM3")))

# COMMAND ----------

from delta.tables import DeltaTable

delta_table = DeltaTable.forName(
    spark,
    "claim_history"
)

delta_table.update(
    condition="""
        claim_id IN ('CLM1','CLM2','CLM3')
        AND is_current='Y'
    """,
    set={
        "end_date": "current_date()",
        "is_current": "'N'"
    }
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM claim_history
# MAGIC WHERE claim_id IN ('CLM1','CLM2','CLM3');

# COMMAND ----------

new_versions = (
    updated_claims
    .filter(F.col("claim_id").isin("CLM1","CLM2","CLM3"))
    .select("claim_id","claim_status")
    .withColumn("effective_date", F.current_date())
    .withColumn("end_date", F.lit(None).cast("date"))
    .withColumn("is_current", F.lit("Y"))
)

new_versions.write \
    .mode("append") \
    .saveAsTable("claim_history")

# COMMAND ----------

new_versions = (
    updated_claims
    .filter(F.col("claim_id").isin("CLM1","CLM2","CLM3"))
    .select("claim_id","claim_status")
    .withColumn("effective_date", F.current_date())
    .withColumn("end_date", F.lit(None).cast("date"))
    .withColumn("is_current", F.lit("Y"))
)

new_versions.write \
    .mode("append") \
    .saveAsTable("claim_history")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM claim_history
# MAGIC WHERE claim_id='CLM1';

# COMMAND ----------

spark.sql("""
OPTIMIZE claim_history
""")

# COMMAND ----------

spark.sql("""
OPTIMIZE claim_history
ZORDER BY (claim_id)
""")

# COMMAND ----------

spark.sql("DESCRIBE DETAIL claim_history").show(truncate=False)

# COMMAND ----------

df = (
    spark.readStream
         .format("cloudFiles")
         .option("cloudFiles.format", "csv")
         .option("header", "true")
         .load("/Volumes/claims/raw")
)

# COMMAND ----------

spark.sql("SHOW TABLES").show(truncate=False)

# COMMAND ----------

silver_df = spark.table("silver_claims")

# COMMAND ----------

display(silver_df)

# COMMAND ----------

from pyspark.sql import functions as F

claims_v2 = (
    silver_df
    .withColumn(
        "state",
        F.when(F.col("claim_number") % 4 == 0, "KA")
         .when(F.col("claim_number") % 4 == 1, "MH")
         .when(F.col("claim_number") % 4 == 2, "TN")
         .otherwise("AP")
    )
    .withColumn(
        "hospital_name",
        F.when(F.col("claim_number") % 4 == 0, "Apollo")
         .when(F.col("claim_number") % 4 == 1, "Manipal")
         .when(F.col("claim_number") % 4 == 2, "Fortis")
         .otherwise("Aster")
    )
)


# COMMAND ----------

gold_state_summary = (
    claims_v2
    .groupBy("state")
    .agg(
        F.count("*").alias("total_claims"),
        F.sum("claim_amount").alias("total_amount")
    )
)

display(gold_state_summary)


# COMMAND ----------

gold_state_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_state_summary")

# COMMAND ----------

fraud_claims = (
    claims_v2
    .filter(F.col("claim_amount") > 90000)
)

display(fraud_claims)

# COMMAND ----------

fraud_claims.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_fraud_claims")

# COMMAND ----------

spark.sql("SHOW TABLES").show(truncate=False)

# COMMAND ----------

spark.sql("""
OPTIMIZE silver_claims
""")

# COMMAND ----------

spark.sql("""
OPTIMIZE silver_claims
ZORDER BY (claim_id)
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY claim_history;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM claim_history VERSION AS OF 0;

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

history = DeltaTable.forName(
    spark,
    "claim_history"
)


# COMMAND ----------

spark.sql("""
OPTIMIZE silver_claims
""")

# COMMAND ----------

spark.sql("""
OPTIMIZE silver_claims
ZORDER BY (claim_id)
""")
