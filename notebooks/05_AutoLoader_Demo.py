# Databricks notebook source
df = (
    spark.readStream
         .format("cloudFiles")
         .option("cloudFiles.format", "csv")
         .option("header", "true")
         .load("/Volumes/raw_claims")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW CATALOGS;

# COMMAND ----------

from delta.tables import DeltaTable

history = DeltaTable.forName(
    spark,
    "claim_history"
)

print("Success")

# COMMAND ----------

silver_df = spark.table("silver_claims")

display(silver_df)


# COMMAND ----------

from pyspark.sql import functions as F

updates_df = (
    silver_df
    .filter(F.col("claim_id").isin("CLM1","CLM2","CLM3"))
    .withColumn(
        "claim_status",
        F.lit("Approved")
    )
)

display(updates_df)

# COMMAND ----------

from delta.tables import DeltaTable

history = DeltaTable.forName(
    spark,
    "claim_history"
)

# COMMAND ----------

history.alias("t").merge(
    updates_df.alias("s"),
    """
    t.claim_id = s.claim_id
    AND t.is_current = 'Y'
    """
).whenMatchedUpdate(
    condition="t.claim_status <> s.claim_status",
    set={
        "end_date": "current_date()",
        "is_current": "'N'"
    }
).execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM claim_history
# MAGIC WHERE claim_id IN ('CLM1','CLM2','CLM3');
# MAGIC

# COMMAND ----------

new_records = (
    updates_df
    .select(
        "claim_id",
        "claim_status"
    )
    .withColumn("effective_date", F.current_date())
    .withColumn("end_date", F.lit(None).cast("date"))
    .withColumn("is_current", F.lit("Y"))
)

new_records.write \
    .mode("append") \
    .saveAsTable("claim_history")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM claim_history
# MAGIC WHERE claim_id = 'CLM1';

# COMMAND ----------

spark.sql("OPTIMIZE silver_claims")

# COMMAND ----------

spark.sql("""
OPTIMIZE silver_claims
ZORDER BY (claim_id)
""")

# COMMAND ----------

spark.sql("""
DESCRIBE HISTORY silver_claims
""").show(truncate=False)

# COMMAND ----------

spark.sql("""
SELECT *
FROM silver_claims VERSION AS OF 0
""").show(5)

# COMMAND ----------

df = (
    spark.readStream
         .format("cloudFiles")
         .option("cloudFiles.format", "csv")
         .option("header", "true")
         .load("/tmp/raw_claims")
)

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F

# COMMAND ----------

from delta.tables import DeltaTable

print("Delta imported successfully")

# COMMAND ----------

