# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "bed869e4-f15b-4cc1-9368-c7a9b3e08a83",
# META       "default_lakehouse_name": "lh_jde_gold",
# META       "default_lakehouse_workspace_id": "9ea13355-c802-4ca5-883f-e5dbf8ecc720",
# META       "known_lakehouses": [
# META         {
# META           "id": "bed869e4-f15b-4cc1-9368-c7a9b3e08a83"
# META         },
# META         {
# META           "id": "915ea8b7-e01a-4182-b41a-c283df48a086"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import functions as F

df = spark.read.table("lh_jde_gold.rpt.dim_hold_orders_code")

df_updated = df.withColumn(
    "holdorderscode_description",
    F.when(F.trim(F.col("holdorderscode_code")) == "", F.lit(""))
     .otherwise(F.col("holdorderscode_description"))
).persist()
df_updated.count()

df_updated.write.mode("overwrite").saveAsTable("lh_jde_gold.rpt.dim_hold_orders_code")
df_updated.unpersist()

# full table
display(spark.read.table("lh_jde_gold.rpt.dim_hold_orders_code"))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }
