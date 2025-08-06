# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Business Transformations - Gold Layer
# MAGIC
# MAGIC ## Obiettivi
# MAGIC - Creare aggregazioni business da dati Silver
# MAGIC - Calcolare KPI principali per management
# MAGIC - Preparare tabelle per dashboard
# MAGIC - Implementare logica business specifica
# MAGIC
# MAGIC ## Output attesi
# MAGIC - Tabelle Gold per analytics
# MAGIC - KPI dashboard-ready
# MAGIC - Metriche business validate

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

print("GOLD LAYER: Business Transformations")

df_silver = spark.table("silver_sales_clean")
print(f"Caricati {df_silver.count()} record dal Silver Layer")

print(f"Colonne: {df_silver.columns}")

# COMMAND ----------

print("CALCOLO KPI BUSINESS")

total_revenue = df_silver.agg(sum("total_amount")).collect()[0][0]
total_orders = df_silver.count()
avg_order_value = df_silver.agg(avg("total_amount")).collect()[0][0]
unique_customers = df_silver.select("customer_id").distinct().count()

print(f"Fatturato totale: {total_revenue}")
print(f"Ordini totali: {total_orders}")
print(f"Fatturato medio: {avg_order_value}")
print(f"Numero di clienti unici: {unique_customers}")

# Calcoliamo i KPI per crearci poi la nostra tabella Gold
kpi_data = spark.createDataFrame([
    {"metric": "total_revenue", "value": float(total_revenue)},
    {"metric": "total_orders", "value": float(total_orders)},
    {"metric": "avg_order_value", "value": float(avg_order_value)},
    {"metric": "unique_customers", "value": float(unique_customers)}
])

# COMMAND ----------

print("AGGREGAZIONI")


df_categoria = df_silver.groupBy("category").agg(count("*").alias("ordini"), round(sum("total_amount"),2).alias("fatturato"), round(avg("total_amount"),2).alias("fatturato_medio"), countDistinct("customer_id").alias("clienti_unici")).orderBy(desc("fatturato"))

print("Top categoria per fatturato:")
df_categoria.show()

print("Top città per fatturato")

df_citta = df_silver.groupBy("city").agg(count("*").alias("ordini"), round(sum("total_amount"),2).alias("fatturato"), round(avg("total_amount"),2).alias("fatturato_medio"), countDistinct("customer_id").alias("clienti_unici")).orderBy(desc("fatturato"))

df_citta.show()

# COMMAND ----------

df_categoria.write.format("delta").mode("overwrite").saveAsTable("gold_category_analystics")
df_citta.write.format("delta").mode("overwrite").saveAsTable("gold_city_analystics")
kpi_data.write.format("delta").mode("overwrite").saveAsTable("gold_kpi_summary")

print("Tabelle Gold Salvate:")
print("- gold_category_analystics")
print("- gold_city_analystics")
print("- gold_kpi_summary")

# COMMAND ----------

print("Dashboard Gold")

print("Fatturato per categoria:")
display(df_categoria)

print("Fatturato per città:")
display(df_citta)



# COMMAND ----------

print("NOTEBOOK 2 COMPLETATO")
print("\nRisultati Gold Layer:")
print("✅ KPI business calcolati")
print("✅ Aggregazioni categoria/città")
print("✅ 3 tabelle Gold create")
print("✅ Dashboard interattive pronte")

print("\nProssimo: Notebook 3 - Scheduling & Automation")