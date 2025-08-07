# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline Automation & Scheduling
# MAGIC
# MAGIC ## Obiettivi
# MAGIC - Simulare arrivo dati giornalieri
# MAGIC - Creare job automatizzato
# MAGIC - Implementare monitoring
# MAGIC - Pipeline end-to-end funzionante
# MAGIC
# MAGIC ## Output
# MAGIC - Job Databricks schedulato
# MAGIC - Pipeline completa Bronze→Silver→Gold
# MAGIC - Monitoring automatico

# COMMAND ----------

from datetime import datetime, timedelta
from pyspark.sql.functions import *
import random

print("Setup della pipeline")

# simulo nuovi l'arrivo di nuovi dati con la data di ieri
yesterday = (datetime.now() - timedelta(days=1)).strftime("%d-%m-%Y")

print(f"Processiamo i dati del giorno {yesterday}")

def generate_daily_orders(date_str, num_orders=15):

  last_id_result = spark.sql("""
                             SELECT MAX(CAST(SUBSTRING(order_id, 5) AS INT)) as max_id from bronze_sales_raw
                             """).collect()[0]["max_id"]
  
  start_id = last_id_result + 1 if last_id_result else 1

  categorie = ['Elettronica', 'Abbigliamento', 'Casa', 'Sport', 'Libri']
  citta = ['Milano', 'Roma', 'Napoli', 'Torino', 'Bologna', 'Firenze', 'Palermo']

  prodotti = {
      'Elettronica': ['Smartphone', 'Laptop', 'Cuffie', 'Tablet', 'Smartwatch'],
      'Abbigliamento': ['T-shirt', 'Jeans', 'Scarpe', 'Giacca', 'Maglione'],
      'Casa': ['Lampada', 'Cuscino', 'Pentola', 'Tovaglia', 'Vaso'],
      'Sport': ['Scarpe running', 'Pallone', 'Racchetta', 'Borsa sport', 'Tappetino'],
      'Libri': ['Romanzo', 'Manuale tecnico', 'Fumetto', 'Biografia', 'Ricettario']
  }

  ordini_giornalieri = []

  for i in range(num_orders):
    categoria = random.choice(categorie)
    prodotto = random.choice(prodotti[categoria])

    ordine = {
      "order_id": f'ORD_{start_id + i:04d}',
      "customer_id": f"CUST_{random.randint(1,300):03d}",
      "product_name": prodotto,
      "category": categoria,
      "quantity": random.randint(1, 4),
      "price": __builtins__.round(random.uniform(10, 500), 2),
      "order_date": date_str,
      "city": random.choice(citta),
      "payment_method": random.choice(['Carta di credito', 'PayPal', 'Contanti'])
    }

    ordini_giornalieri.append(ordine)

  return ordini_giornalieri


nuovi_ordini = generate_daily_orders(yesterday)
print(f"Generati {len(nuovi_ordini)} nuovi ordini per la data {yesterday}")
print(f"Ordine di esempio: {nuovi_ordini[0]}")


# COMMAND ----------

# Pipeline completa Bronze→Silver→Gold

def daily_pipeline():
    
    # pipeline completa per processamento dati giornalieri
    print(f"Data processing Pipeline: {yesterday}")


    print(f"\nBronze: Caricamento nuovi dati...")

    nuovi_ordini = generate_daily_orders(yesterday)
    df_new_bronze = spark.createDataFrame(nuovi_ordini)
    df_new_bronze.write.format("delta").mode("append").saveAsTable("bronze_sales_raw")
    print(f"Bronze: {len(nuovi_ordini)} dati caricati")


    print(f"\nSilver: Pulizia e Validazione...")

    df_all_bronze = spark.table("bronze_sales_raw")
    df_silver = df_all_bronze.withColumn("order_date", to_date(col("order_date"), "dd-MM-yyyy"))
    df_silver = df_silver.withColumn("total_amount", round(col("price") * col("quantity"),2))
    df_silver.write.format("delta").mode("overwrite").saveAsTable("silver_sales_clean")
    print(f"Silver: {df_silver.count()} dati validati")


    print(f"\nGold: Aggiornamento KPI...")
    
    total_revenue = df_silver.agg(sum("total_amount")).collect()[0][0]
    total_orders = df_silver.count()
    avg_order_value = df_silver.agg(avg("total_amount")).collect()[0][0]
    unique_customers = df_silver.select("customer_id").distinct().count()

    kpi_data = spark.createDataFrame([
        {'metric': 'total_revenue', 'value': float(total_revenue)},
        {'metric': 'total_orders', 'value': float(total_orders)},
        {'metric': 'avg_order_value', 'value': float(avg_order_value)},
        {'metric': 'unique_customers', 'value': float(unique_customers)}
    ])

    kpi_data.write.format("delta").mode("overwrite").saveAsTable("gold_kpi_summary") 

    df_categoria = df_silver.groupBy("category").agg(count("*").alias("ordini"), round(sum("total_amount"),2).alias("fatturato"), round(avg("total_amount"),2).alias("ordine_medio")).orderBy(desc("fatturato"))

    df_categoria.write.format("delta").mode("overwrite").saveAsTable("gold_category_amalystics")
    print(f"Gold: KPI aggiornati")

    df_citta = df_silver.groupBy("city").agg(count("*").alias("ordini"), round(sum("total_amount"), 2).alias("fatturato"), countDistinct("customer_id").alias("clienti_unici")).orderBy(desc("fatturato"))
    df_citta.write.format("delta").mode("overwrite").saveAsTable("gold_city_analytics")

    return df_silver.count()

total_records = daily_pipeline()
print(f"Total records processed: {total_records}")

# COMMAND ----------

# Cella di monitoraggio

def pipeline_monitoring():
    print("Pipeline Status")

    bronze_count = spark.table("bronze_sales_raw").count()
    silver_count = spark.table("silver_sales_clean").count()

    print(f"Bronze: {bronze_count} records")
    print(f"Silver: {silver_count} records")

    ultimi_giorni = spark.sql("""
                              select order_date, count(*) as ordini
                              from silver_sales_clean
                              group by order_date
                              order by order_date desc
                              limit 7
                              """)

    print("\nNumero Record ultimi giorni di attività:")
    ultimi_giorni.show()


pipeline_monitoring()

# COMMAND ----------

print("NOTEBOOK 3 COMPLETATO - Pipeline automation pronta")
print("\nProssimi passi per scheduling:")
print("1. Creare Databricks Job dalla UI")
print("2. Schedulare esecuzione daily_pipeline()")
print("3. Configurare monitoring automatico")

print(f"\n📊 STATUS FINALE:")
print(f"✅ Pipeline testata e funzionante")
print(f"✅ Dati incrementali processati")
print(f"✅ Tutte le tabelle Gold aggiornate")
print(f"✅ Ready per automazione")