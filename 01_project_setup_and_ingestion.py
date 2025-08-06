# Databricks notebook source
# MAGIC %md
# MAGIC # Progetto ETL Pipeline - Sales Data
# MAGIC
# MAGIC ## Cosa voglio fare
# MAGIC Creare una pipeline automatica che ogni giorno:
# MAGIC - Prende i dati di vendita 
# MAGIC - Li controlla e pulisce
# MAGIC - Li trasforma per fare analisi
# MAGIC - Li salva per creare report
# MAGIC
# MAGIC ## Piano di lavoro
# MAGIC 1. Generare dati finti realistici
# MAGIC 2. Creare controlli qualità 
# MAGIC 3. Fare trasformazioni
# MAGIC 4. Schedulare esecuzione automatica
# MAGIC 5. Dashboard finale
# MAGIC
# MAGIC N.B.: Uso Databricks Free Edition quindi alcune cose saranno essere limitate

# COMMAND ----------

from datetime import datetime, timedelta
import random
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, sum, avg, to_date, year, month
#from pyspark.sql.functions import * (non importava correttamente e creava conglitti di nomi con col)
from pyspark.sql.types import *

print("Inizio progetto ETL Pipeline")
print("Data di Oggi:", datetime.now().strftime("%d-%m-%Y"))

#parametri per generare i successivi dati:
num_record = 1000 # numero di ordini fittizzi
num_customers = 200 # numero di clienti diversi
giorni_indietro = 90 # periodo di analisi dei dati (ultimi 3 mesi)

print(f"Andrò a generare {num_record} ordini per {num_customers} clienti")

# COMMAND ----------

print("Inizio generazione dati di vendita")

# Dati base
categorie = ['Elettronica', 'Abbigliamento', 'Casa', 'Sport', 'Libri']
citta = ['Milano', 'Roma', 'Napoli', 'Torino', 'Bologna', 'Firenze', 'Palermo']

prodotti = {
    'Elettronica': ['Smartphone', 'Laptop', 'Cuffie', 'Tablet', 'Smartwatch'],
    'Abbigliamento': ['T-shirt', 'Jeans', 'Scarpe', 'Giacca', 'Maglione'],
    'Casa': ['Lampada', 'Cuscino', 'Pentola', 'Tovaglia', 'Vaso'],
    'Sport': ['Scarpe running', 'Pallone', 'Racchetta', 'Borsa sport', 'Tappetino'],
    'Libri': ['Romanzo', 'Manuale tecnico', 'Fumetto', 'Biografia', 'Ricettario']
}

ordini = []

for i in range(num_record):
    categoria = random.choice(categorie)
    prodotto = random.choice(prodotti[categoria])
    giorni_fa = random.randint(1, giorni_indietro)
    data_ordine = datetime.now() - timedelta(days=giorni_fa)

    ordine = {
        "order_id": f"ORD_{i+1:04d}",
        "customer_id": f"CUST_{random.randint(1, num_customers):03d}",
        "product_name": prodotto,
        "category": categoria,
        "quantity": random.randint(1, 4),
        "price": __builtins__.round(random.uniform(10, 500), 2),
        "order_date": data_ordine.strftime("%d-%m-%Y"),
        "city": random.choice(citta),
        "payment_method": random.choice(['Carta di credito', 'Contanti', 'PayPal'])
    }

    ordini.append(ordine)

#date_ordini = [datetime.strptime(ord['order_date'], "%d-%m-%Y") for ord in ordini]

print(f"Generati {len(ordini)} ordini")
#print(f"Periodo: {min(date_ordini).strftime('%d-%m-%Y')} - {max(date_ordini).strftime('%d-%m-%Y')}")

print("\nPrimi 3 ordini generati:")
for i in range(3):
    print(f"Ordine: {i+1}: {ordini[i]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## BRONZE LAYER - Raw Data
# MAGIC
# MAGIC **Obiettivo**: Convertire i dati grezzi in formato Spark DataFrame e salvarli come tabella Delta.
# MAGIC
# MAGIC **Cosa facciamo:**
# MAGIC - Conversione lista Python → Spark DataFrame
# MAGIC - Verifica schema e struttura dati
# MAGIC - Salvataggio in formato Delta per persistenza
# MAGIC - Nessuna trasformazione, solo validazione che i dati siano leggibili (le trasformazioni saranno il silver e gold layer)
# MAGIC
# MAGIC **Pattern**: Primo step dell'architettura medallion - i dati raw, al naturale

# COMMAND ----------

print("BRONZE LAYER: Conversione dati grezzi")

df_bronze = spark.createDataFrame(ordini)

print(f"Creata tabella temporanea 'bronze' con {df_bronze.count()} record")
print(f"colonne: {df_bronze.columns}")

print("Schema DataFrame Bronze:")
df_bronze.printSchema()

# notiamo già che la colonna order_date è di tipo stringa, non date
# non ci sono ancora correzioni di tipi nel bronze layer. Solo dati grezzi
# creare tutte celle indipendenti e a tenuta stagna aiuta nel debug a identificare la posizione dell'errore

print("\nPrime 5 righe:")
df_bronze.show(5)

df_bronze.write.format("delta").mode("overwrite").saveAsTable("bronze_sales_raw")
print("Dati salvati in tabella: bronze_sales_raw")

# COMMAND ----------

# Verifica rapida che la tabella sia stata salvata e sia permanente
spark.sql("SHOW TABLES").show()

# la tabella c'è ma vedo anche quelle dei miei altri notebook. provo ad usare vie alternative per filtrare solo quella che mi interessa

try:
    spark.sql("DESCRIBE bronze_sales_raw").show()
    print("Tabella trovata")
except:
    print("Tabella non trovata")

record_saved = spark.sql("SELECT COUNT(*) as total FROM bronze_sales_raw").collect()[0]['total']
print(f"Record salvati: {record_saved}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## SILVER LAYER - Data Quality & Cleaning
# MAGIC
# MAGIC **Obiettivo**: Pulire e validare i dati, convertire i tipi, applicare controlli qualità.
# MAGIC
# MAGIC **Cosa facciamo:**
# MAGIC - Conversione order_date da string a date
# MAGIC - Calcolo total_amount (quantity × unit_price)
# MAGIC - Controlli qualità automatici (null, duplicati, range valori)
# MAGIC - Schema standardizzato e validato
# MAGIC
# MAGIC **Pattern**: Dati puliti e pronti per le trasformazioni business del Gold layer.

# COMMAND ----------

print("Silver Layer: Pulizia dati e controlli qualità")

df_silver = spark.table("bronze_sales_raw")

df_silver = df_silver.withColumn("order_date", to_date(col("order_date"), "dd-MM-yyyy"))
df_silver = df_silver.withColumn("total_amount",round(col("quantity") * col("price"),2))

print("Conversioni Completate: order_date di tipo date e creata colonna total_amount")

print("\nControlli Qualità:")

total_records = df_silver.count()

# null_counts = df_silver.filter(col("order_id").isNull()).count()
null_counts = df_silver.select([count(when(col(c).isNull(), c)).alias(c) for c in df_silver.columns]).collect()[0]
print("Valori nulli per colonna:")
for col in df_silver.columns:
  null_count = null_counts[col]
  if null_count > 0:
    print(f"{col}: {null_count}")
  else:
    print(f"{col}: Nessun valore null")

#controllo che date sia effettivamente in formato data
df_silver.printSchema()

# COMMAND ----------

# continuo con i controlli qualità del dato

duplicati = df_silver.groupBy("order_id").count().where('count > 1').count()
# uso il where("count > 1") perché filter(col("count") > 1) crea un conflitto tra colonna e funzione count
print(f"Ordini duplicati: {duplicati}")

print("\nControlli ambito business:")
min_amount = df_silver.agg({"total_amount": "min"}).collect()[0][0]
max_amount = df_silver.agg({"total_amount": "max"}).collect()[0][0]
avg_amount = df_silver.agg({"total_amount": "avg"}).collect()[0][0]

print(f"Total amount - Min: €{min_amount:.2f}, Max: €{max_amount:.2f}, Avg: €{avg_amount:.2f}")

cat_count = df_silver.select("category").distinct().count()
print(f"Numero di categorie prodotto: {cat_count}")

print("\nSchema Silver finale")
df_silver.printSchema()
print()
df_silver.show(3)

df_silver.write.format("delta").mode("overwrite").saveAsTable("silver_sales_clean")
print("Dati salvati in tabella: silver_sales_clean")

count_silver = spark.sql("SELECT COUNT(*) AS total FROM silver_sales_clean").collect()[0]['total']
print(f"Record salvati: {count_silver}")


# COMMAND ----------

# MAGIC %sql
# MAGIC show tables

# COMMAND ----------

# MAGIC %md
# MAGIC Notebook 1 COMPLETATO - Setup e pulizia dati
# MAGIC
# MAGIC Prossimo: Notebook 2 - Business transformations (GOLD layer)
# MAGIC
# MAGIC ####**PREVIEW GOLD LAYER** - Fatturato per categoria (analisi KPI nel prossimo notebook):####

# COMMAND ----------

category_revenue = spark.sql("""
                             SELECT
                                 category,
                                 count(*) as ordini,
                                 round(sum(total_amount), 2) as fatturato_totale,
                                 round(avg(total_amount), 2) as fatturato_medio
                             FROM silver_sales_clean
                             GROUP BY category
                             ORDER BY fatturato_totale DESC
                             """)

display(category_revenue)