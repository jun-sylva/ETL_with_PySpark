from pyspark.sql import SparkSession
from pyspark.sql.functions import col, min, max
import os
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"  # ou ton IP locale (192.168.x.x)
os.environ["SPARK_UI_PORT"] = "4050"  # Changer le port UI de Spark

# Initialisation de SparkSession
spark = SparkSession.builder \
    .appName("Summer") \
    .master("local[*]") \
    .getOrCreate()

# Création d'un DataFrame simple
# data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
# columns = ["Nom", "Âge"]
# df = spark.createDataFrame(data, columns)

# extract file
path = "/Users/junior/Documents/github/pyspark/summer.csv"
print(f"Lecture du fichier CSV depuis : {path}")
# Charger un fichier CSV en DataFrame
df = spark.read.csv(path, header=True, inferSchema=True)
df.printSchema()
# liste pour chaque type de medal Gold, Silver Bronze
gold_df = df.filter(col("Medal") == "Gold")
silver_df = df.filter(col("Medal") == "Silver")
bronze_df = df.filter(col("Medal") == "Bronze")

# 4️⃣ Afficher les résultats
# df_filtered.show()

# 5️⃣ Sauvegarder les résultats au format Parquet
# df_filtered.write.mode("overwrite").parquet("output/employees_high_salary")

df.printSchema()
# gold_df.show()
# silver_df.show()
# bronze_df.show()

count_ath_gold = gold_df.groupBy("Athlete").count().orderBy(col("count").desc())
count_ath_silver = silver_df.groupBy("Athlete").count().orderBy(col("count").desc())
count_ath_bronze = bronze_df.groupBy("Athlete").count().orderBy(col("count").desc())
count_ath_gold.show()
count_ath_silver.show()
count_ath_bronze.show()

# 6️⃣ Arrêter Spark
spark.stop()

#spark.stop()
