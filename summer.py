from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# 1️⃣ Initialisation de SparkSession
spark = SparkSession.builder \
    .appName("Job_Spark_Example") \
    .master("local[*]") \  # Utilise toutes les cores disponibles en local
    .getOrCreate()

# 2️⃣ Charger un fichier CSV en DataFrame
df = spark.read.csv("data/employees.csv", header=True, inferSchema=True)

# 3️⃣ Traitement des données : filtrer les employés avec un salaire > 50 000
df_filtered = df.filter(col("salary") > 50000)

# 4️⃣ Afficher les résultats
df_filtered.show()

# 5️⃣ Sauvegarder les résultats au format Parquet
df_filtered.write.mode("overwrite").parquet("output/employees_high_salary")

# 6️⃣ Arrêter Spark
spark.stop()
