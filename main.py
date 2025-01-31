from pyspark.sql import SparkSession

# Initialisation de SparkSession
spark = SparkSession.builder \
    .appName("Test PySpark avec PyCharm") \
    .getOrCreate()

# Création d'un DataFrame simple
data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
columns = ["Nom", "Âge"]
df = spark.createDataFrame(data, columns)

# Affichage du DataFrame
df.show()

# Arrêter Spark
spark.stop()
