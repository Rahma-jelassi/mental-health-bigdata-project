"""
Spark Streaming Job - Version Simplifiée
Traite en temps réel les posts Reddit depuis Kafka
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import re
import os

# Mots-clés de détresse
DISTRESS_KEYWORDS = [
    'suicide', 'kill myself', 'end it all', 'no reason to live',
    'hopeless', 'worthless', 'give up', "can't go on",
    'depressed', 'anxious', 'panic', 'overwhelmed',
    'lonely', 'isolated', 'scared', 'die'
]

def initialize_spark():
    """Initialise Spark avec support Kafka"""
    print("=" * 70)
    print("🚀 INITIALISATION SPARK STREAMING")
    print("=" * 70)
    
    spark = SparkSession.builder \
        .appName("MentalHealthStreaming") \
        .master("local[*]") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
        .config("spark.sql.streaming.checkpointLocation", "../data/checkpoints") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    print("✅ Spark initialisé\n")
    return spark


def clean_text_udf():
    """UDF pour nettoyer le texte"""
    def clean(text):
        if not text or text == "":
            return ""
        text = str(text).lower()
        text = re.sub(r'http\S+|www\S+', '', text)
        text = re.sub(r'[^a-z0-9\s]', '', text)
        text = re.sub(r'\s+', ' ', text).strip()
        return text
    
    return udf(clean, StringType())


def calculate_risk_score_udf():
    """UDF pour calculer le score de risque"""
    def calculate(text):
        if not text:
            return 0
        
        score = 0
        text_lower = str(text).lower()
        
        for keyword in DISTRESS_KEYWORDS:
            if keyword in text_lower:
                score += 10
        
        return min(score, 100)
    
    return udf(calculate, IntegerType())


def process_stream(spark):
    """Traite le stream Kafka"""
    
    # Schéma des données Reddit
    schema = StructType([
        StructField("id", StringType()),
        StructField("title", StringType()),
        StructField("text", StringType()),
        StructField("author", StringType()),
        StructField("subreddit", StringType()),
        StructField("created_utc", DoubleType()),
        StructField("score", IntegerType()),
        StructField("num_comments", IntegerType()),
        StructField("url", StringType()),
        StructField("timestamp", StringType())
    ])
    
    print("📡 Connexion à Kafka topic: reddit-posts...")
    
    # Lire depuis Kafka
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "reddit-posts") \
        .option("startingOffsets", "latest") \
        .load()
    
    print("✅ Connecté à Kafka\n")
    
    # Parser le JSON
    df = df.selectExpr("CAST(value AS STRING) as json") \
           .select(from_json(col("json"), schema).alias("data")) \
           .select("data.*")
    
    print("🔄 Application du preprocessing...\n")
    
    # 1. Combiner titre + texte
    df = df.withColumn("combined_text", 
                       concat_ws(" ", col("title"), col("text")))
    
    # 2. Nettoyage
    clean_udf = clean_text_udf()
    df = df.withColumn("cleaned_text", clean_udf(col("combined_text")))
    
    # 3. Score de risque
    risk_udf = calculate_risk_score_udf()
    df = df.withColumn("risk_score", risk_udf(col("combined_text")))
    
    # 4. Timestamp de traitement
    df = df.withColumn("processed_at", current_timestamp())
    
    # Colonnes finales
    df_output = df.select(
        "id",
        "author",
        "subreddit",
        "title",
        "risk_score",
        "score",
        "num_comments",
        "timestamp",
        "processed_at"
    )
    
    return df_output


def main():
    """Fonction principale"""
    print("\n" + "=" * 70)
    print("🎯 SPARK STREAMING - MENTAL HEALTH ANALYSIS")
    print("=" * 70)
    print("⚠️  IMPORTANT:")
    print("   Terminal 1 : Kafka")
    print("   Terminal 2 : Reddit Collector")
    print("   Terminal 3 : Ce script (Spark)")
    print("=" * 70)
    
    input("\n▶️  Appuie sur ENTRÉE pour démarrer Spark...\n")
    
    spark = initialize_spark()
    
    try:
        # Traiter le stream
        df_processed = process_stream(spark)
        
        # Afficher dans la console
        print("=" * 70)
        print("📊 AFFICHAGE DES POSTS TRAITÉS (toutes les 10s)")
        print("=" * 70)
        print()
        
        query = df_processed \
            .writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", False) \
            .trigger(processingTime='10 seconds') \
            .start()
        
        print("✅ Spark Streaming actif!")
        print("📊 Les posts traités s'afficheront ci-dessous...\n")
        print("⏹️  CTRL+C pour arrêter\n")
        
        # Attendre
        query.awaitTermination()
    
    except KeyboardInterrupt:
        print("\n\n⏹️  Arrêt de Spark Streaming...")
        spark.stop()
        print("✅ Spark arrêté proprement")
    except Exception as e:
        print(f"\n❌ Erreur: {e}")
        import traceback
        traceback.print_exc()
        spark.stop()


if __name__ == "__main__":
    main()