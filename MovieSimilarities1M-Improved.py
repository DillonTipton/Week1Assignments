import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
import pandas as pd
from math import sqrt
# TODO:
# Refactor to use DataFrames
# Save output to Parquet
# ----------------------------
# Main Spark setup
# ----------------------------
def computeCosineSimilarity(spark, data):
    # Compute xx, xy and yy columns
    pairScores = data \
      .withColumn("xx", func.col("rating1") * func.col("rating1")) \
      .withColumn("yy", func.col("rating2") * func.col("rating2")) \
      .withColumn("xy", func.col("rating1") * func.col("rating2")) 

    # Compute numerator, denominator and numPairs columns
    calculateSimilarity = pairScores \
      .groupBy("movie1", "movie2") \
      .agg( \
        func.sum(func.col("xy")).alias("numerator"), \
        (func.sqrt(func.sum(func.col("xx"))) * func.sqrt(func.sum(func.col("yy")))).alias("denominator"), \
        func.count(func.col("xy")).alias("numPairs")
      )

    # Calculate score and select only needed columns (movie1, movie2, score, numPairs)
    result = calculateSimilarity \
      .withColumn("score", \
        func.when(func.col("denominator") != 0, func.col("numerator") / func.col("denominator")) \
          .otherwise(0) \
      ).select("movie1", "movie2", "score", "numPairs")

    return result
spark = SparkSession.builder.appName("MovieSimilarities").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel('WARN')
# ----------------------------
# S3 paths (CHANGE THIS)
# For local, use ml-100k
# For server, use s3a
# ----------------------------
MOVIES_PATH = "s3a://rev-spark-609375805055-us-east-2-an/ml-1m/movies.dat"
RATINGS_PATH = "s3a://rev-spark-609375805055-us-east-2-an/ml-1m/ratings.dat"
#MOVIES_PATH = "./ml-100k/u.item"
#RATINGS_PATH = "./ml-100k/u.data"
movieNamesSchema = StructType([
    StructField("movieID", IntegerType(), True),
    StructField("movieTitle", StringType(), True)
])
ratingsSchema = StructType([
    StructField("userID", IntegerType(), True),
    StructField("movieID", IntegerType(), True),
    StructField("rating", IntegerType(), True),
    StructField("timestamp", LongType(), True)
])
# ----------------------------
# Load and broadcast movie names
# ----------------------------
print("Loading movie names from S3...")
names = (
    spark.read
    .option("sep", "|")
    .option("charset", "ISO-8859-1")
    .schema(movieNamesSchema)
    .csv(MOVIES_PATH)
)
#nameDict = func.broadcast(names)
namesDict = dict(names.select("movieID", "movieTitle").rdd.map(tuple).collect())
nameDict = spark.sparkContext.broadcast(namesDict)
# ----------------------------
# Load ratings from S3
# ----------------------------
print("Loading ratings from S3...")
movies = (
    spark.read
    .option("sep", "\t")
    .schema(ratingsSchema)
    .csv(RATINGS_PATH)
)
# ----------------------------
# Build movie pairs
# ----------------------------

ratings = movies.select("userId", "movieId", "rating")

# Emit every movie rated together by the same user.
# Self-join to find every combination.
# Select movie pairs and rating pairs
moviePairs = ratings.alias("ratings1") \
      .join(ratings.alias("ratings2"), (func.col("ratings1.userId") == func.col("ratings2.userId")) \
            & (func.col("ratings1.movieId") < func.col("ratings2.movieId"))) \
      .select(func.col("ratings1.movieId").alias("movie1"), \
        func.col("ratings2.movieId").alias("movie2"), \
        func.col("ratings1.rating").alias("rating1"), \
        func.col("ratings2.rating").alias("rating2"))


moviePairSimilarities = computeCosineSimilarity(spark, moviePairs).cache()

# ----------------------------
# Compute similarities
# ----------------------------

# Optional: save full results
#moviePairSimilarities.write.parquet("s3a://rev-spark-609375805055-us-east-2-an/output/movie-sims")
moviePairSimilarities.write.parquet("./movie-sims.parquet")

# ----------------------------
# Query similar movies
# ----------------------------
if (len(sys.argv) > 1):
    movieID = int(sys.argv[1])
    scoreThreshold = 0.97
    coOccurrenceThreshold = 50
    #filteredResults = moviePairSimilarities.filter(lambda pairSim: (pairSim[0][0] == movieID or pairSim[0][1] == movieID) and pairSim[1][0] > scoreThreshold and pairSim[1][1] > coOccurrenceThreshold)
    filteredResults = moviePairSimilarities.filter(
        ((func.col("movie1") == movieID) | (func.col("movie2") == movieID))
        & (func.col("score") > scoreThreshold)
        & (func.col("numPairs") > coOccurrenceThreshold)
    )

    #results = filteredResults.map(lambda pairSim: (pairSim[1], pairSim[0])).sortByKey(ascending=False).take(10)
    results = filteredResults.sort(func.col("score").desc()).take(10)
    print("Filtering done")
    print("\nTop 10 similar movies for:",
            nameDict.value[str(movieID)])

    for movieIDA, movieIDB, score, strength in results:
        similarMovieID = movieIDA if movieIDA != movieID else movieIDB
        print(
            nameDict.value[similarMovieID],
            "\tscore:", score,
            "\tstrength:", strength
        )
spark.stop()