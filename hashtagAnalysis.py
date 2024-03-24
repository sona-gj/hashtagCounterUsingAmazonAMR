from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col, lower, when, coalesce

# Initialize Spark session
spark = SparkSession.builder.appName("HashtagExtractor").getOrCreate()

# Read the JSON file
json_file_path = "tinyTwitter.json"
df = spark.read.json(json_file_path)

# Extract hashtags
hashtags_df_properties = df.select(
    explode(split(col("value.properties.text"), " ")).alias("word")).filter(col("word").rlike("^#\\S+"))
properties_hashtag_counts = hashtags_df_properties.groupBy("word").count()

hashtags_df_doc = df.select(
    explode(split(col("doc.text"), " ")).alias("word")).filter(col("word").rlike("^#\\S+"))
doc_hashtag_counts = hashtags_df_doc.groupBy("word").count()

hashtags_df_description = df.select(
    explode(split(col("doc.user.description"), " ")).alias("word")).filter(col("word").rlike("^#\\S+"))
description_hashtag_counts = hashtags_df_description.groupBy("word").count()


# Union all DataFrames
combined_hashtags_df = hashtags_df_properties.union(hashtags_df_doc).union(hashtags_df_description)

# Convert hashtags to lowercase
combined_hashtags_df = combined_hashtags_df.withColumn("word", lower(col("word")))

# Count occurrences of each hashtag
combined_hashtag_counts = combined_hashtags_df.groupBy("word").count()

# Arrange hashtags based on count and select top 20
top_20_hashtags = combined_hashtag_counts.orderBy("count", ascending=False).limit(20)

# Show the top 20 hashtags
top_20_hashtags.show()

# Stop Spark session
spark.stop()
