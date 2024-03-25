from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, lower, col
from pyspark.sql.types import ArrayType, StringType, StructType, StructField
import re

# Initialize Spark session
spark = SparkSession.builder.appName("Hashtag Finder").getOrCreate()

# Read data from "smallTwitter.json"
df = spark.read.json("smallTwitter.json")

# Regular expression pattern to remove trailing periods
pattern_to_remove_period = re.compile(r'(\w+)\.+$')

# Function to find hashtags in a text
def find_hashtags(text):
    if isinstance(text, str):
        cleaned_text = re.sub(pattern_to_remove_period, r'\1', text)
        hashtags = re.findall(r'\#\w+', cleaned_text, flags=re.IGNORECASE)
        return hashtags
    else:
        return []

# Register UDF to find all hashtags in a text
find_all_hashtags_udf = spark.udf.register("find_all_hashtags", find_hashtags, ArrayType(StringType()))

# Function to extract hashtags from a column
def extract_hashtags(column_name):
    if column_name == "doc.entities.user_mentions":
        user_mentions_text_df = df.select(explode("doc.entities.user_mentions.name").alias("user_mention_name"))
        user_mentions_hashtag_df = user_mentions_text_df.withColumn('hashtags', find_all_hashtags_udf(col('user_mention_name'))).select(
            explode('hashtags').alias('word')
        )
        return user_mentions_hashtag_df
    else:
        return df.select(explode(find_all_hashtags_udf(col(column_name))).alias('word'))

# Columns to extract hashtags from
columns_to_extract_hashtags_from = [
    "value.properties.text",
    "doc.user.description",
    "doc.text",
    "doc.entities.user_mentions",
    "doc.user.name"
]

# Schema for the DataFrame
schema = StructType([StructField("word", StringType(), True)])

# Create an empty DataFrame with the desired schema
all_hashtags_df = spark.createDataFrame([], schema)

# Union results from all columns
for column_name in columns_to_extract_hashtags_from:
    column_hashtags_df = extract_hashtags(column_name)
    all_hashtags_df = all_hashtags_df.unionByName(column_hashtags_df)

# Calculate hashtag counts
hashtag_counts = all_hashtags_df.groupBy(lower(all_hashtags_df["word"]).alias("hashtag")).count()

# Sort and limit the results
sorted_hashtag_counts = hashtag_counts.orderBy(col("count").desc()).limit(20)

# Show the results
sorted_hashtag_counts.show()
