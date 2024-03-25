from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, lower, col
from pyspark.sql import functions as F
from pyspark.sql.functions import udf, explode, col
from pyspark.sql.types import ArrayType, StringType
import re

spark = SparkSession.builder.appName("Hashtag Frequency").getOrCreate()
df = spark.read.json("smallTwitter.json")
pattern_to_remove_period = re.compile(r'(\w+)\.+$')


# Enhanced UDF to remove emojis and match hashtags in a case-insensitive manner
def find_hashtags(text):
    if isinstance(text, str):
        cleaned_text = re.sub(pattern_to_remove_period, r'\1', text)
        hashtags = re.findall(r'\#\w+', cleaned_text, flags=re.IGNORECASE)
        return hashtags
    else:
        return []

# Register the UDF with Spark
find_all_hashtags_udf = udf(find_hashtags, ArrayType(StringType()))

text_df = df.select("value.properties.text")
description_df = df.select("doc.user.description")
doc_text_df = df.select("doc.text")
user_mentions_text_df = df.select(explode("doc.entities.user_mentions.name").alias("user_mention_name"))
doc_user_name_df = df.select("doc.user.name")

hashtags_df = text_df.withColumn('hashtags', find_all_hashtags_udf(col('text'))).select(
    explode('hashtags').alias('word')
)

desc_hashtag_df = description_df.withColumn('hashtags', find_all_hashtags_udf(col('description'))).select(
    explode('hashtags').alias('word')
)

desc_text_hashtag_df = doc_text_df.withColumn('hashtags', find_all_hashtags_udf(col('text'))).select(
    explode('hashtags').alias('word')
)

user_mentions_hashtag_df = user_mentions_text_df.withColumn('hashtags', find_all_hashtags_udf(col('user_mention_name'))).select(
    explode('hashtags').alias('word')
)

doc_user_name_hashtag_df = doc_user_name_df.withColumn('hashtags', find_all_hashtags_udf(col('name'))).select(
    explode('hashtags').alias('word')
)


hashtag_counts = hashtags_df.groupBy(lower(hashtags_df["word"]).alias("hashtag")).count()
hashtag_desc_counts = desc_hashtag_df.groupBy(lower(desc_hashtag_df["word"]).alias("hashtag")).count()
hashtag_doc_counts = desc_text_hashtag_df.groupBy(lower(desc_text_hashtag_df["word"]).alias("hashtag")).count()
hashtag_user_mentions_counts = user_mentions_hashtag_df.groupBy(lower(user_mentions_hashtag_df["word"]).alias("hashtag")).count()
hashtag_doc_user_name_counts = doc_user_name_hashtag_df.groupBy(lower(doc_user_name_hashtag_df["word"]).alias("hashtag")).count()

union_df = hashtag_counts.unionAll(hashtag_desc_counts).unionAll(hashtag_doc_counts).unionAll(hashtag_user_mentions_counts).unionAll(hashtag_doc_user_name_counts)

grouped_df = union_df.groupBy("hashtag").agg(F.sum("count").alias("total_count"))

sorted_grouped_df = grouped_df.orderBy(F.desc("total_count")).limit(20)

sorted_grouped_df.show()