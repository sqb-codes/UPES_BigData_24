from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split

def start_spark_streaming(input_dir):
    spark = SparkSession.builder \
        .appName("YouTubeCommentsStreaming") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

    # Read from the directory as a streaming source
    comment_stream = spark.readStream \
        .format("text") \
        .load(input_dir)

    # Process the stream (e.g., word count)
    words = comment_stream.select(explode(split(comment_stream.value, " ")).alias("word"))
    word_count = words.groupBy("word").count()

    # Write results to memory for Flask
    query = word_count.writeStream \
        .outputMode("complete") \
        .format("memory") \
        .queryName("word_counts") \
        .start()

    query.awaitTermination()

start_spark_streaming("temp_dir/streaming_dir")