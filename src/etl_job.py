from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, split, explode, regexp_replace, desc
import sys 

spark = SparkSession.builder \
    .appName("Reddit_DB__Analysis")\
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print("Connected to PySpark")

input_path = sys.argv[1]
output_path = sys.argv[2]


try:
    # Assuming the data is in JSON format. 
    # If it is Parquet, change .json() to .parquet()
    # If it is CSV, change .json() to .csv(header=True, inferSchema=True)
    df = spark.read.json(input_path)
    
    # Check how many partitions the data was split into across your workers
    num_partitions = df.rdd.getNumPartitions()
    print(f"Success! Data loaded into {num_partitions} distributed partitions.")
    
    # Print the schema to see the columns and data types
    print("\nData Schema:")
    df.printSchema()

except Exception as e:
    print(f"Error reading from HDFS: {e}")
    sys.exit(1) 

# 1. Clean the text: lower case and remove punctuation
# This regex replaces anything that is NOT a word character or whitespace with nothing
cleaned_text = regexp_replace(lower(col("body")), r"[^\w\s]", "")

# 2. Split the text into an array of words based on spaces
words_array = split(cleaned_text, r"\s+")

# 3. Explode the array so each word gets its own row, and filter out empty strings
words_df = df.select(explode(words_array).alias("word")) \
             .filter(col("word") != "")

# 4. Group by word, count them, and sort from most frequent to least frequent
word_frequencies = words_df.groupBy("word") \
                           .count() \
                           .orderBy(desc("count"))

print("Spark transformation plan created. Ready to compute!")

# This triggers the actual calculation across your cluster. 
# It might take a minute depending on how large your database is!
print("Crunching numbers across the cluster... Please wait.")

word_freq_df = word_frequencies
word_freq_df.write.mode("overwrite").parquet(output_path)

spark.stop()
