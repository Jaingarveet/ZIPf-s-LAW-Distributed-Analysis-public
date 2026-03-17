import os
import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col 
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

# 1. Initialize Spark
findspark.init()
spark = SparkSession.builder \
    .appName("Batch_Zipf_Visualization") \
    .getOrCreate()

# 2. Define your base HDFS path
MASTER_IP = "192.168.2.111"
HDFS_BASE = f"hdfs://{MASTER_IP}:9000/user/ubuntu"

# 3. Create a clean local directory for your images
LOCAL_IMG_DIR = "./zipf_graphs"
if not os.path.exists(LOCAL_IMG_DIR):
    os.makedirs(LOCAL_IMG_DIR)
    print(f"Created local directory: {LOCAL_IMG_DIR}")

# 4. Dictionary of all your runs
# Format: "HDFS_Folder_Name": "Clean Title for the Graph"
runs_to_plot = {
    # Horizontal Strong (Fixed 5GB)
    "output_h_strong_2": "Horizontal Strong (2 Cores, 5GB)",
    "output_h_strong_4": "Horizontal Strong (4 Cores, 5GB)",
    "output_h_strong_6": "Horizontal Strong (6 Cores, 5GB)",
    
    # Horizontal Weak (Growing Data)
    "output_h_weak_2": "Horizontal Weak (2 Cores, 2GB)",
    "output_h_weak_4": "Horizontal Weak (4 Cores, 4GB)",
    
    # Vertical Strong (Fixed 5GB, 3 Executors)
    "output_v_strong_exec_1": "Vertical Strong (3 Total Cores, 5GB)",
    "output_v_strong_exec_2": "Vertical Strong (6 Total Cores, 5GB)",
    
    # Vertical Weak (Growing Data, 3 Executors)
    "output_v_weak_exec_1": "Vertical Weak (3 Total Cores, 2GB)",
    "output_v_weak_exec_2": "Vertical Weak (6 Total Cores, 4GB)"
}

print("Starting batch generation of Zipf's Law plots...\n")

# 5. Loop through all the runs
for folder_name, clean_title in runs_to_plot.items():
    hdfs_path = f"{HDFS_BASE}/{folder_name}"
    
    print(f"Processing: {clean_title}...")
    
    try:
        # Try to read the Parquet file from HDFS
        word_freq_df = spark.read.parquet(hdfs_path)
        
        # Load top 10,000 into Pandas
        top_words_pd = word_freq_df.orderBy(col("count").desc()).limit(10000).toPandas()
        
        if top_words_pd.empty:
            print(f"  -> WARNING: {folder_name} is empty. Skipping.\n")
            continue

        # Math for Zipf's
        top_words_pd['rank'] = np.arange(1, len(top_words_pd) + 1)
        max_frequency = top_words_pd['count'].max()
        top_words_pd['theoretical_zipf'] = max_frequency / top_words_pd['rank']

        # Draw Graph
        plt.figure(figsize=(10, 6))
        plt.plot(top_words_pd['rank'], top_words_pd['count'], color='blue', linewidth=2, label='Actual Reddit Data')
        plt.plot(top_words_pd['rank'], top_words_pd['theoretical_zipf'], color='red', linestyle='--', linewidth=2, label="Perfect Zipf's Law")

        plt.xscale('log')
        plt.yscale('log')
        
        # Use the clean title!
        plt.title(f"Zipf's Law: {clean_title}", fontsize=16)
        plt.xlabel("Rank (Log Scale)", fontsize=12)
        plt.ylabel("Frequency (Log Scale)", fontsize=12)
        plt.grid(True, which="both", ls="--", alpha=0.5)
        plt.legend(fontsize=12)

        # Save to the organized folder
        safe_filename = folder_name + ".png"
        save_path = os.path.join(LOCAL_IMG_DIR, safe_filename)
        
        plt.savefig(save_path, bbox_inches='tight')
        plt.close() # Close the figure so memory doesn't explode!
        
        print(f"  -> SUCCESS: Saved to {save_path}\n")

    except Exception as e:
        print(f"  -> SKIPPED: Could not load data for {folder_name}. (Error: {str(e)[:50]})\n")

spark.stop()
print("All available graphs have been generated and saved in the './zipf_graphs' folder!")