This project implements a distributed data pipeline to process and analyze the Webis-TLDR-17 dataset (approx. 20GB, 3.8M posts) on the Swedish Science Cloud. The primary objective is to verify Zipf's Law—a statistical phenomenon where word frequency is inversely proportional to its rank—using a scalable Spark/HDFS cluster.

# Install Python dependencies
pip install -r requirements.txt

# Download data and unzip into HDFS
To get the compressed data run the following:
wget -r https://zenodo.org/api/records/1043504/files/corpus-webis-tldr-17.zip/content

To unzip the first X gbs and pipe it into hdfs run this replacing the paths and the X:
unzip -p /path/to/zip reddit_Xgb.json | head -c XG | sed '$d' | hdfs dfs -put - /destination/path