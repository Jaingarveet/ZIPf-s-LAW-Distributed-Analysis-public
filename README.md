This project implements a distributed data pipeline to process and analyze the Webis-TLDR-17 dataset (approx. 20GB, 3.8M posts) on the Swedish Science Cloud. The primary objective is to verify Zipf's Law—a statistical phenomenon where word frequency is inversely proportional to its rank—using a scalable Spark/HDFS cluster.

## 🛠️ Tech Stack & Infrastructure

**Core Frameworks & Languages**
* **Apache Spark (PySpark):** The core distributed computing engine used for the ETL pipeline, parallel processing, and global Shuffle aggregations.
* **Python 3:** The primary language used for the Spark application logic and rendering the final scalability visualizations.
* **Bash:** Used for automating the benchmarking loops, managing environment variables, and submitting Spark jobs to the cluster.

**Infrastructure & Storage**
* **HDFS (Hadoop Distributed File System):** The distributed storage layer used to house the 5GB of raw Reddit JSON data and the partitioned Parquet output files.
* **Ubuntu Linux VMs:** A 4-node standalone cluster (1 Master, 3 Workers) utilizing `ssc.medium` instances (2 vCPUs, 4GB RAM) for physical compute.

**Data Science & Visualization**
* **Pandas & NumPy:** Used on the Master node to process the final, reduced Top 10,000 word arrays.
* **Matplotlib:** Used to generate the Zipf's Law frequency distributions and the horizontal/vertical performance benchmark charts.

## Contributors:
* Infrastructure Engineer: Stanisław Kaminski
* Data Engineer (Ingestion): Aidan Lee-Gilligan
* Core Logic Developer: Radosław Gersz
* Test and QA engineer: Garveet Jain
* Analyst and Reporter: Jainyu Liu


# The architecture diagram for the project: 

![Architecture diagram](https://github.com/user-attachments/assets/fce102d2-1f62-474e-9b8f-54605d0e6248)

# Install Python dependencies
pip install -r requirements.txt

# Download data and unzip into HDFS
To get the compressed data run the following:
wget -r https://zenodo.org/api/records/1043504/files/corpus-webis-tldr-17.zip/content

To unzip the first X gbs and pipe it into hdfs run this replacing the paths and the X:
unzip -p /path/to/zip reddit_Xgb.json | head -c XG | sed '$d' | hdfs dfs -put - /destination/path


## 🚀 Running the Pipeline

The following scripts must be executed from the **Master Node** of the cluster. 

### Step 1: Execute the Scaling Benchmarks
This automated bash script runs the complete suite of scaling experiments (Horizontal Strong/Weak and Vertical Strong/Weak) and logs the execution times.

\`\`\`bash
bash scripts/run_benchmark.sh
\`\`\`

**⚠️ Critical Execution Warnings:**
* **Runtime:** This script runs multiple 5GB Spark jobs consecutively. Please allow approximately **45–60 minutes** for the full suite to complete.
* **Memory Constraints:** The script explicitly limits Spark to `--executor-memory 2400M`. **Do not alter this value.** The assigned `ssc.medium` instances possess limited usable RAM after OS and Hadoop daemon overhead. Increasing the executor memory limit will trigger JVM `OutOfMemory` errors and crash the worker nodes.
* **benchmark_logs.txt:** The runs store the execution time inside the benchmark_logs.txt file which will contain some extra output that comes at time of starting a spark application. 

### Step 2: Generate Zipf's Law Visualizations
After the benchmarks complete, generate the frequency distribution plots from the HDFS output. Because extracting the Top 10,000 words requires a massive cluster-wide global sort, this script **must** be submitted to the cluster via `spark-submit`.

\`\`\`bash
spark-submit \
    --master spark://192.168.2.111:7077 \
    --executor-cores 2 \
    --executor-memory 2400M \
    src/generate_all_zipf_plots.py
\`\`\`

**⚠️ Critical Execution Warnings:**
* **The Global Sort Pause:** During execution, the progress bar will consistently pause at `[Stage X:======> (1 + 0) / 2]`. **Do not terminate the process (Ctrl+C).** The console is not frozen; the cluster is performing a heavy network Shuffle to globally sort millions of word pairs. This step takes 60–90 seconds per directory.
* **Avoid Local Execution:** Do not run this script using standard `python3`. Doing so forces the Master VM to process all HDFS partitions locally, which will exhaust the Master node's RAM and cause an immediate application failure.

### Step 3: Generate Performance & Scalability Graphs
To generate the final 8 scaling graphs (Runtime vs. Workers, Speedup/Efficiency Factor vs. Ideal Baseline) we used manual plotting using excel since we needed manual cleanups of logs.

*Note: This script requires `matplotlib` and `pandas`. All generated `.png` files will be saved directly to the corresponding directories mentioned in the scripts.*
