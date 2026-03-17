#!/bin/bash
# run_benchmark.sh
# Automated scaling tests: Horizontal (Strong/Weak) & Vertical (Strong/Weak)

# ==========================================
# 1. CONTROL PANEL
# ==========================================
MASTER_IP="192.168.2.111"
SPARK_MASTER="spark://$MASTER_IP:7077"
HDFS_BASE="hdfs://$MASTER_IP:9000/user/ubuntu"

# Paths
PYTHON_JOB="etl_job.py"
LOG_FILE="benchmark_logs.txt"

# Clear previous logs
echo "Starting Spark Scaling Benchmarks" > $LOG_FILE
echo "=================================" >> $LOG_FILE

# ==========================================
# 2. HORIZONTAL STRONG SCALING (Fixed Data: 5GB)
# ==========================================
echo "--- Running Horizontal Strong Scaling Tests ---"

# 2 cores (1 worker), 4 cores (2 workers), 6 cores (3 workers)
for CORES in 2 4 6
do
    INPUT_DIR="$HDFS_BASE/reddit-db/5gig/reddit_5gb.json"
    OUTPUT_DIR="$HDFS_BASE/output_h_strong_$CORES"
    
    echo "Testing Horizontal Strong Scaling with $CORES total cores..."
    echo "TEST: Horizontal Strong | CORES: $CORES | DATA: 5GB" >> $LOG_FILE
    
    # --executor-cores 2 FORCES Spark to use multiple VMs
    { time spark-submit \
        --master $SPARK_MASTER \
        --executor-cores 2 \
        --total-executor-cores $CORES \
        --executor-memory 2G \
        $PYTHON_JOB $INPUT_DIR $OUTPUT_DIR ; } 2>> $LOG_FILE
        
    echo "---------------------------------" >> $LOG_FILE
    sleep 10
done

# ==========================================
# 3. HORIZONTAL WEAK SCALING (Growing Data)
# ==========================================
echo "--- Running Horizontal Weak Scaling Tests ---"

# Limited to 2 elements: 2 cores (1 worker) and 4 cores (2 workers)
HW_CORES_ARRAY=(2 4)
HW_DATA_ARRAY=("2gig/reddit_2gb.json" "4gig/reddit_4gb.json")

for i in ${!HW_CORES_ARRAY[@]}; do
    CORES=${HW_CORES_ARRAY[$i]}
    DATA_FOLDER=${HW_DATA_ARRAY[$i]}
    
    INPUT_DIR="$HDFS_BASE/reddit-db/$DATA_FOLDER"
    OUTPUT_DIR="$HDFS_BASE/output_h_weak_$CORES"
    
    echo "Testing Horizontal Weak Scaling with $CORES total cores on $DATA_FOLDER..."
    echo "TEST: Horizontal Weak | CORES: $CORES | DATA: $DATA_FOLDER" >> $LOG_FILE
    
    { time spark-submit \
        --master $SPARK_MASTER \
        --executor-cores 2 \
        --total-executor-cores $CORES \
        --executor-memory 2G \
        $PYTHON_JOB $INPUT_DIR $OUTPUT_DIR ; } 2>> $LOG_FILE
        
    echo "---------------------------------" >> $LOG_FILE
    sleep 10
done

# ==========================================
# 4. VERTICAL STRONG SCALING (3 VMs, Fixed Data: 5GB)
# ==========================================
echo "--- Running Vertical Strong Scaling Tests ---"

V_CORES_PER_EXEC=(1 2)
NUM_EXECUTORS=3
FIXED_MEM="2400M"  # Memory stays exactly the same for both tests!

for i in ${!V_CORES_PER_EXEC[@]}; do
    CORES=${V_CORES_PER_EXEC[$i]}
    TOTAL_CORES=$((CORES * NUM_EXECUTORS))
    
    INPUT_DIR="$HDFS_BASE/reddit-db/5gig/reddit_5gb.json"
    OUTPUT_DIR="$HDFS_BASE/output_v_strong_exec_${CORES}"
    
    echo "Testing Vertical Strong Scaling with $NUM_EXECUTORS executors, $CORES cores/exec, $FIXED_MEM RAM..."
    echo "TEST: Vertical Strong | TOTAL CORES: $TOTAL_CORES | CORES/EXEC: $CORES | FIXED MEM/EXEC: $FIXED_MEM | DATA: 5GB" >> $LOG_FILE
    
    { time spark-submit \
        --master $SPARK_MASTER \
        --executor-cores $CORES \
        --total-executor-cores $TOTAL_CORES \
        --executor-memory $FIXED_MEM \
        $PYTHON_JOB $INPUT_DIR $OUTPUT_DIR ; } 2>> $LOG_FILE
        
    echo "---------------------------------" >> $LOG_FILE
    sleep 10
done

# ==========================================
# 5. VERTICAL WEAK SCALING (3 VMs, Growing Data)
# ==========================================
echo "--- Running Vertical Weak Scaling Tests ---"

VW_CORES_PER_EXEC=(1 2)
VW_DATA_ARRAY=("2gig/reddit_2gb.json" "4gig/reddit_4gb.json")
# NUM_EXECUTORS and FIXED_MEM are carried over from above

for i in ${!VW_CORES_PER_EXEC[@]}; do
    CORES=${VW_CORES_PER_EXEC[$i]}
    TOTAL_CORES=$((CORES * NUM_EXECUTORS))
    DATA_FOLDER=${VW_DATA_ARRAY[$i]}
    
    INPUT_DIR="$HDFS_BASE/reddit-db/$DATA_FOLDER"
    OUTPUT_DIR="$HDFS_BASE/output_v_weak_exec_${CORES}"
    
    echo "Testing Vertical Weak Scaling with $NUM_EXECUTORS executors, $CORES cores/exec on $DATA_FOLDER..."
    echo "TEST: Vertical Weak | TOTAL CORES: $TOTAL_CORES | CORES/EXEC: $CORES | FIXED MEM/EXEC: $FIXED_MEM | DATA: $DATA_FOLDER" >> $LOG_FILE
    
    { time spark-submit \
        --master $SPARK_MASTER \
        --executor-cores $CORES \
        --total-executor-cores $TOTAL_CORES \
        --executor-memory $FIXED_MEM \
        $PYTHON_JOB $INPUT_DIR $OUTPUT_DIR ; } 2>> $LOG_FILE
        
    echo "---------------------------------" >> $LOG_FILE
    sleep 10
done

echo "All Vertical benchmarks completed successfully!"
echo "Check $LOG_FILE for results."