# **Ethereum Transactions and Smart Contract Analysis**

## **📌 Project Overview**
This project explores **Ethereum blockchain data** using **Big Data Processing techniques** like **MapReduce and Apache Spark**. The analysis includes **transaction patterns, popular smart contracts, active miners, scam detection, and gas price trends.**

---

## **🚀 Key Analyses**

### **1️⃣ Time-Based Transaction Analysis (Part A - 20%)**
- **Objective:** Analyze Ethereum transactions over time.
- **Approach:**
  - Extract timestamps from transactions.
  - Convert timestamps into Year-Month format.
  - Count transactions per month.
  - Compute the average transaction value per month.
- **Tools:** `mrjob`, Hadoop, Matplotlib (for visualization).
- **Results:** Transaction trends plotted over time.

---

### **2️⃣ Top 10 Most Popular Smart Contracts (Part B - 25%)**
- **Objective:** Identify top 10 most-used smart contracts.
- **Approach:**
  - Extract receiver addresses from transaction dataset.
  - Aggregate values for each address.
  - Filter addresses that belong to smart contracts.
  - Sort and select the top 10 contracts.
- **Tools:** `mrjob` with multiple MapReduce jobs.
- **Results:** A list of top 10 smart contracts based on Ether volume.

---

### **3️⃣ Top 10 Most Active Miners (Part C - 15%)**
- **Objective:** Identify top miners based on block size contribution.
- **Approach:**
  - Extract miner addresses and block sizes from block dataset.
  - Sum up block sizes per miner.
  - Sort and select the top 10 miners.
- **Tools:** `mrjob` with multiple MapReduce jobs.
- **Results:** A ranked list of top 10 miners with total block size contribution.

---

### **4️⃣ Scam Analysis (Part D - 40%)**
- **Objective:** Analyze Ethereum scams and their impact.
- **Approach:**
  - Extract scam-related addresses from `scams.json` dataset.
  - Correlate with transaction dataset to compute volume per scam type.
  - Identify the most lucrative scam types and their statuses.
- **Tools:** `mrjob`, Hadoop.
- **Results:**
  - **Most Lucrative Scam Type:** Scamming.
  - **Highest Impact Scam Status:** Active scams affected 88,444 addresses.

---

### **5️⃣ Fork Analysis**
- **Objective:** Identify Ethereum blockchain forks.
- **Approach:**
  - Extract miner information and block numbers.
  - Detect blocks with multiple miners (indicating a fork).
  - Identify addresses that profited from the fork.
- **Tools:** `mrjob`, Hadoop.
- **Results:**
  - Fork detected in **December 2017**.
  - Address **0x0000000000b3f879cb30fe243b4dfee438691c04** profited the most.

---

### **6️⃣ Gas Price Trends (Gas Guzzlers Analysis)**
- **Objective:** Analyze changes in Ethereum gas prices over time.
- **Approach:**
  - Extract gas price and timestamp.
  - Compute average gas price per month.
  - Visualize price fluctuations.
- **Tools:** `mrjob`, Hadoop, Matplotlib.
- **Results:**
  - Gas prices spiked in early months of each year.
  - Prices dropped after **December 2017 fork**.

---

## **💡 Comparative Evaluation: MapReduce vs. Spark**
- **MapReduce Approach:** Used `mrjob` for multi-step processing.
- **Spark Approach:** Used `pyspark` for in-memory distributed processing.
- **Findings:**
  - **Spark is 3-5x faster** than Hadoop MapReduce.
  - **Spark requires less code** (~15 lines vs. 50+ lines in MapReduce).
  - **MapReduce writes intermediate results to HDFS**, making it slower.

---

## **📜 Installation & Setup**
### **🔹 Prerequisites**
- **Python 3.8+**
- **Hadoop (for MapReduce jobs)**
- **Apache Spark (for comparative evaluation)**
- **Required Python libraries:**
  ```sh
  pip install mrjob pyspark numpy pandas matplotlib seaborn
  ```


### **🔹 Run MapReduce Jobs**
```sh
python time_analysis.py -r hadoop input_data.csv
python scam_analysis.py -r hadoop input_data.csv
```

### **🔹 Run Spark Job**
```sh
spark-submit spark_analysis.py
```


---

## **📞 Contact**
👩‍💻 **Anubha Gupta**  
📧 Email: [ganubha30@gmail.com](mailto:ganubha30@gmail.com)  
🔗 GitHub: [AnubhaGupta-15](https://github.com/AnubhaGupta-15)  
