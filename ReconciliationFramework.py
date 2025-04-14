import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit
from pyspark.sql.functions import col, monotonically_increasing_id, rand, concat_ws, lit
import random
import os
import glob
import shutil


# Initialize Spark session
spark = SparkSession.builder \
    .appName("") \
    .getOrCreate()

class ReconciliationFramework:
    def __init__(self, file1, file2, key_column):
        """Initialize reconciliation framework with two datasets and a matching column."""
        self.file1 = file1
        self.file2 = file2
        self.key_column = key_column
        self.df1 = None
        self.df2 = None
        self.mismatches = None

    def load_data(self):
        """Load datasets into Spark DataFrames."""
        try:
            self.df1 = spark.read.option("header", True).csv(self.file1)
            self.df2 = spark.read.option("header", True).csv(self.file2)
            print("Data loaded successfully!")
        except Exception as e:
            print(f"Error loading data: {e}")
            raise

    def preprocess(self):
        """Standardize column names and handle missing values."""
        self.df1 = self.df1.withColumnRenamed(self.key_column, self.key_column.lower())
        self.df2 = self.df2.withColumnRenamed(self.key_column, self.key_column.lower())

        # Replace nulls with empty strings for easier comparison
        self.df1 = self.df1.fillna("")
        self.df2 = self.df2.fillna("")
    
    def reconcile(self):
        """Perform reconciliation by finding matches and mismatches."""
        join_condition = self.df1[self.key_column.lower()] == self.df2[self.key_column.lower()]
        
        # Full outer join to capture all discrepancies
        reconciled_df = self.df1.join(self.df2, join_condition, "outer") \
            .select(
                self.df1[self.key_column.lower()].alias("source_key"),
                self.df2[self.key_column.lower()].alias("target_key"),
                when(self.df1[self.key_column.lower()].isNull(), "Missing in Source")
                .when(self.df2[self.key_column.lower()].isNull(), "Missing in Target")
                .otherwise("Matched")
                .alias("Status")
            )

        self.mismatches = reconciled_df.filter(col("Status") != "Matched")

    def compare_row_counts(self):
        source_count = self.df1.count()
        target_count = self.df2.count()
        print(f"Source Row Count: {source_count}")
        print(f"Target Row Count: {target_count}")

        # Compare the counts
        if source_count == target_count:
            print("Row counts match!")
        else:
            print(f"Row counts do not match! Source: {source_count}, Target: {target_count}")
    
    
    def export_results(self, output_file="reconciliation_results.csv"):
        """Export mismatches to a CSV file."""
        if self.mismatches:
            self.mismatches.write.mode("overwrite").csv(output_file, header=True)
            print(f"Results saved to {output_file}")
        else:
            print("No mismatches found!")

    def generateSourceCsv(self):
        num_records= 10
        names = ["Alice", "Bob", "Charlie", "David", "Emma", "Frank", "Grace", "Hannah", "Isaac", "Jack"]

        # Create a DataFrame with sequential account IDs
        df = spark.range(1, num_records + 1).toDF("account_id") \
                .withColumn("name", concat_ws("", lit(random.choice(names)), col("account_id") % 10)) \
                .withColumn("balance", (rand() * 5000).cast("decimal(10,2)"))  # Random balance between 0-5000

        # Step 3: Save DataFrame as a CSV file
        df.coalesce(1).write.mode("overwrite").option("header", True).csv("source_data")

        print("✅ Source CSV created!")

        # Find the generated part file
        csv_file = glob.glob("source_data/part-00000-*.csv")[0]

        # Rename it to "target.csv"
        os.rename(csv_file, "source.csv")

        # Step 7: Remove the temporary folder
        shutil.rmtree("source_data")  

    def generateTargetCsv(self):
        print("sva")
        source_df = spark.read.option("header", True).csv("source.csv")

        # Step 3: Modify Data (Introduce Small Changes)
        # Remove 5% of records
        target_df = source_df.sample(fraction = 0.80)#, seed = 42)
        
        # Step 4: Modify balance for only 2 random rows
        sample_rows = target_df.limit(2).select("account_id").rdd.flatMap(lambda x: x).collect()  # Select 2 random accounts

        # Update balance only for these 2 accounts
        target_df = target_df.withColumn(
            "balance",
            when(col("account_id").isin(sample_rows), (col("balance").cast("double") * (1 + (rand() - 0.5) * 0.1)).cast("decimal(10,2)"))
            .otherwise(col("balance"))
        )

        # Step 4: Save as a Single CSV File
        target_df.coalesce(1).write.mode("overwrite").option("header", True).csv("target_data")

        print("✅ Target CSV created with minor changes!")

        # Find the generated part file
        csv_file = glob.glob("target_data/part-00000-*.csv")[0]

        # Rename it to "target.csv"
        os.rename(csv_file, "target.csv")

        # Step 7: Remove the temporary folder
        shutil.rmtree("target_data")  

    def writeCsvToS3(self):
        df = self.df2
        hadoop_conf = spark._jsc.hadoopConfiguration().set("spark.jars.packages", 
         "org.apache.hadoop:hadoop-aws:3.2.0")
        hadoop_conf.set("fs.s3a.access.key", "ASIASKRH5RYABM6CFHL3")
        hadoop_conf.set("fs.s3a.secret.key", "Ie3PNcFGulOAwOxLUqdDSzH4N+THxzfc/8WWHYeW")
        hadoop_conf.set("fs.s3a.endpoint", "s3.amazonaws.com")

        s3_output_path = "s3a://svabeachbucket"
        df.write \
          .option("header", "true") \
          .csv(s3_output_path)



    def run(self):
            """Execute the reconciliation pipeline."""
            self.generateSourceCsv()
            self.generateTargetCsv()
            self.load_data()
            self.preprocess()
            self.reconcile()
            self.compare_row_counts()
            self.export_results()
            #   self.writeCsvToS3()
            print("Reconciliation process completed!")


# Example usage
if __name__ == "__main__":
    reconciler = ReconciliationFramework("source.csv", "target.csv", key_column="account_id")
    reconciler.run()



# Read parquet, create a view out of that. 
# Compare data and list out the differences