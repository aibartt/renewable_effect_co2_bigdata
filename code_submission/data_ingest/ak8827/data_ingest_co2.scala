// Import necessary Spark SQL libraries and functions
import org.apache.spark.sql.{SparkSession, DataFrame, functions}
import org.apache.spark.sql.functions._

// Define a function to load a dataset using SparkSession and return a DataFrame
def loadCO2Dataset(spark: SparkSession, path: String): DataFrame = {
  // Read the dataset from the specified path in CSV format
  val data = spark.read
    .option("header", "true")   // Use the first line of the CSV file as the header
    .csv(path)

  // Print the schema of the loaded dataset
  data.printSchema()

  // Return the loaded DataFrame
  data
}

// Create a SparkSession with the given application name ("Data Ingesting")
val spark = SparkSession.builder()
  .appName("Data Ingesting")
  .getOrCreate()

// Define the path to the CO2 emissions dataset stored in HDFS
val datasetPath = "hdfs://nyu-dataproc-m/user/ak8827_nyu_edu/project/fossil_co2.csv" 

// Load the dataset by calling the 'loadDataset' function and passing the SparkSession and dataset path
val data = loadCO2Dataset(spark, datasetPath)

// Stop the SparkSession to release resources after data processing is complete
spark.stop()