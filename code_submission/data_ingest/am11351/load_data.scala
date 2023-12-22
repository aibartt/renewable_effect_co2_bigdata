import org.apache.spark.sql.{SparkSession, DataFrame, functions}
import org.apache.spark.sql.functions._

// Data loading. Create DataFrame from CSV file
def loadDataset(spark: SparkSession, path: String): DataFrame = {
  val df = spark.read
    .option("header", "true")   // Use the first line of the CSV file as the header
    .csv(path)
  df
}

val spark = SparkSession.builder
  .appName("Data Loading")
  .getOrCreate()

// Load the RE investments
val datasetPath = "project/investment_data.csv"

val df = loadDataset(spark, datasetPath)

df.show()

spark.stop()