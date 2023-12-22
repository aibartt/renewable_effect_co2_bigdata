// Import necessary Spark SQL libraries and functions
import org.apache.spark.sql.{SparkSession, DataFrame, functions}
import org.apache.spark.sql.functions._

// Function to load CO2 emissions dataset into a DataFrame using SparkSession
def loadCO2Dataset(spark: SparkSession, path: String): DataFrame = {
  // Read the dataset from the specified path in CSV format
  val data = spark.read
    .option("header", "true")   // Use the first line of the CSV file as the header
    .csv(path)

  // Return the loaded DataFrame
  data
}

// Function to clean CO2 emissions data
def cleanCO2Dataset(data: DataFrame): DataFrame = {
  // Mapping table for country names
  val countryMapping = Map(
    "france (including monaco)" -> "france",
    "occupied palestinian territory" -> "state of palestine",
    "st. kitts and nevis" -> "saint kitts and nevis",
    "st. vincent & the grenadines" -> "saint vincent and the grenadines",
    "sao tome & principe" -> "sao tome and principe",
    "plurinational state of bolivia" -> "bolivia",
    "democratic people s republic of korea" -> "democratic people's republic of korea",
    "timor-leste (formerly east timor)" -> "timor-leste",
    "myanmar (formerly burma)" -> "myanmar",
    "democratic republic of the congo (formerly zaire)" -> "democratic republic of the congo",
    "republic of cameroon" -> "cameroon",
    "lao people s democratic republic" -> "lao people's democratic republic",
    "czech republic (czechia)" -> "czech republic",
    "islamic republic of iran" -> "iran",
    "italy (including san marino)" -> "italy",
    "bosnia & herzegovina" -> "bosnia and herzegovina",
    "timor-leste (formerly east timor)" -> "timor-leste",
    "libyan arab jamahiriyah" -> "libya",
    "antigua & barbuda" -> "antigua and barbuda"
  )

  // Data cleaning - filter the years to retain only those between 2000 and 2023
  val data_filtered = data.filter(col("Year") >= 2000 && col("Year") <= 2023)
  println("Data is filtered based on years (2000 - 2023)")

  // Select only the desired columns: Year, Country, Total, Per Capita and drop rows with null values
  val data_selected = data_filtered.select("Year", "Country", "Total", "Per Capita").na.drop()
  println("Necessary columns are selected, null values are dropped")

  // Update country names based on the mapping
  val mapCountryName = udf((country: String) => countryMapping.getOrElse(country.toLowerCase, country))
  val updatedData = data_selected.withColumn("Country", mapCountryName(lower(col("Country"))))
  println("Country names are standardized")

  // Reduce the data to a single partition for output as a single file
  val data_coalesced = updatedData.coalesce(1)

  data_coalesced.show(false)

  // Count the number of records after cleaning and display the count
  val count_data_coalesced = data_coalesced.count()
  println(s"Number of records after the cleaning process: $count_data_coalesced")

  // Write the cleaned data to HDFS in a specified path
  data_coalesced.write.mode("overwrite").csv("hdfs://nyu-dataproc-m/user/ak8827_nyu_edu/project/clean_data")

  // Return the cleaned DataFrame
  data_coalesced
}

// Create a SparkSession with the given application name ("Data Cleaning")
val spark = SparkSession.builder()
  .appName("Data Cleaning")
  .getOrCreate()

// Define the path to the CO2 emissions dataset stored in HDFS
val datasetPath = "hdfs://nyu-dataproc-m/user/ak8827_nyu_edu/project/fossil_co2.csv" 

// Load the CO2 dataset using 'loadCO2Dataset' function and SparkSession
val data = loadCO2Dataset(spark, datasetPath)

// Clean the loaded data using 'cleanCO2Data' function
val clean_data = cleanCO2Dataset(data)

// Stop the SparkSession to release resources after data processing is complete
spark.stop()
