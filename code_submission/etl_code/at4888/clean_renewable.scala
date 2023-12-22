import org.apache.spark.sql.{SparkSession, DataFrame, functions}
import org.apache.spark.sql.functions._

// Data loading. Create DataFrame from CSV file
def loadDataset(spark: SparkSession, path: String): DataFrame = {
  val df = spark.read
    .option("header", "true")   // Use the first line of the CSV file as the header
    .csv(path)
    .withColumnRenamed("Avg. HH size", "AvgHHSize") // Because of special character in the column name we have to rename it to avoid error
  df
}

// Function to trim column names
def trimColumnNames(df: DataFrame): DataFrame = {
  df.columns.foldLeft(df)((currentDF, colName) => 
    currentDF.withColumnRenamed(colName, colName.trim)
  )
}


// Integrated cleaning function for the Renewable Energy generation data with unit standardization
def cleanRenewableDataset(df: DataFrame): DataFrame = {
  // Trim column names to remove any leading or trailing whitespace
  val trimmedDF = trimColumnNames(df)

  val filteredDF = trimmedDF.select("Years", "IRENA Menu Label", "Technology", "Type", "UN Region English", "Unit", "Values")  
    //Using universal naming of columns across datasets
    .withColumnRenamed("IRENA Menu Label", "Country")
    .withColumn("Country", lower(col("Country")))
    // Standardize units: Convert MW to GWh if necessary because we cannot take the "Values" column record without standardization
    .withColumn("Values", 
      when(col("Unit") === "GWh", col("Values"))
      // if the unit is MW it would be divided by 1000 to get GWh value
      .when(col("Unit") === "MW", col("Values") / lit(1000))
      .otherwise(col("Values")))
    .withColumn("Values", col("Values").cast("double"))
    .na.drop()
    .filter(col("Years").between(2006, 2017))

    // Mapping table for country names
  val countryMapping = Map(
    "bolivia (plurinational state of)" -> "bolivia",
    "cote d'ivoire" -> "cote d ivoire",
    "czechia" -> "czech republic",
    "iran (islamic republic of)" -> "iran",
    "north macedonia" -> "macedonia",
    "venezuela (bolivarian republic of)" -> "venezuela"
  )

  // Update country names based on the mapping
  val mapCountryName = udf((country: String) => countryMapping.getOrElse(country.toLowerCase, country))
  val updatedData = filteredDF.withColumn("Country", mapCountryName(lower(col("Country"))))

  updatedData
}

val spark = SparkSession.builder
  .appName("Project Cleaning")
  .getOrCreate()

// Load, profile, and clean the renewable energy generation dataset
val datasetPath = "project/renewable_data.csv"
val df = loadDataset(spark, datasetPath)
val clean_df = cleanRenewableDataset(df)

clean_df.show(100)

//OPTIONAL: Write cleaned dataset into a CSV file and store it in HDFS
//val data_coalesced = clean_df.coalesce(1)

// Write the results to a separate table
//data_coalesced.write.csv("project/clean_renewable")

spark.stop()