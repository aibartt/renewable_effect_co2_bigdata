import org.apache.spark.sql.{SparkSession, DataFrame, functions}
import org.apache.spark.sql.functions._

val developedCountries = List("united states", "canada", "united kingdom", "germany", "france", "japan", 
  "australia", "switzerland", "sweden", "norway", "denmark", "netherlands", "finland", "singapore", "south korea",
  "new zealand", "austria", "belgium", "ireland", "luxembourg", "iceland", "italy", "spain", "portugal", "greece",
  "israel", "united arab emirates", "china")

val countryMapping = Map(
    "bolivia (plurinational state of)" -> "bolivia",
    "côte d'ivoire" -> "cote d ivoire",
    "czechia" -> "czech republic",
    "iran (islamic republic of)" -> "iran",
    "united kingdom of great britain and northern ireland" -> "united kingdom",
    "venezuela (bolivarian republic of)" -> "venezuela",
    "türkiye" -> "turkey",
    "micronesia (federated states of)" -> "federated states of micronesia",
    "guinea-bissau" -> "guinea bissau"
  )


// Data loading. Create DataFrame from CSV file
def loadDataset(spark: SparkSession, path: String): DataFrame = {
  val df = spark.read
    .option("header", "true")   // Use the first line of the CSV file as the header
    .csv(path)
  df
}

//Renewable energy finance flow dataset cleaning with optional filters (Country and Technology)
def cleanFinanceDataset(df: DataFrame, countryFilter: Option[List[String]] = None, technologyFilter: Option[String] = None): DataFrame = {
  // Remove rows with null values in any column
  val cleanedDF = df.na.drop().filter(col("Year") >= 2000 && col("Year") <= 2023)

  // Convert age column to integer and replace null values with a default value (e.g., 0)
  val outputDF = cleanedDF.withColumn("TotalFinanceFlow", col("`Amount (2020 USD million)`").cast("double"))

  // Update country names based on the mapping
  val mapCountryName = udf((country: String) => countryMapping.getOrElse(country.toLowerCase, country))
  val updatedData = outputDF.withColumn("Country", mapCountryName(lower(col("Country/Area"))))

  // Apply filters only if they are not None
  val filteredDF = (countryFilter, technologyFilter) match {
    case (Some(countries), Some(tech)) =>
      updatedData.filter(col("Country").isin(countries: _*) && col("Technology") === tech)
    case (Some(countries), None) =>
      updatedData.filter(col("Country").isin(countries: _*))
    case (None, Some(tech)) =>
      updatedData.filter(col("Technology") === tech)
    case (None, None) =>
      updatedData
  }

  filteredDF
}

val spark = SparkSession.builder
  .appName("Project Cleaning")
  .getOrCreate()

// Load, profile, and clean the second dataset
val datasetPath = "project/investment_data.csv"
val df = loadDataset(spark, datasetPath)
val clean_df = cleanFinanceDataset(df)
val clean_df_developed_countries = cleanFinanceDataset(df, developedCountries)

clean_df.show()

clean_df_developed_countries.show()

//OPTIONAL: Write cleaned dataset into a CSV file and store it in HDFS
//val data_coalesced = clean_df.coalesce(1)

// Write the results to a separate table
//data_coalesced.write.csv("project/clean_finance")

spark.stop()