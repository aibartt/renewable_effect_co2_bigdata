import org.apache.spark.sql.{SparkSession, DataFrame, functions}
import org.apache.spark.sql.functions._

val developedCountries = List("united states", "canada", "united kingdom", "germany", "france", "japan", 
  "australia", "switzerland", "sweden", "norway", "denmark", "netherlands", "finland", "singapore", "south korea",
  "new zealand", "austria", "belgium", "ireland", "luxembourg", "iceland", "italy", "spain", "portugal", "greece",
  "israel", "united arab emirates", "qatar", "hong kong", "taiwan")


def loadAndProfileDataset(spark: SparkSession, path: String): DataFrame = {
  val df = spark.read
    .option("header", "true")   // Use the first line of the CSV file as the header
    .csv(path)

  //Profiling part (OPTIONAL)

  // Print schema and sample data
  // println(s"Dataset Schema for $path:")
  // df.printSchema()
  // println(s"Sample Data for $path:")
  // df.show(5, truncate = false)

  // // Basic statistical summary
  // println(s"Summary Statistics for $path:")
  // df.describe().show()

  // // Check for null values
  // println(s"Null Values in $path:")
  // df.columns.foreach { col =>
  //   val nullCount = df.filter(df(col).isNull || df(col) === "").count()
  //   println(s"$col: $nullCount null values")
  // }

  // // Check for distinct values
  // println(s"Distinct Values in $path:")
  // df.columns.foreach { col =>
  //   val distinctCount = df.select(col).distinct().count()
  //   println(s"$col: $distinctCount distinct values")
  // }

  df
}

// Function to trim column names
def trimColumnNames(df: DataFrame): DataFrame = {
  df.columns.foldLeft(df)((currentDF, colName) => 
    currentDF.withColumnRenamed(colName, colName.trim)
  )
}

// Integrated cleaning function for the renewable data with unit standardization
def cleanRenewableDataset(df: DataFrame): DataFrame = {
  // Trim column names to remove any leading or trailing whitespace
  val trimmedDF = trimColumnNames(df)

  trimmedDF.select("Years", "IRENA Menu Label", "Technology", "Type", "UN Region English", "Unit", "Values")
    .withColumnRenamed("IRENA Menu Label", "Country")
    .withColumn("Country", lower(col("Country")))
    // Standardize units: Convert MW to GWh if necessary
    .withColumn("Values", 
      when(col("Unit") === "GWh", col("Values"))
      .when(col("Unit") === "MW", col("Values") / lit(1000))
      .otherwise(col("Values")))
    .withColumn("Values", col("Values").cast("double"))
    .na.drop()
    .filter(col("Years").between(2000, 2023))
}


def cleanEmissionDataset(df: DataFrame, countryFilter: Option[List[String]] = None): DataFrame = {
  // Remove rows with null values in any column
  val cleanedDF = df.na.drop().filter(col("Year") >= 2000 && col("Year") <= 2023)
                  .withColumn("Country", lower(col("Country")))
                  //.filter(col("Country").isin(countryFilter: _*))
  cleanedDF
}

def cleanFinanceDataset(df: DataFrame, countryFilter: Option[List[String]] = None, technologyFilter: Option[String] = None): DataFrame = {
  // Remove rows with null values in any column
  val cleanedDF = df.na.drop().filter(col("Year") >= 2000 && col("Year") <= 2023)

  // Convert age column to integer and replace null values with a default value (e.g., 0)
  val outputDF = cleanedDF
    .withColumn("Year", col("Year").cast("int"))
    .na.fill(0, Seq("Year"))
    .withColumn("Country", lower(col("Country/Area")))
    .withColumn("TotalFinanceFlow", col("`Amount (2020 USD million)`").cast("double"))

  // Apply filters only if they are not None
  val filteredDF = (countryFilter, technologyFilter) match {
    case (Some(countries), Some(tech)) =>
      outputDF.filter(col("Country").isin(countries: _*) && col("Technology") === tech)
    case (Some(countries), None) =>
      outputDF.filter(col("Country").isin(countries: _*))
    case (None, Some(tech)) =>
      outputDF.filter(col("Technology") === tech)
    case (None, None) =>
      outputDF
  }

  filteredDF
}

def calculateCorrelation(df: DataFrame, col1: String, col2: String): Double = {
    // Use the corr method to calculate the correlation coefficient
    df.select(corr(col1, col2)).as[Double].first()
  }


// Create a Spark session
val spark = SparkSession.builder
  .appName("Big Data Project")
  .getOrCreate()

// Load, profile, and clean the first dataset
val dataset1Path = "project/fossil_co2.csv"
val df1 = loadAndProfileDataset(spark, dataset1Path)
val clean_df1 = cleanEmissionDataset(df1)

// Load, profile, and clean the second dataset
val dataset2Path = "project/investment_data.csv"
val df2 = loadAndProfileDataset(spark, dataset2Path)
val clean_df2 = cleanFinanceDataset(df2)

val summedData = clean_df2.groupBy("Country", "Year")
  .agg(functions.sum("TotalFinanceFlow").alias("TotalAmount"))

summedData.describe().show()

// Register the DataFrames as a temporary SQL tables
clean_df1.createOrReplaceTempView("emissions")
summedData.createOrReplaceTempView("investments")

val query = """SELECT i.country as Country, i.year as investment_year, e.year as emissions_year, i.TotalAmount as investment_amount, e.Total as total_emission
FROM investments as i
JOIN emissions as e ON i.country = e.country 
"""

for (i <- 0 to 5) {
  val corrQuery = s"$query AND i.year = e.year - $i;"
  val correlationTable: DataFrame = spark.sql(corrQuery)

  // Show the results
  correlationTable.describe().show()

  val column1 = "investment_amount"
  val column2 = "total_emission"

  // Calculate the correlation coefficient between the two columns
  val correlationCoefficient = calculateCorrelation(correlationTable, column1, column2)

  // Print the result
  println(s"Correlation Coefficient between $column1 and $column2: $correlationCoefficient")
}

//val data_coalesced = correlationTable.coalesce(1)

// Write the results to a separate table
//data_coalesced.write.csv("project/corr_finance_co2_developed")

// Stop the Spark session
spark.stop()
