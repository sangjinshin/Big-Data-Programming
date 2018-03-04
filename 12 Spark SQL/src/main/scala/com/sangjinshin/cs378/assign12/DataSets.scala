package com.sangjinshin.cs378.assign12

import org.apache.spark.sql.SparkSession

/**
 * Spark DataSets
 *
 * Input a CSV file into a DataSet and compute statistics using Spark SQL
 *
 * @author Sangjin Shin
 * UTEID: ss62273
 * Email: sangjinshin@outlook.com
 */
object DataSets {

  def main(args: Array[String]) {

    /* Input and output path */
    val inputDir = args(0)
    val outputDir = args(1)

    /* Create a SparkSession */
    val spark = SparkSession
      .builder()
      .appName("DataSets")
      .config("spark.master", "local")
      .getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    // DataFrame to load a CSV file
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(inputDir)

    // Register the DataFrame as a temporary view
    df.createOrReplaceTempView("session_table")

    // Compute statistics
    runPriceStatistics(spark, outputDir)
    runMileageStatistics(spark, outputDir)
    runEventStatistics(spark, outputDir)

  }

  /**
   * By make/model, the min, max, and average price (excluding VINs with price = 0)
   * - Order the output CSV by make, then model
   */
  private def runPriceStatistics(spark: SparkSession, outputDir: String): Unit = {

    val tempDF = spark.sql("SELECT DISTINCT make, model, price FROM session_table WHERE CAST(price as DOUBLE) != 0")
    tempDF.repartition(1).write.format("csv").save(outputDir + "/temp/price")

    val df = spark.read
      .option("inferSchema", "true")
      .csv(outputDir + "/temp/price")

    df.createOrReplaceTempView("price_table")

    val priceDF = spark.sql("SELECT _c0, _c1, MIN(CAST(_c2 as DOUBLE)), MAX(CAST(_c2 as DOUBLE)), AVG(CAST(_c2 as DOUBLE)) FROM price_table GROUP BY _c0, _c1 ORDER BY _c0, _c1")
    priceDF.repartition(1).write.format("csv").save(outputDir + "/price")

  }

  /**
   * By year, the min, max, and average mileage (excluding VINs with mileage - 0)
   * - Order the output CSV by year
   */
  private def runMileageStatistics(spark: SparkSession, outputDir: String): Unit = {

    val tempDF = spark.sql("SELECT DISTINCT year, mileage FROM session_table WHERE CAST(mileage as INT) != 0")
    tempDF.repartition(1).write.format("csv").save(outputDir + "/temp/mileage")

    val df = spark.read
      .option("inferSchema", "true")
      .csv(outputDir + "/temp/mileage")

    df.createOrReplaceTempView("mileage_table")

    val mileageDF = spark.sql("SELECT _c0, MIN(CAST(_c1 as INT)), MAX(CAST(_c1 as INT)), AVG(CAST(_c1 as INT)) FROM mileage_table GROUP BY _c0 ORDER BY _c0")
    mileageDF.repartition(1).write.format("csv").save(outputDir + "/mileage")

  }

  /**
   * By VIN, the total for each event type/action (split out the event type/action from the event)
   * - Order the output CSV by VIN, then event type/action
   */
  private def runEventStatistics(spark: SparkSession, outputDir: String): Unit = {

    val tempDF = spark.sql("SELECT vin, SUBSTRING_INDEX(event, ' ', 1) FROM session_table")
    tempDF.repartition(1).write.format("csv").save(outputDir + "/temp/event")

    val df = spark.read
      .option("inferSchema", "true")
      .csv(outputDir + "/temp/event")

    df.createOrReplaceTempView("event_table")

    val eventDF = spark.sql("SELECT _c0, _c1, count(*) as occurrences FROM event_table GROUP BY _c0, _c1 ORDER BY _c0, _c1")
    eventDF.repartition(1).write.format("csv").save(outputDir + "/event")

  }
}