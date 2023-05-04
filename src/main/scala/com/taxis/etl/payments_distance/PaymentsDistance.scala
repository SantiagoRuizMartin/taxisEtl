package com.taxis.etl.payments_distance

import com.taxis.etl.extract.ReadFromEnv
import com.taxis.etl.helpers.DataFrameUtils
import com.taxis.etl.utils.Spark
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions.col

object PaymentsDistance {

  def main(args: Array[String]): Unit = {
    Spark.init("myApp")
    getDataframes()
    //val rates = calculateRate(dataframe)
  }

  def getDataframes(): DataFrame = {
    // Get data from three files
    val parquets = ReadFromEnv.readList()
    val df_concat = parquets.map(DataFrameUtils.getDataframeFromParquet _)
      // Filter by the desired columns
      .map((x: DataFrame) => x.select("Trip_distance", "PULocationID", "DOLocationID", "Tolls_amount", "Total_amount"))
      // Concatenating the dataframes
      .reduce((x: DataFrame, y: DataFrame) => x.union(y))
    // Write result in two different files
    df_concat.write.option("delimiter",",").option("header","true").mode(SaveMode.Overwrite).csv("results/csv")
    df_concat.write.format("parquet").mode(SaveMode.Overwrite).save("results/parquet")
    df_concat
  }

  def calculateRate(dataFrame: DataFrame): Boolean = {
    return false
  }

  /*
  PULocationID
  DOLocationID
  Trip_distance
  Tolls_amount
  Total_amount
  */
}