package com.taxis.etl.payments_distance

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
    val newDF2020 = DataFrameUtils.getDataframeFromParquet("resources/yellow_tripdata_2020-01.parquet")
    val newDF2021 = DataFrameUtils.getDataframeFromParquet("resources/yellow_tripdata_2021-01.parquet")
    val newDF2022 = DataFrameUtils.getDataframeFromParquet("resources/yellow_tripdata_2022-01.parquet")

    // Filter by the desired columns
    val result2020 = newDF2020.select("Trip_distance", "PULocationID", "DOLocationID", "Tolls_amount", "Total_amount")
    val result2021 = newDF2021.select("Trip_distance", "PULocationID", "DOLocationID", "Tolls_amount", "Total_amount")
    val result2022 = newDF2022.select("Trip_distance", "PULocationID", "DOLocationID", "Tolls_amount", "Total_amount")

    // Concatenating the dataframes
    val temp_df_concat = result2020.union(result2021)
    val df_concat = temp_df_concat.union(result2022)

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