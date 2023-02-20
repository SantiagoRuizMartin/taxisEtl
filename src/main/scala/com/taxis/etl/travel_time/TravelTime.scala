package com.taxis.etl.travel_time

import com.taxis.etl.helpers.DataFrameUtils
import org.apache.spark.sql.functions.col

object TravelTime {

  def main(args: Array[String]): Unit = {
    getDataframes()
  }

  def getDataframes() = {
    val newDF = DataFrameUtils.getDataframeFromParquet("src/main/resources/yellow_tripdata_2020-01.parquet")
    val result = newDF.select(col("Trip_distance"))
    result.write.csv("src/main/resources/results/result1.csv")
  }
}