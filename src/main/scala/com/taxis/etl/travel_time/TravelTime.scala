package com.taxis.etl.travel_time

import com.taxis.etl.helpers.DataFrameUtils
import com.taxis.etl.utils.Spark
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.col

object TravelTime {

  def main(args: Array[String]): Unit = {
    Spark.init("myApp")
    getDataframes()
  }

  def getDataframes() = {
    val newDF = DataFrameUtils.getDataframeFromParquet("resources/yellow_tripdata_2020-01.parquet")
    val result = newDF.select(col("Trip_distance"))
    result.write.option("delimiter","\t").option("header","true").csv("results/csv")
    result.write.format("parquet").mode(SaveMode.Overwrite).save("results/parquet")
  }
}