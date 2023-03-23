package com.taxis.etl.travel_time

import com.taxis.etl.helpers.DataFrameUtils
import com.taxis.etl.utils.Spark
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions.{col, unix_timestamp}

object TravelTime {

  def main(args: Array[String]): Unit = {
    Spark.init("myApp")

    //    Extract
    val mainDf = getMainDataframe
    //    Transform
    val df1 = transformMilesToKms(mainDf)
    val df3 = getTripTime(df1)
    val finalDf = getTripInformation(df3)
    //    Load
    loadDfResult(finalDf)
  }

  //  Extract *-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*--*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*
  def getMainDataframe: DataFrame = {
    val df1 = DataFrameUtils.getDataframeFromParquet("resources/yellow_tripdata_2020-01.parquet")
    val df2 = DataFrameUtils.getDataframeFromParquet("resources/yellow_tripdata_2021-01.parquet")
    val df3 = DataFrameUtils.getDataframeFromParquet("resources/yellow_tripdata_2022-01.parquet")

    df1.union(df2).union(df3)
  }

  // Transform *-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*--*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-
  def transformMilesToKms(df: DataFrame): DataFrame = {
    df.withColumn("trip_Distance_kms", col("trip_distance") * 1.6)
  }

  def getTripTime(df: DataFrame): DataFrame = {
    df.withColumn("travel_time",
      (unix_timestamp(col("tpep_dropoff_datetime")) - unix_timestamp(col("tpep_pickup_datetime"))) / 60)
  }

  def getTripInformation(df: DataFrame): DataFrame = {
    df.select("VendorID", "passenger_count", "trip_Distance_kms", "travel_time", "payment_type")
  }

  //  Load -*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*--*-*-*-*-*-*-*-*-*-*-*-*-*-*-*
  def loadDfResult(dataFrame: DataFrame): Unit = {
    dataFrame.write.option("delimiter", "\t").option("header", "true").mode(SaveMode.Overwrite).csv("results/csv")
    dataFrame.write.format("parquet").mode(SaveMode.Overwrite).save("results/parquet")
  }

}