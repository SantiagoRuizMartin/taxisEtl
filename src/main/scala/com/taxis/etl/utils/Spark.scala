package com.taxis.etl.utils

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object Spark {

  private var _spark: Option[SparkSession] = None


  private val SparkContextNotInitialized = "SparkContext not initialized!"

  def spark : SparkSession = {

    if (_spark.isEmpty) {
      throw new RuntimeException(SparkContextNotInitialized)
    }
    _spark.get
  }

  def sc : SparkContext = {

    if (_spark.isEmpty) {
      throw new RuntimeException(SparkContextNotInitialized)
    }
    _spark.get.sparkContext
  }

  def sql : SQLContext = {

    if (_spark.isEmpty) {
      throw new RuntimeException(SparkContextNotInitialized)
    }
    _spark.get.sqlContext
  }

  def init(sparkConf: SparkConf): Unit = {

    if (_spark.isDefined) {
      _spark
    }
    _spark = Option(SparkSession.builder().config(sparkConf).getOrCreate())
  }

  def init(appName : String): Unit = {
    init(new SparkConf().setAppName(appName))
  }

  def shutdown(): Unit = {
    spark.stop()
    _spark = None
  }
}
