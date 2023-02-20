package com.taxis.etl.helpers

import com.taxis.etl.utils.Spark.spark
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import scala.util.{Failure, Success, Try}

object DataFrameUtils {
  val log: Logger = Logger.getLogger("ETL_logger")

  def filterByColumnAndPrefixEndsWith(df: DataFrame, columnName: String, endPrefix: String): Try[Array[Row]] = {
    Try(df.filter(col(columnName) endsWith endPrefix).collect())
  }

  def filterByColumnAndPrefixStartsWith(df: DataFrame, columnName: String, startPrefix: String): Try[Array[Row]] = {
    Try(df.filter(col(columnName) startsWith startPrefix).collect())
  }

  def getEmptyList(df: DataFrame, columnName: String): Try[Dataset[Row]] = {
    Try(df.select(columnName).filter(row => row.getString(0) == "" || row.getString(0) == null))
  }

  def getColumnValues(df: DataFrame, columnName: String): Try[Array[Row]] = {
    Try(df.select(columnName).collect())
  }

  def getFilteredByColumnAndExpression(df: DataFrame, columnName: String, regularExpression: String): Try[Array[Row]] = {
    Try(df.select(columnName).filter(df.col(columnName) rlike regularExpression).collect())
  }

  def getColumnsFromDataFrame(df: DataFrame, columns: Seq[String]): Try[Array[Row]] = {
    Try(df.select(columns.map(name => col(name)): _*).collect())
  }

  def tryToGetDataframeFromParquet(path: String): Try[DataFrame] = {
    Try(spark.read.parquet(path))
  }

  def tryToGetDataframeFromJson(path: String): Try[DataFrame] = {
    Try(spark.read.json(path))
  }

  def tryToGetDataFrameFromTsv(path: String): Try[DataFrame] = {
    Try(spark.read.option("delimiter", "\t").option("header", "true").csv(path))
  }

  def getDataframeFromParquet(path: String): DataFrame = {
    tryToGetDataframeFromParquet(path) match {
      case Success(dataFrame) =>
        dataFrame
      case Failure(exception) =>
        log.error(s"There was an error trying to get the Data Frame, please see: \n $exception")
        spark.emptyDataFrame
    }
  }

  def getDataframeFromJson(path: String): DataFrame = {
    tryToGetDataframeFromJson(path) match {
      case Success(dataFrame) =>
        dataFrame
      case Failure(exception) =>
        log.error(s"There was an error trying to get the Data Frame, please see: \n $exception")
        spark.emptyDataFrame
    }
  }

  def getDataframeFromTSV(inputPath: String): DataFrame = {
    tryToGetDataFrameFromTsv(inputPath) match {
      case Success(dataFrame) =>
        dataFrame
      case Failure(exception) =>
        log.error(s"There was an error trying to get the Data Frame, please see: \n $exception")
        spark.emptyDataFrame
    }
  }
}
