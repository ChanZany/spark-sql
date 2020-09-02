package com.chanzany.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSQL_Load_csv {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL_Load_csv")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    //TODO 方式1
//    val frame: DataFrame = spark.read.format("csv")
//      .option("sep", ",")
//      .option("inferSchema", "true")
//      .option("header", "true")
//      .load("input/user.csv")

    //TODO 方式2
    val frame = spark.read.option("header","true").csv("input/user.csv")
    frame.show()

    spark.stop()

  }



}
