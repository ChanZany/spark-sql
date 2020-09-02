package com.chanzany.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object SparkSQL_GeneralLoad {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL_GeneralLoad")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._

    //TODO Spark通用的读取
    //RuntimeException: file:xxx/user.json is not a Parquet file
    // SparkSQL通用读取的默认数据格式为Parquet格式(列式存储)
//    val frame: DataFrame = spark.read.load("input/user.json")
//    val frame: DataFrame = spark.read.load("input/users.parquet")

    //TODO 如果想要改变读取文件的格式，需要使用特殊的操作
    // 如果读取的文件格式为JSON格式，Spark对JSON文件的格式有要求
    // Spark读取文件默认以行为单位，所以要求文件中的每一行符合JSON的格式要求
    // 如果文件格式不符合要求，会有解析错误产生
//    val frame = spark.read.format("json").load("input/user.json")
//    frame.show()

    //在读取json时可以将数据源直接作为一张表
    // 表名：json.`path`
    spark.sql("select * from json.`input/user.json`").show()

    spark.stop()

  }



}
