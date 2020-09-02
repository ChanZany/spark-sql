package com.chanzany.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSQLDemo2 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL_demo2")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._


    val rdd: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List(
      (1, "张三", 23),
      (2, "李四", 22),
      (3, "王五", 20)
    ))
    val df: DataFrame = rdd.toDF("id", "name", "age")

    val userRDD = rdd.map {
      case (id, name, age) => {
        User(id, name, age)
      }
    }

    val ds = userRDD.toDS()

    //TODO SparkSQL封装的对象提供了大量的方法进行数据的处理，类似于RDD的算子操作
    //    df.join()
    //    df.coalesce()
    //    df.reduce()


    //TODO 释放环境对象
    spark.stop()


  }

  case class User(id: Int, name: String, age: Int)

}
