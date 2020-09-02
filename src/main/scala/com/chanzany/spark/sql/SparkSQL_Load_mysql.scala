package com.chanzany.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object SparkSQL_Load_mysql {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL_Load_mysql")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val frame = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/spark-sql")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "root")
      .option("dbtable", "user")
      .load()

    frame.write.format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/spark-sql")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "root")
      .option("dbtable","user1")
      .mode(SaveMode.Append)
      .save()


    spark.stop()

  }


}
