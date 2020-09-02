package com.chanzany.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSQL_Load_Hive {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL_Load_Hive")
    //TODO 默认情况下SparkSQL支持本地Hive操作，执行前需要启用Hive的支持
    val spark = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()

    //    spark.sql("create table aa(id int)")
    //    spark.sql("load data local inpath 'input/aa.txt' into table aa")
    //    spark.sql("select * from aa").show()

    spark.sql("show databases").show()
    spark.stop()

  }


}
