package com.chanzany.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSQL_GeneralSave {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL_GeneralSave")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    //TODO Spark通用的保存
    val df = spark.read.format("json").load("input/user.json")
    // TODO 默认的保存格式为parquet
    //    df.write.save("output")
    // 如果想要按指定的格式保存，那么需要进行对应的格式化操作
//    df.write.format("json").save("output")

    //org.apache.spark.sql.AnalysisException: path file:/D:/Code/spark-sql/output already exists
    // 如果路径已经存在，那么执行保存操作会发生错误
    // 如果非要在路径已经存在的情况下保存新的数据，可以指定保存模式
//    df.write.mode("overwrite").format("json").save("output")
    df.write.mode("append").format("json").save("output")

    spark.stop()

  }


}
