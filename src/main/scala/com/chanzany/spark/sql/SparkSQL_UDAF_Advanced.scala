package com.chanzany.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, SparkSession}

object SparkSQL_UDAF_Advanced {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("SparkSql_UDAF_Advanced")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    val rdd: RDD[(Int, String, Long)] = spark.sparkContext.makeRDD(List(
      (1, "张三", 23L),
      (2, "李四", 22L),
      (3, "王五", 20L)
    ))

    val df: DataFrame = rdd.toDF("id", "name", "age")
    df.createOrReplaceTempView("user")

    //TODO 使用自定义聚合函数(强类型)在SQL中完成数据的转换
    // 1. 创建UDAF函数
    val udaf = new MyAvgUDAFClass
    //TODO  2. 在DSL中使用强类型的UDAF函数
    // 由于聚合函数是强类型，而Sql中没有类型概念，所以无法使用
    // 可以采用DSL语法进行访问，将聚合函数转换为查询的列让Dataset访问
    val ds = df.as[User]
    ds.select(udaf.toColumn).show()

    spark.stop()
  }

  case class User(id: Int, name: String, age: Long)

  case class AvgBuffer(var totalAge: Long, var count: Double)

  /** 自定义聚合函数-强类型
   * 1. 继承UserDefinedAggregateFunction
   * 2. 重写方法
   */
  class MyAvgUDAFClass extends Aggregator[User, AvgBuffer, Double] {
    //TODO 缓冲区初始化
    override def zero: AvgBuffer = {
      AvgBuffer(0L, 0d)
    }

    //TODO 聚合数据
    override def reduce(buffer: AvgBuffer, user: User): AvgBuffer = {
      buffer.totalAge = buffer.totalAge + user.age
      buffer.count = buffer.count + 1
      buffer
    }

    //TODO 合并缓存区
    override def merge(b1: AvgBuffer, b2: AvgBuffer): AvgBuffer = {
      b1.totalAge = b1.totalAge + b2.totalAge
      b1.count = b1.count + b2.count
      b1
    }

    //TODO 函数计算
    override def finish(reduction: AvgBuffer): Double = {
      reduction.totalAge/reduction.count
    }

    override def bufferEncoder: Encoder[AvgBuffer] = Encoders.product

    override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
  }


}
