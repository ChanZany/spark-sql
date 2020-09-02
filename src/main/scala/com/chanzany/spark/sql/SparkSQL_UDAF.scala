package com.chanzany.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object SparkSQL_UDAF {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("SparkSql_UDAF")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    val rdd: RDD[(Int, String, Long)] = spark.sparkContext.makeRDD(List(
      (1, "张三", 23L),
      (2, "李四", 22L),
      (3, "王五", 20L)
    ))

    val df: DataFrame = rdd.toDF("id", "name", "age")
    df.createOrReplaceTempView("user")

    //TODO 使用自定义聚合函数在SQL中完成数据的转换
    // 1. 创建UDAF函数
    val udaf = new MyAvgUDAF
    // 2. 注册到SparkSQL中
    spark.udf.register("avgAge", udaf)
    // 3. 在SQL中使用聚合函数
    spark.sql("select avgAge(age) from user").show()


    spark.stop()
  }

  case class User(id: Int, name: String, age: Int)

  /** 自定义聚合函数
   * 1. 继承UserDefinedAggregateFunction
   * 2. 重写方法
   */
  class MyAvgUDAF extends UserDefinedAggregateFunction {
    //TODO 指定输入类型:年龄信息
    override def inputSchema: StructType = {
      StructType(Array(StructField("age", LongType)))

    }


    //TODO 指定计算类型(缓冲区)：年龄总和，人的数量
    override def bufferSchema: StructType = {
      StructType(Array(
        StructField("totalAge", LongType),
        StructField("count", DoubleType)
      ))
    }

    //TODO 指定输出类型(年龄总和/人的数量)
    override def dataType: DataType = DoubleType

    //TODO 指定函数是否稳定(随机)
    override def deterministic: Boolean = true

    //TODO 函数的初始化
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = 0L
      buffer(1) = 0d

    }

    //TODO 更新缓存区（聚合的规则）
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      buffer(0) = buffer.getLong(0) + input.getLong(0)
      buffer(1) = buffer.getDouble(1) + 1
      //      println("totalAge:"+buffer(0))
      //      println("count:"+buffer(1))
    }

    //TODO 不同机器的缓冲区数据合并
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
      buffer1(1) = buffer1.getDouble(1) + buffer2.getDouble(1)
      //      println("totalAge of merge:"+buffer1(0))
      //      println("count of merge:"+buffer1(1))
    }

    //TODO 函数的计算（计算逻辑）
    override def evaluate(buffer: Row): Any = {
      //      print(buffer.getLong(0),buffer.getLong(1))
      buffer.getLong(0) / buffer.getDouble(1)
    }
  }


}
