package com.chanzany.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object SparkSQLDemo {
  def main(args: Array[String]): Unit = {
    //TODO 创建SparkSession对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL_demo")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    //TODO 导入隐式转换，这里的spark是环境变量的名称（必须用val声明）
    import spark.implicits._

    //TODO 逻辑操作
    val jsonDF = spark.read.json("input/user.json")
    //将dataFrame转换为临时的视图
    jsonDF.createOrReplaceTempView("user")

    //TODO spark sql
    spark.sql("select * from user").show()


    //TODO DSL
    // 如果查询列名采用单引号，那么需要隐式转换
    jsonDF.select("username","age").show
    jsonDF.select('username ,'age).show


    print("*****************************")

    //TODO RDD<=>DataFrame
    val rdd: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List(
      (1, "张三", 23),
      (2, "李四", 22),
      (3, "王五", 20)
    ))
    val df: DataFrame = rdd.toDF("id", "name", "age")
    val dfToRDD: RDD[Row] = df.rdd
    df.show()

    //TODO RDD<=>DataSet
    val userRDD = rdd.map {
      case (id, name, age) => {
        User(id, name, age)
      }
    }

    val userDS = userRDD.toDS()
    val userDSToRDD = userDS.rdd

    userDS.show()

    //TODO DataFrame<=>DataSet
    val dfToDS: Dataset[User] = df.as[User]

    val dsToDF = dfToDS.toDF()

    dfToDS.show()
    dsToDF.show()

    //TODO 释放环境对象
    spark.stop()



  }

  case class User(id:Int,name:String,age:Int)

}
