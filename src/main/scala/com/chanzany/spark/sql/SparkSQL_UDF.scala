package com.chanzany.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSQL_UDF {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("SparkSql_udf")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    val rdd: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List(
      (1, "张三", 23),
      (2, "李四", 22),
      (3, "王五", 20)
    ))

    val df: DataFrame = rdd.toDF("id", "name", "age")
    df.createOrReplaceTempView("user")
    //    df.map(row=>{
    //      val id = row(0)
    //      val name = row(1)
    //      val age = row(2)
    //      Row(id,"Name:"+name,age)
    //    })

    //    val userRDD = rdd.map {
    //      case (id, name, age) => {
    //        User(id, name, age)
    //      }
    //    }
    //    val ds = userRDD.toDS()
    //    val newDS = ds.map(user => {
    //      User(user.id, "Name:" + user.name, user.age)
    //    })
    //    newDS.show()

    //TODO 使用自定义函数在SQL中完成数据的转换
    spark.udf.register("addName", (x: String) => "Name:" + x)
    spark.udf.register("changeAge", (x: Int) => x - 10)

//    spark.sql("select addName(name),changeAge(age) from user").show()
    spark.sql("select addName(name),avg(age) from user").show()


    spark.stop()
  }

  case class User(id: Int, name: String, age: Int)

}
