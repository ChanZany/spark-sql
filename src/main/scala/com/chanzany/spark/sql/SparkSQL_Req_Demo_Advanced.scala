package com.chanzany.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction

object SparkSQL_Req_Demo_Advanced {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "chanzany")
    val conf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL_Req_Demo_Advanced")
    val spark = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()
    import spark.implicits._

    spark.sql("use spark_practice")

    //TODO 从Hive表中获取满足条件的数据
    spark.sql(
      """
        |select
        |	  a.*,
        |	  c.area,
        |	  p.product_name
        |from user_visit_action a
        |join city_info c on c.city_id = a.city_id
        |join product_info p on p.product_id = a.click_product_id
        |where a.click_product_id>-1
        |""".stripMargin).createOrReplaceTempView("t1")

    //TODO 将数据根据区域和商品进行分组，统计商品点击的数量
    // 北京，上海，成都，深圳
    // *********************************
    // in:cityName:String
    // buffer：(total,map)
    // out：remark:String
    // （商品点击总和，每个城市的点击总和）
    // （商品点击总和，Map[城市，点击sum]）
    // 城市点击sum/商品点击总和 %
    spark.sql(
      """
        |select
        |	  area,
        |	  product_name,
        |	  count(*) as clickCount
        |from t1
        |group by area,product_name
        |""".stripMargin).createOrReplaceTempView("t2")

    //TODO 将统计结果根据数量进行排序(降序)
    spark.sql(
      """
        |select
        |	  *,
        |	  rank() over(partition by area order by clickCount desc) as rank
        |from t2
        |""".stripMargin).createOrReplaceTempView("t3")
    //TODO 将组内排序后的结果取TOP-N
    spark.sql(
      """
        |select
        |	  *
        |from t3
        |where rank<=3
        |""".stripMargin).show()

    spark.stop()

  }

  //自定义城市备注的聚合函数
  class CityRemarkUDAF extends UserDefinedAggregateFunction{
}

}
