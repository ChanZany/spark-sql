package com.chanzany.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, MapType, StringType, StructField, StructType}

object SparkSQL_Req_Demo_Advanced {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "atguigu")
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
        |	  p.product_name,
        |   c.city_name
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
    // TODO 创建自定义聚合函数
    val udaf = new CityRemarkUDAF()
    // TODO 注册聚合函数
    spark.udf.register("cityRemark", udaf)

    spark.sql(
      """
        |select
        |	  area,
        |	  product_name,
        |	  count(*) as clickCount,
        |   cityRemark(city_name)
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
  class CityRemarkUDAF extends UserDefinedAggregateFunction {
    //TODO 输入类型[城市1，城市2...]
    override def inputSchema: StructType = {
      StructType(Array(StructField("cityName", StringType)))
    }

    //TODO 缓冲区类型(totalCnt,map[cityName,clickCnt])
    override def bufferSchema: StructType = {
      StructType(Array(
        StructField("totalCount", LongType),
        StructField("cityMap", MapType(StringType, LongType))
      ))
    }

    //TODO 输出类型("北京-20%,河北-15%,其他65%")
    override def dataType: DataType = StringType

    override def deterministic: Boolean = true

    //TODO 缓冲区初始化
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = 0L
      buffer(1) = Map[String, Long]()
    }

    //TODO 缓冲区数据与输入数据的聚合
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      val cityName: String = input.getString(0)
      //点击总和增加
      buffer(0) = buffer.getLong(0) + 1
      //城市点击增加
      val cityMap: Map[String, Long] = buffer.getAs[Map[String, Long]](1)
      val newClickCount: Long = cityMap.getOrElse(cityName, 0L) + 1
      buffer(1) = cityMap.updated(cityName, newClickCount)
    }

    //TODO 缓存区数据之间的合并
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      //合并点击总和
      buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
      //合并城市点击Map
      val map1: collection.Map[String, Long] = buffer1.getMap[String, Long](1)
      val map2: collection.Map[String, Long] = buffer2.getMap[String, Long](1)
      buffer1(1) = map1.foldLeft(map2) {
        case (map, (k, v)) => {
          map.updated(k, map.getOrElse(k, 0L) + v)
        }
      }
    }

    //TODO 计算逻辑：(totalCnt,map[cityName,clickCnt])=>"北京-20%,河北-15%,其他65%"
    override def evaluate(buffer: Row): Any = {
      val res = new StringBuilder
      val totalCount: Long = buffer.getLong(0)
      val cityMap: collection.Map[String, Long] = buffer.getMap[String, Long](1)

      val cityToCntList: List[(String, Long)] = cityMap.toList.sortWith(
        (left, right) => left._2 > right._2
      ).take(2)

      val hasRest: Boolean = cityMap.size>2
      var mainPie = 0L

      cityToCntList.foreach {
        case (city, cnt) => {
          val r = (100 * cnt / totalCount)
          res.append(city + "-" + r+"%,")
          mainPie = mainPie + r
        }
      }

      if (hasRest){
        res.toString()+"其他-"+(100L-mainPie)+"%"
      }else{
        res.toString()
      }

    }
  }

}
