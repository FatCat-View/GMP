package com.analyse

import org.apache.spark.sql.SparkSession

/**
  * 省市指标统计
  */
object ProCity {
  def main(args: Array[String]): Unit = {
    //判断
    if(args.length != 1){
      println("输入目录不正确")
      sys.exit()
    }

    //输入输出目录
    val Array(input) =args

    val spark = SparkSession
      .builder()
      .appName("ct")
      .master("local")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    //获取数据
    val dataframe = spark.read.parquet(input)

    //注册临时视图
    dataframe.createTempView("logs")

    //获取数据
    val dataframe2 =spark.sql("select provincename,cityname,count(*) ct from log group by provincename,cityname")

    //分区存储
    dataframe2.write.partitionBy("provincename","cityname").json("C:\\Users\\Lenovo\\Desktop\\procity")

    spark.stop()

  }


}
