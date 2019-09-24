package com.analyse

import com.util.AnalUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * 统计地域指标
  */
object Location {

  def main(args: Array[String]): Unit = {

    if(args.length != 2){
      println("输入目录不正确")
      sys.exit()
    }

    val Array(input,output)=args

    val spark = SparkSession
      .builder()
      .appName("ct")
      .master("local")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    val frame = spark.read.parquet(input)

    //临时视图字段
    // REQUESTMODE	PROCESSNODE	ISEFFECTIVE	ISBILLING	ISBID	ISWIN	adorderid(广告id) winprice(消费) adpayment(成本)
    val analRdd: RDD[((Nothing, Nothing), List[Double])] = frame.rdd.map(
      row => {
        val requestmode = row.getAs[Int]("requestmode")
        val processnode = row.getAs[Int]("processnode")
        val iseffective = row.getAs[Int]("iseffective")
        val isbilling = row.getAs[Int]("isbilling")
        val isbid = row.getAs[Int]("isbid")
        val iswin = row.getAs[Int]("iswin")
        val adordeerid = row.getAs[Int]("adorderid")
        val winprice = row.getAs[Double]("winprice")
        val adpayment = row.getAs[Double]("adpayment")

        //点击指标
        val click = AnalUtils.clickPt(requestmode, iseffective)

        //广告
        val bidding = AnalUtils.biddingPt(iseffective, isbilling, isbid, iswin, adordeerid, winprice, adpayment)

        //请求
        val request = AnalUtils.requestPt(requestmode, processnode)

        //所有指标
        val allIndi = click ++ bidding ++ request

        //返回
        ((row.getAs("provincename"), row.getAs("cityname")), allIndi)
      }
    )

    //
     val analRdd2 = analRdd.reduceByKey(
       (list1,list2)=>
         {
           //list1(1,1,1,1).zip(list2(1,1,1,1))=list((1,1),(1,1),(1,1),(1,1))
           list1.zip(list2).map(t=>t._1+t._2)
         }
     )

    //anal      1 省份   2  输出
    analRdd2.map(x=>x._1+","+x._2.mkString(",")).saveAsTextFile(output)

  }

}
