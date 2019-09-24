package com.analyse


import com.util.AnalUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.SparkSession

/**
  * 媒体指标
  */
object appAnal {
  def main(args: Array[String]): Unit = {

    if(args.length != 3){
      println("输入目录不正确")
      sys.exit()
    }

    val Array(input1,input2,output) =args

    val spark = SparkSession
      .builder()
      .appName("ct")
      .master("local")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    //读取数据
    val dictionMap = spark.sparkContext.textFile(input2).map(_.split("\\s",-1))

    //过滤
    dictionMap.filter(_.length>=5).map(arr=>(arr(4),arr(1))).collectAsMap()

    //广播
    val broadcast = spark.sparkContext.broadcast(input2)

    //读取文件
    val df = spark.read.parquet(input1)

    df.rdd.map(row=>{
      // 取媒体相关字段
      var appName = row.getAs[String]("appname")
      if(StringUtils.isBlank(appName)){
        appName = broadcast.value.getOrElse(row.getAs[String]("appid"),"unknow")
      }
      val requestmode = row.getAs[Int]("requestmode")
      val processnode = row.getAs[Int]("processnode")
      val iseffective = row.getAs[Int]("iseffective")
      val isbilling = row.getAs[Int]("isbilling")
      val isbid = row.getAs[Int]("isbid")
      val iswin = row.getAs[Int]("iswin")
      val adordeerid = row.getAs[Int]("adorderid")
      val winprice = row.getAs[Double]("winprice")
      val adpayment = row.getAs[Double]("adpayment")

      // 请求数
      val request =  AnalUtils.requestPt(requestmode,processnode)

      // 点击
      val click =AnalUtils.clickPt(requestmode,iseffective)

      // 广告
      val bidding =AnalUtils.biddingPt(iseffective,isbilling,isbid,iswin,adordeerid,winprice,adpayment)

      // 指标
      val allList:List[Double] = request ++ click ++ bidding
      (appName,allList)
    })

      .map(x =>x._1+","+x._2.mkString(",")).saveAsTextFile(output)








  }
}
