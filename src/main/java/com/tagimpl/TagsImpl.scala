package com.tagimpl

import com.util.TagUtils
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.SparkSession

object TagsImpl {
  def main(args: Array[String]): Unit = {

    if(args.length!=4){
      println("目录不正确")
      sys.exit()
    }

    val Array(inputPath,docs,stopwords,day)=args

    val spark = SparkSession.builder().appName("Tags").master("local").getOrCreate()

    val df = spark.read.parquet(inputPath)

    val docsRDD = spark.sparkContext.textFile(docs).map(_.split("\\s")).filter(_.length>=5)

    val broadValue = spark.sparkContext.broadcast(docsRDD)

    val stopwordsRDD = spark.sparkContext.textFile(stopwords).map((_,0)).collectAsMap()

    val broadValues = spark.sparkContext.broadcast(stopwordsRDD)

    df.map(row=>{
      val userId:String = TagUtils.getOneUserId(row)

      //商圈标签
      val businessList = Business.makeTags(row)

    })
  }
}
