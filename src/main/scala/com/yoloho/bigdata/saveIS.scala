package com.yoloho.bigdata

import java.io.{FileNotFoundException, IOException}

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object saveIS {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\service\\hadoop-2.7.4-window") //加载hadoop组件
    val conf: SparkConf = new SparkConf().setAppName("").setMaster("local[4]")
      .set("spark.executor.memory", "1g")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")
    try {
      val hiveContext = new HiveContext(sc)
      val frame: DataFrame = hiveContext.sql("select * from bdm.dayima_search_log order by search_time desc limit 1000")
      frame.write.csv("e://demo00"+System.currentTimeMillis())
      frame.collect().foreach(println) //查询数据
    }
    catch {
      case e: FileNotFoundException => println("Missing file exception")
      case ex: IOException => println("IO Exception")
      case ee: ArithmeticException => println(ee)
      case eee: Throwable => println("found a unknown exception" + eee)
      case ef: NumberFormatException => println(ef)
      case ec: Exception => println(ec)
      case e: IllegalArgumentException => println("illegal arg. exception")
      case e: IllegalStateException => println("illegal state exception")
    }
    finally {
      sc.stop()
    }


  }
}
