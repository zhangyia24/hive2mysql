package com.yoloho.bigdata

import java.io.{FileNotFoundException, IOException}
import java.sql.BatchUpdateException
import java.util.{Date, Properties}

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object hive2mysql {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\service\\hadoop-2.7.4-window") //加载hadoop组件
    val conf: SparkConf = new SparkConf().setAppName("hive2mysql").setMaster("local[4]")
      .set("spark.executor.memory", "1g")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val sparksession: SparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()


    try {
      val hiveContext = new HiveContext(sc)
      val l: Long = System.currentTimeMillis()

      println("开始时间"+new Date().toLocaleString)
      val frame: DataFrame = hiveContext.sql("select * from bdm.dayima_search_log ")

      val url="jdbc:mysql://localhost:3306/spark"
      val tablename="search0912"
      val properties = new Properties()
      properties.setProperty("user","root")
      properties.setProperty("password","dayima")
      frame.write.mode("append").jdbc(url,tablename,properties)
      println("结束时间"+new Date().toLocaleString)

      frame.collect().foreach(println) //查询数据
    }
    catch {
      case e1:BatchUpdateException=>println(new Date().toLocaleString)
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
