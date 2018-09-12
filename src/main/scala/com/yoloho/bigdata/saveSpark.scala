package com.yoloho.bigdata

import java.io.{FileNotFoundException, IOException}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

object saveSpark {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\service\\hadoop-2.7.4-window") //加载hadoop组件
    val conf = new SparkConf().setAppName("HiveApp").setMaster("local[4]")
      .set("spark.executor.memory", "1g")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .setJars(Seq("D:\\workspace\\scala\\out\\scala.jar"))
    //加载远程spark
    //.set("hive.metastore.uris", "thrift://192.168.66.66:9083")//远程hive的meterstore地址
    // .set("spark.driver.extraClassPath","D:\\json\\mysql-connector-java-5.1.39.jar")
    val sparkcontext = new SparkContext(conf);
    try {
      val hiveContext = new HiveContext(sparkcontext);
      hiveContext.sql("use siat"); //使用数据库
      hiveContext.sql("DROP TABLE IF EXISTS src") //删除表
      hiveContext.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) " +
        "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' "); //创建表
      hiveContext.sql("LOAD DATA LOCAL INPATH 'D:\\workspace\\scala\\src.txt' INTO TABLE src  "); //导入数据
      hiveContext.sql(" SELECT * FROM src").collect().foreach(println); //查询数据
    }
    catch {
      case e: FileNotFoundException => println("Missing file exception")
      case ex: IOException => println("IO Exception")
      case ee: ArithmeticException => println(ee)
      case eee: Throwable => println("found a unknown exception" + eee)
      case ef: NumberFormatException => println(ef)
      case ec: Exception => println(ec)
      case e: IllegalArgumentException => println("illegal arg. exception");
      case e: IllegalStateException => println("illegal state exception");
    }
    finally {
      sparkcontext.stop()
    }
  }
}




