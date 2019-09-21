package com.twq.prepaser

/**
export HADOOP_CONF_DIR=/soft/hadoop/etc/hadoop/
spark-submit --master yarn \
--class com.twq.prepaser.PreparseETL \
--driver-memory 512M \
--executor-memory 512M \
--num-executors 2 \
--executor-cores 1 \
--conf spark.traffic.analysis.rawdata.input=hdfs://master:9999/user/hadoop-twq/traffic-analysis/rawlog/20180617 \
/home/hadoop-twq/traffice-analysis/jars/spark-preparse-etl-1.0-SNAPSHOT-jar-with-dependencies.jar prod
  */


import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession,Encoders,SaveMode}

object PreparseETL {

  def main(args: Array[String]): Unit = {


    /*val conf = new SparkConf()

    if(args.isEmpty){
      conf.setMaster("local")
    }*/

    //spark应用入口

    val spark = SparkSession.builder()
      .appName("PreparseETL")
         .master("local")
      .enableHiveSupport()
      .getOrCreate()

    //元数据地址

    val rawdataInputPath = spark.conf.get("spark.traffic.analysis.rawdata.input",
    "hdfs://master:9999/user/hadoop-twq/traffic-analysis/rawlog/20180616")

    //设置数据的分区数
    val numberPartitions = spark.conf.get("spark.traffic.analysis.rawdata.numberPartitions", "2").toInt

    //解析每一行数据  调用weblog-perparaser的方法
    val preParsedLogRDD = spark.sparkContext.textFile(rawdataInputPath)
      .flatMap(line => Option(WebLogPreParser.parse(line)))

    //根据rdd和javebeen创建dataset
    val preParsedLogDS = spark.createDataset(preParsedLogRDD)(Encoders.bean(classOf[PreParsedLog]))

    //写数据到hive表，按照实体类的年月日分区
    preParsedLogDS.coalesce(numberPartitions).write
      .mode(SaveMode.Append)
      .partitionBy("year","month","day")
      .saveAsTable("rawdata.web")

    spark.stop()
  }


}
