package com.kero99.wp

import org.apache.spark.sql.SparkSession

/**
  *
  * @author wp
  * @date 2019-08-30 14:17
  *
  */
class WpSession extends App {
  val session = SparkSession.builder()
    .master("local[2]")
    .appName("test")
    .getOrCreate()
  val sc = session.sparkContext
  sc.setLogLevel("WARN")
}
