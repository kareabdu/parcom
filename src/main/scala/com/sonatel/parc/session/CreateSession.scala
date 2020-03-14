package com.sonatel.parc.session

import org.apache.spark.sql.SparkSession

object CreateSession {

  val spark = SparkSession.builder()
    .appName("Session KPI")
    .master("local")
    .getOrCreate()
  spark

}
