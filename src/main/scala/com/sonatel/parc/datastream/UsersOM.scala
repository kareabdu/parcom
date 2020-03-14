package com.sonatel.parc.datastream

import com.sonatel.parc.session.CreateSession
import org.apache.spark.sql.functions.lit


object UsersOM extends App {

  /*
    Function used to give all active customers in the parc OM
   */
  def getActiveCustomers = {
    val spark = CreateSession.spark
    import spark.implicits._

    val omSubscribers = LoadFile.load("om_subscribers.csv")
    val listUsers = omSubscribers.select("MSISDN").collect().map(_(0)).toList
    val gradeNames = omSubscribers.select("USER_GRADE_NAME").collect().map(_(0)).toList
    val UsersInParcs = Parc.getUsersInParcs.select("NUMBER").collect().map(_(0)).toList
    // This dataFrame will contains MSISDN, grade_name and parc_actif_jour of OMUsers
    var df =  Seq.empty[(String, String, Integer)]
      .toDF("MSISDN", "grade_name", "parc_actif_jour")
    for (i <-0 to listUsers.length -1) {
      val gradeUser = gradeNames(i)
      val msisdn = listUsers(i).toString
      var intOut = 0
      if (UsersInParcs.contains(msisdn)) {  intOut = 1  }
      val parcDF = Seq((s"$msisdn", s"$gradeUser", intOut)).toDF("MSISDN", "grade_name", "parc_actif_jour")
      // Finally merge all results
      df = parcDF.union(df)
      println(i)
      df.show()
    }
    df
  }
  /*val p = getActiveCustomers
  p.filter(p("parc_actif_jour") === 1).show()*/

  /*
    Function used to give all active customers by service in the parc OM
   */
  def getActiveCustomersByService = {
    val spark = CreateSession.spark
    import spark.implicits._

    val allParcs = Parc.getAllParcs()
    val omSubscribers = LoadFile.load("om_subscribers.csv")
    val listUsers = omSubscribers.select("MSISDN").collect().map(_(0)).toList
    val gradeNames = omSubscribers.select("USER_GRADE_NAME").collect().map(_(0)).toList
    val UsersInParcs = Parc.getUsersInParcs.select("NUMBER", "PARC")
    //UsersInParcs.createOrReplaceTempView("UsersInParcs")

    var dfTotal =  Seq.empty[(String, String, String, Integer)]
      .toDF("MSISDN", "transaction_tag" , "grade_name", "parc_actif_service_jour")
    /*----------------------------------------------------------------------------------------*/
    for (i <-0 to 1) {
      val msisdn = listUsers(i).toString
      var df = UsersInParcs.select("NUMBER", "PARC").filter(UsersInParcs("NUMBER") === s"$msisdn")
      df = df.withColumn("grade_name", lit(gradeNames(i)))
        .withColumn("parc_actif_service_jour", lit(1))
      // Select parc present in the dataFrame
      var parcsUser = df.select("PARC").collect().map(_(0)).toList
      parcsUser = allParcs.diff(parcsUser)
      parcsUser.foreach(parc => {
        val grade = gradeNames(i)
        val row = Seq((s"$msisdn", s"$parc", s"$grade", 0))
          .toDF("msisdn", "transaction_tag", "grade_name", "parc_actif_service_jour")
        df = row.union(df)
        df.show()
      })
      dfTotal = df.union(dfTotal)
      dfTotal.orderBy("MSISDN").show(100)
    }
    /*----------------------------------------------------------------------------------------*/

    /*for (i <-0 to listUsers.length) {
      val msisdn = listUsers(i).toString
      //var df = spark.sql(s"SELECT NUMBER AS msisdn, PARC AS transaction_tag FROM UsersInParcs WHERE NUMBER=$msisdn")
      var df = UsersInParcs.select("NUMBER", "PARC").filter(UsersInParcs("NUMBER") === s"$msisdn")
      df = df.withColumn("grade_name", lit(gradeNames(i)))
        .withColumn("parc_actif_service_jour", lit(1))
      // Select parc present in the dataFrame
      var parcsUser = df.select("PARC").collect().map(_(0)).toList
      parcsUser = allParcs.diff(parcsUser)
      parcsUser.foreach(parc => {
        val grade = gradeNames(i)
        val row = Seq((s"$msisdn", s"$parc", s"$grade", 0))
          .toDF("msisdn", "transaction_tag", "grade_name", "parc_actif_service_jour")
        row.show()
        df = row.union(df)
        df.show()
      })
      dfTotal = df.union(dfTotal)
      dfTotal.show()
    }*/
    dfTotal
  }
  getActiveCustomersByService


}
