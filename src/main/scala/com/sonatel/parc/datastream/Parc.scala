package com.sonatel.parc.datastream

import com.sonatel.parc.session.CreateSession
import org.apache.spark.sql
import org.apache.spark.sql.functions.date_sub
import org.apache.spark.sql.functions.lit

import scala.collection.mutable.ListBuffer


object Parc {

  val spark = CreateSession.spark

  /*
    Function used to load the Json file and generate the sql request and the regroupement from a parc
   */
  def getRequestAndRegroupement(file : String, dateStart : String) : List[String] = {
    // get started date
    val spark = CreateSession.spark
    import spark.implicits._
    val dateEnd = Seq(s"$dateStart").toDF("date")
      .select($"date", date_sub($"date", 30).as("diff"))
    val elementsFirst = dateStart.split("-")
    val yearFirst = elementsFirst(0)
    val monthFirst = elementsFirst(1)
    val dFirst = elementsFirst(2)
    val dayFirst = s"$yearFirst$monthFirst$dFirst".toInt

    val elementsEnd = dateEnd.select($"diff").first().get(0).toString.split("-")
    val yearEnd = elementsEnd(0)
    val monthEnd = elementsEnd(1)
    val dEnd = elementsEnd(2)
    val dayEnd = s"$yearEnd$monthEnd$dEnd".toInt

    //val spark = CreateSession.spark
    val lRule = Functions.getRuleObject(s"$file")
    val parcRule = spark.sparkContext.broadcast(lRule)
    // get the selected column 
    val colparc = parcRule.value.colparc
    val regroupement = parcRule.value.regroupement
    var andFilter = ""
    var request =
      s"""
         |SELECT DISTINCT $colparc AS NUMBER FROM transactions WHERE
         |""".stripMargin
    if (parcRule.value.filters.length > 0) {
      andFilter = " AND "
    }
    for (level1 <- parcRule.value.filters) {
      for (level2 <- level1.value) {
        val colSelected = level2.col
        val operator = level2.operator
        // Verify if the number of values is equal to 1
        var value2 = " "
        if (level2.value.length == 1) {
          val value1 = level2.value(0)
          request = request + colSelected + operator + s"'$value1'" + s" $andFilter "
        }
        // get all values
        else {
          value2 = value2 + " ("
          for (v <- level2.value) {
            value2 = value2 + s"'$v'" + ","
          }
          request = request + colSelected + s" $operator " + value2.substring(0, value2.length - 1) + ")" + s" $andFilter "
        }
      }
    }
    request = request +
      s"""
         |year BETWEEN $yearEnd AND $yearFirst AND
         |month BETWEEN $monthEnd AND $monthFirst AND
         |day BETWEEN $dayEnd AND $dayFirst""".stripMargin

    List(request, regroupement)
  }

  /*
    Function used to select all users finding in all parcs
   */
  def getUsersInParcs : sql.DataFrame  = {

    val spark = CreateSession.spark
    // Create a dataframe. This dataframe will be used to merge all results
    import spark.implicits._
    var dfMerge =  Seq.empty[(String, String)].toDF("NUMBER", "PARC")

    val fileSource = LoadFile.load("transactions_om_2020.csv")
    fileSource.createOrReplaceTempView("transactions")
    val filesJson = SelectFilesParcs.getFiles("src/main/resources/Parc2")

    if (filesJson.nonEmpty) {
      try for(i <- 0 to filesJson.length) {
        val file = filesJson(i)
        val requestAndRegroupement = Parc.getRequestAndRegroupement(s"$file", "2020-02-01")
        val request = requestAndRegroupement(0)
        val regroupement = requestAndRegroupement(1)
        val df = spark.sql(s"$request").withColumn("PARC", lit(regroupement))
        // Merge the result to the dataframe
        dfMerge = df.union(dfMerge)
      }
      catch {
        case ex : java.lang.IndexOutOfBoundsException => { println("Traitement OK!!!") }
      }
    }
    else {  println("Veuillez verifier le repertoire selectionné") }
    dfMerge
  }
  //getUsersInParcs.show()

  /*
    Function used to get all parcs names
   */
  def getAllParcs(path: String = "src/main/resources/Parc2") : List[String] = {
    val filesJson = SelectFilesParcs.getFiles(s"$path")
    var parcs = new ListBuffer[String]()
    if(filesJson.nonEmpty) {
      filesJson.foreach(file => {
        val lRule = Functions.getRuleObject(s"$file")
        val parcRule = spark.sparkContext.broadcast(lRule)
        parcs += parcRule.value.regroupement
      })
    }
    else
    {
      println("Directory empty or not exist!!!")
    }
    parcs.toList
  }

}
