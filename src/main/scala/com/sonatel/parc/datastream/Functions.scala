package com.sonatel.parc.datastream

import com.sonatel.parc.model.{Rule}

import scala.io.Source

object Functions {

  def getRuleObject(pathJsonFile: String) : Rule = {
    import net.liftweb.json._
    implicit val formats: DefaultFormats.type = DefaultFormats

    val isDir = pathJsonFile.contains("{")
    val fileContents = if (isDir) {
      pathJsonFile
    } else {
      val stream = Source.fromFile(pathJsonFile)
      val value = stream.getLines.mkString
      stream.close()
      value
    }

    val json = parse(fileContents)
    json.extract[Rule]
  }

}
