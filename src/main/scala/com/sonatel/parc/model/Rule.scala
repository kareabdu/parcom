package com.sonatel.parc.model

case class Filter(name:String, value: Array[FilterValue])

case class FilterValue(col: String, operator: String, value: Array[String])

//case class Rule(Libelle: String, colparc: String, filters: Array[Filter], groupby : Array[String])

case class Rule(Libelle: String, colparc: String, filters: Array[Filter], regroupement : String)

