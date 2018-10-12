package com.github.anicolaspp.aggregator

import play.api.libs.json.Json

trait JsonSerializer {
  implicit val linkReads = Json.format[Link]

  def toJson(t: (Link, Int)) =
    s"{\42_id\42 : \42${t._1.path.drop(1).replace('/', '.')}\42, \42total\42: ${t._2}}"

}
