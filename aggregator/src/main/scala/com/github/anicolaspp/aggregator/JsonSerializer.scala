package com.github.anicolaspp.aggregator

import play.api.libs.json.{Format, Json}

trait JsonSerializer {
  private implicit val linkReads: Format[Link] = Json.format[Link]

  def toJson(t: (Link, Int)) =
    s"{\42_id\42 : \42${t._1.path.drop(1).replace('/', '.')}\42, \42total\42: ${t._2}}"

  def fromJson(json: String) = linkReads.reads(Json.parse(json)).asOpt.get
}
