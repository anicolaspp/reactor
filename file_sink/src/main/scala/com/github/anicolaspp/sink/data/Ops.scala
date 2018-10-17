package com.github.anicolaspp.sink.data

import com.github.anicolaspp.sink.conf.Configuration
import com.mapr.db.spark.impl.OJAIDocument
import org.joda.time.DateTime

import scala.util.Try

object Ops {

  import com.github.anicolaspp.sink.conf.ConfigurationKeys._

  def createIndex(json: OJAIDocument)(implicit config: Configuration): Index =
    Index(json.asJsonString(), s"${config.indexes(baseFilePathKey)}/${DateTime.now().getMillis}.json", isHotLink(json))

  private def isHotLink(document: OJAIDocument): Boolean =
    Try {
      document.getBoolean("isHot")
    }.getOrElse(false) || Try {
      document.getBoolean("hot")
    }.getOrElse(false)
}