package com.github.anicolaspp.sink.conf


case class Configuration(appName: String,
                         inputStream: String = "",
                         offsetsTable: String = "",
                         indexes: Map[String, String] = Map.empty) extends Serializable

object Configuration {

  import ConfigurationKeys._

  def parse(args: Seq[String]): Configuration = parser.parse(args, Configuration.default).get

  def default: Configuration = Configuration("File Sink")

  private val parser = new scopt.OptionParser[Configuration]("File Sink") {
    head("File Sink")

    opt[String]('n', "name")
      .action((n, config) => config.copy(appName = n))
      .maxOccurs(1)
      .text("Spark application name override")

    opt[String]('s', "input-stream")
      .action((s, config) => config.copy(inputStream = s))
      .maxOccurs(1)
      .required()
      .text("MapR-ES stream") // /user/mapr/streams/click_stream:all_links_2"

    opt[String]('o', "offsets-table")
      .action((o, config) => config.copy(offsetsTable = o))
      .maxOccurs(1)
      .required()
      .text("MapR-DB offset table") // /user/mapr/tables/sink_offsets

    //--indexes isHot=/user/mapr/tables/indexes/isHot,time=/user/mapr/tables/indexes/time,basefilepath=/user/mapr/files/links/
    opt[Map[String, String]]("indexes")
      .action((idxs, config) => config.copy(indexes = idxs))
      .required()
      .text("Name of the indexes and corresponding index table name")
      .validate { map =>
        if (map.values.exists(_.endsWith("/"))) {
          Left("Paths to tables and file system cannot end with `/` character.")
        } else if (!map.contains(isHotKey) || !map.contains(timeKey) || !map.contains(baseFilePathKey)) {
          Left(s"The following keys are required: $isHotKey, $timeKey, $baseFilePathKey")
        } else {
          Right()
        }
      }
  }
}


