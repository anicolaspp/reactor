package com.github.anicolaspp.generalstats.conf

case class Configuration(stream: String, tableName: String, appName: String)

object Configuration {

  def parse(args: Seq[String]): Configuration = parser.parse(args, Configuration.default).get

  def default: Configuration = DefaultConfiguration

  object DefaultConfiguration extends Configuration(
    "/user/mapr/streams/click_stream:all_links_2",
    "/user/mapr/tables/total_counts",
    "general stats"
  )

  private val parser = new scopt.OptionParser[Configuration]("general stats") {
    head("general stats")

    opt[String]('s', "stream")
      .action((p, config) => config.copy(stream = p))
      .maxOccurs(1)
      .text("MapR stream name to read messages from")

    opt[String]('t', "table")
      .action((t, config) => config.copy(tableName = t))
      .maxOccurs(1)
      .text("MapR-DB table name to write stats to")

    opt[String]('n', "name")
      .action((n, config) => config.copy(appName = n))
      .text("Spark application name override")
  }
}