package com.github.anicolaspp.maprdb.conf

case class Configuration(appType: AppType)

object Configuration {

  def parse(args: Seq[String]): Configuration = parser.parse(args, Configuration.default).get

  def default: Configuration = Configuration(Transact)

  private lazy val parser = new scopt.OptionParser[Configuration]("maprdb atomic updates") {
    head("maprdb atomic updates")

    opt[AppType]('t', "appTyoe")
      .action((appType, config) => config.copy(appType = appType))
      .maxOccurs(1)
      .required()
      .text("What type of app will be executed. [gen, transact]")
      .validate {
        case Undefined  => Left("AppType cannot be undefined. Please, choose between gen or transact")
        case _          => Right()
      }
  }

  implicit val appTypeReader: scopt.Read[AppType] =
    scopt.Read.reads(readAppTypeFromString)

  private def readAppTypeFromString(arg: String) = arg match {
    case "gen" => Gen
    case "transact" => Transact
    case _ => Undefined
  }
}