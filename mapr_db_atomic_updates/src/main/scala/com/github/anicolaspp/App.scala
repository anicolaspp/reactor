package com.github.anicolaspp

import com.github.anicolaspp.DocumentStore._
import org.ojai.store.{DocumentStore, DriverManager}

import scala.concurrent.ExecutionContextExecutor

object App {

  private lazy val connection = DriverManager.getConnection("ojai:mapr:")

  def main(args: Array[String]): Unit = {

    implicit val documentStore: DocumentStore = connection.getStore("/user/mapr/tables/view_counts")

    implicit val ex: ExecutionContextExecutor = scala.concurrent.ExecutionContext.global

    documentStore.getJsonDocuments().foreach(println)

    val sessionID = "001"

    UpdateSameId.run(sessionID, 10)

    documentStore.getJsonDocuments().foreach(println)

    documentStore.close()
  }
}