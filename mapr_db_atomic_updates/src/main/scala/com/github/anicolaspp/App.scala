package com.github.anicolaspp

import com.github.anicolaspp.DocumentStore._
import org.ojai.store.{DocumentStore, DriverManager}

import scala.concurrent.ExecutionContext.Implicits.global

object App {

  private lazy val connection = DriverManager.getConnection("ojai:mapr:")

  def main(args: Array[String]): Unit = {

    implicit val documentStore: DocumentStore = connection.getStore("/user/mapr/tables/view_counts")

    documentStore.getJsonDocuments().foreach(println)

    val sessionID = "001"

    UpdateSameIdInParallel
      .run(sessionID, 10, 20)
      .foreach { _ =>
        documentStore.getJsonDocuments().foreach(println)
        documentStore.close()
      }

    println("done....")
  }
}