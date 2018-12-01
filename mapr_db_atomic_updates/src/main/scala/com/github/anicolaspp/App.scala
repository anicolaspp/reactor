package com.github.anicolaspp

import com.github.anicolaspp.DocumentStore._
import org.ojai.store.{DocumentStore, DriverManager}

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global

import scala.concurrent.duration._

object App {

  private lazy val connection = DriverManager.getConnection("ojai:mapr:")

  def main(args: Array[String]): Unit = {

    implicit val documentStore: DocumentStore = connection.getStore("/user/mapr/tables/view_counts")

    documentStore.getJsonDocuments().foreach(println)

    val sessionID = "001"

    val updates = UpdateSameIdInParallel
      .run(sessionID, times = 10, threads = 20)
      .map { _ =>
        documentStore.getJsonDocuments().foreach(println)
        documentStore.close()
      }

    Await.ready(updates, 10 minutes)

    println("done....")
  }
}