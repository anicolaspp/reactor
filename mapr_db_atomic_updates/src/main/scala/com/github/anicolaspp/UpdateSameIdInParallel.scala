package com.github.anicolaspp

import org.ojai.store.DocumentStore

import scala.concurrent.{ExecutionContext, Future}

object UpdateSameIdInParallel {

  def run(id: String, times: Int, threads: Int)(implicit ec: ExecutionContext, store: DocumentStore): Future[Int] = Future {

    val updates = (1 to threads).map { _ => UpdateSameId.run(id, times) }

    Future.reduceLeft(updates)(_ + _)
  }
    .flatten
}
