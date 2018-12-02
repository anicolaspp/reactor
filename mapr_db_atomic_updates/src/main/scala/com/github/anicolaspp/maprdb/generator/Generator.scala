package com.github.anicolaspp.maprdb.generator

import com.mapr.db.MapRDB
import org.ojai.store.DocumentStore

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random

object Generator {

  def generate(numberOfSessions: Int)(implicit ex: ExecutionContext, store: DocumentStore): Future[List[String]] = Future {
    val inserts = (1 to numberOfSessions)
      .par
      .map(_ => Random.nextString(1000))
      .map { randomSessionId =>
        Future {
          val document = MapRDB.newDocument()
            .set("_id", randomSessionId)
            .set("count", 0)

          store.insert(document)

          randomSessionId
        }
      }
      .toStream

    Future.foldLeft(inserts)(List.empty[String])((xs, id) => id :: xs)

  }.flatten

}
