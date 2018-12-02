package com.github.anicolaspp.maprdb

import com.mapr.db.MapRDB
import org.ojai.store.DocumentStore

import scala.concurrent.{ExecutionContext, Future}

object UpdateSameId {

  def run(id: String, times: Int)(implicit ec: ExecutionContext, store: DocumentStore) = Future {

    (1 to times).foreach { _ =>
      val mutation = MapRDB
        .newMutation()
        .increment("count", 1)

      store.update(id, mutation)
    }

    store.findById(id).getInt("count")
  }
}
