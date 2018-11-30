package com.github.anicolaspp

import org.ojai.store.DocumentStore

import scala.collection.JavaConverters._

object DocumentStore {

  implicit class RichDocumentStore(documentStore: DocumentStore) {
    def getJsonDocuments(): Stream[String] =
      toStream(documentStore.find().iterator().asScala)
      .map(_.asJsonString())

    private def toStream[A](iterator: Iterator[A]): Stream[A] = {

      def loop[B](a: Option[B], xs: Iterator[B]): Stream[B] = a match {
        case None    => Stream.empty
        case Some(v) => v #:: loop(next(xs), xs)
      }

      def next[C](iterator: Iterator[C]) = if (!iterator.hasNext) None else Some(iterator.next())

      loop(next(iterator), iterator)
    }
  }

}
