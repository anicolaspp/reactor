package com.github.anicolaspp.maprdb

import org.ojai.store.DocumentStore

import scala.collection.JavaConverters._

object DocumentStore {
  implicit class RichDocumentStore(documentStore: DocumentStore) {
    def getJsonDocuments(): Stream[String] =
      documentStore
        .find()
        .iterator()
        .asScala
        .toStream
        .map(_.asJsonString())
  }
}
