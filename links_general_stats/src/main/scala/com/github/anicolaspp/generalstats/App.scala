package com.github.anicolaspp.generalstats

import com.github.anicolaspp.generalstats.data.Streamer
import com.mapr.db.MapRDB
import com.mapr.db.spark.MapRDBSpark
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka09.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import com.mapr.db.spark._
import com.mapr.db.spark.impl.OJAIDocument
import org.ojai.scala.Document

import scala.util.Try

object App extends Streamer {

  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setAppName("general stats")

    val sc = new SparkContext(config)
    implicit val ssc: StreamingContext = new StreamingContext(sc, Milliseconds(500))

    val messages = getStream("/user/mapr/streams/click_stream:all_links_2")

    messages
      .map(_.value())
      .map(MapRDBSpark.newDocument)
      .map(doc => (doc.getString("path"), 1))
      .reduceByKey(_ + _)
      .foreachRDD { rdd =>

        val fromDB = sc
          .loadFromMapRDB("/user/mapr/tables/total_counts")
          .map(d => (d.getString("_id"), d))

        val joint = rdd.join(fromDB)

        joint
          .map { jointPair =>
            val docTotal = Try {
              jointPair._2._2.getString("total").toInt
            }.getOrElse(0)

            val add = jointPair._2._1

            jointPair._2._2.set("total", (docTotal + add).toString)
          }
          .saveToMapRDB("/user/mapr/tables/total_counts")


        val paths = joint.map(_._1).collect().toSet

        rdd
          .filter(p => !paths.contains(p._1))
          .map(p => MapRDB.newDocument().set("_id", p._1).set("total", p._2))
          .saveToMapRDB("/user/mapr/tables/total_counts")
      }

    ssc.start()
    ssc.awaitTermination()
  }
}

