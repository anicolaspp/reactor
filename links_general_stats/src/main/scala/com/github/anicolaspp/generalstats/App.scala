package com.github.anicolaspp.generalstats

import com.github.anicolaspp.generalstats.data.Streamer
import com.mapr.db.spark.{MapRDBSpark, _}
import org.apache.spark.sql._
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import org.apache.spark.sql.SparkSession


object App extends Streamer {

  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setAppName("general stats")

    val sc = new SparkContext(config)
    implicit val ssc: StreamingContext = new StreamingContext(sc, Milliseconds(500))

    val messages = getStream("/user/mapr/streams/click_stream:all_links_2")

    val sparkSession = SparkSession.builder().getOrCreate()

    messages
      .map(_.value())
      .map(MapRDBSpark.newDocument)
      .map(doc => (doc.getString("path"), 1))
      .reduceByKey(_ + _)

      .foreachRDD { rdd =>
        val m = rdd.map { case (path, sum) => Row(path, sum) }

        val df = sparkSession.createDataFrame(m, StructType(List(StructField("_id", DataTypes.StringType), StructField("sum", DataTypes.IntegerType))))

        println("stream")
        df.show()

        val fromDBRDD = sc
          .loadFromMapRDB("/user/mapr/tables/total_counts")
          .map(d => (d.getString("_id"), d.getInt("total")))
          .map { case (path, sum) => Row(path, sum) }

        val fromDBDF = sparkSession.createDataFrame(fromDBRDD, StructType(List(StructField("_id", DataTypes.StringType), StructField("total", DataTypes.IntegerType))))
          


        val join = df.join(fromDBDF, Seq("_id"), "left_outer").na.fill(0, Seq("total"))

//        join.show()

        val finalDF = join.withColumn("value", join("sum") + join("total"))

        println("final")
        finalDF.show()

        finalDF.sa
      }
    //      .map { case (path, sum) =>
    //
    //        val fromDB = sc
    //          .loadFromMapRDB("/user/mapr/tables/total_counts")
    //          .map(d => (d.getString("_id"), d))
    //          .filter(_._1 == path)
    //
    //        val valueOnDB = fromDB.map { case (_, doc) =>
    //          Try {
    //            doc.getString("total").toInt
    //          }.getOrElse(0)
    //        }.reduce(_ + _)
    //
    //        (path, sum + valueOnDB)
    //      }
    //      .map { case (path, finalSum) => MapRDB.newDocument().set("_id", path).set("total", finalSum) }
    //      .foreachRDD(_.saveToMapRDB("/user/mapr/tables/total_counts"))


    //      .foreachRDD { rdd =>
    //
    //        val fromDB = sc
    //          .loadFromMapRDB("/user/mapr/tables/total_counts")
    //          .map(d => (d.getString("_id"), d))
    //
    //        val joint = rdd.join(fromDB)
    //
    //        joint
    //          .map { jointPair =>
    //            val docTotal = Try {
    //              jointPair._2._2.getString("total").toInt
    //            }.getOrElse(0)
    //
    //            val add = jointPair._2._1
    //
    //            jointPair._2._2.set("total", (docTotal + add).toString)
    //          }
    //          .saveToMapRDB("/user/mapr/tables/total_counts")
    //
    //
    //        val paths = joint.map(_._1).collect().toSet
    //
    //        rdd
    //          .filter(p => !paths.contains(p._1))
    //          .map(p => MapRDB.newDocument().set("_id", p._1).set("total", p._2))
    //          .saveToMapRDB("/user/mapr/tables/total_counts")
    //      }

    ssc.start()
    ssc.awaitTermination()
  }

}

