package com.github.anicolaspp.generalstats.ops

import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

object Ops {
  def dbSchema =
    StructType(List(StructField("_id", DataTypes.StringType), StructField("total", DataTypes.IntegerType)))

  def streamSchema =
    StructType(List(StructField("_id", DataTypes.StringType), StructField("sum", DataTypes.IntegerType)))

  def idCol = "_id"

  def pathCol = "path"

  def valueCol = "value"

  def totalCol = "total"

  def sumCol = "sum"

  def left_outer_join = "left_outer"
}