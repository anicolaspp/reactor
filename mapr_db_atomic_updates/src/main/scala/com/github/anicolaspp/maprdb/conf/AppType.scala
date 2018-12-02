package com.github.anicolaspp.maprdb.conf

sealed trait AppType

case object Gen extends AppType
case object Transact extends AppType
case object Undefined extends AppType
