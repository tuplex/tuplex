name := "Flight query verify"

version := "1.0"

// Note: Spark 2.4.5 is only Scala 2.11.12 compatible. Need to use that version.
scalaVersion := "2.11.12"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.5"