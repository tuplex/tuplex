name := "ZillowScala"

version := "0.1"
organization := "L. Spiegelberg"

scalaVersion := "2.11.12"

// https://mvnrepository.com/artifact/com.fasterxml.jackson.dataformat/jackson-dataformat-csv
libraryDependencies += "com.fasterxml.jackson.dataformat" % "jackson-dataformat-csv" % "2.12.1"


resolvers += Resolver.mavenLocal

assemblyMergeStrategy in assembly := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}
