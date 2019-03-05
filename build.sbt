name := "spark-nnd"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.1" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.2.1" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.2.1" % "provided"
