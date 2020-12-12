name := "spark-report-lab"

version := "0.1"

scalaVersion := "2.11.11"

//logLevel := Level.Error

libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % "2.3.1")
libraryDependencies ++= Seq("org.apache.spark" %% "spark-sql" % "2.3.1")