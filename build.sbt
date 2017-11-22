name := "Antuit_ingestion"
version := "2.0"
scalaVersion := "2.11.8"
val sparkVersion = "2.1.1"
resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
)
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "com.microsoft.sqlserver" % "mssql-jdbc" % "6.1.0.jre8",
  "com.typesafe" % "config" % "1.3.1",
  "com.databricks" % "spark-csv" % sparkVersion
)

