name := "IdsToStix"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.2.1"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.2.1"

libraryDependencies += "org.mongodb.spark" % "mongo-spark-connector_2.11" % "2.3.1"

assemblyMergeStrategy in assembly := {
  {
    case "META-INF/services/org.apache.spark.sql.sources.DataSourceRegister" => MergeStrategy.concat
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case x => MergeStrategy.first
  }
}