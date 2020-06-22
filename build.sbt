name := "SparkInAction2-Chapter17"

version := "1.0.0"

scalaVersion := "2.12.10"

val sparkVersion = "3.0.0"

resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
)

libraryDependencies ++= Seq(
  "org.apache.spark"  %% "spark-core"            % sparkVersion,
  "org.apache.spark"  %% "spark-sql"             % sparkVersion,
  "io.delta"          %% "delta-core"            % "0.7.0",
  "org.postgresql"    %  "postgresql"            % "42.1.4",
  "org.elasticsearch" %  "elasticsearch-hadoop"  % "7.4.0"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}
