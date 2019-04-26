name := "kafka"

version := "0.1"

scalaVersion := "2.11.12"

val sparkVersion = "2.4.1"


lazy val root = (project in file(".")).
  settings(
    name := "kafka",
    version := "1.0",
    scalaVersion := "2.11.12",
    mainClass in Compile := Some("kafka")
  )


libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-library" % "2.11.12" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.4.1" % "provided",
  //"org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.1" % "provided",
  //"org.apache.kafka" %% "kafka-clients" % "2.4.0",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.4.1" % "provided",
  "org.apache.bahir" %% "spark-streaming-twitter" % "2.3.2",
//  "org.apache.bahir" %% "spark-streaming-twitter" % "2.0.0",
  "org.apache.spark" %% "spark-streaming" % "2.4.1" % "provided",
//  "org.apache.bahir" %% "spark-streaming-twitter" % "2.4.0-SNAPSHOT",
  //"org.apache.spark" %% "kafka-clients" % "0.11.0.1"
  //"org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.1",
  "edu.stanford.nlp" % "stanford-corenlp" % "3.5.2" artifacts (Artifact("stanford-corenlp", "models"), Artifact("stanford-corenlp"))
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}