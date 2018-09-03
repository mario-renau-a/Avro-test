name := "AvroTest"

version := "0.1"

scalaVersion := "2.11.6"

libraryDependencies ++= Seq(

  "org.apache.avro" % "avro" % "1.8.2",
  "org.apache.avro" % "avro-compiler" % "1.8.2",
  "org.apache.spark" %% "spark-sql" % "2.3.1",
  "com.databricks" %% "spark-avro" % "4.0.0",
  "org.scalatest" %% "scalatest" % "2.2.1" % "test",
  // Kryo is provided by Spark, but we need this here in order to be able to import the @DefaultSerializer annotation:
  "com.esotericsoftware" % "kryo-shaded" % "3.0.3" % "provided")

//"org.apache.avro" % "avro-maven-plugin" % "1.8.2")


