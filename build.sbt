name := "SparkRDF"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
	"org.apache.spark" %% "spark-core" % "2.0.1" % "provided",
	"org.apache.spark" %% "spark-graphx" % "2.0.1" % "provided",
	"org.apache.jena" % "jena-arq" % "2.13.0"
)

assemblyMergeStrategy in assembly := {
	case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
	case _ => MergeStrategy.last
}

assemblyJarName in assembly := "sparkrdf-" + version.value + ".jar"