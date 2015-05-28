name := "SparkRDF"

version := "0.1"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
	"org.apache.spark" %% "spark-core" % "1.2.0" % "provided",
	"org.apache.spark" %% "spark-graphx" % "1.2.0" % "provided",
	"org.apache.jena" % "jena-arq" % "2.13.0"
)

mergeStrategy in assembly := {
	case n if n.startsWith("META-INF/MANIFEST.MF") => MergeStrategy.discard
	case _ => MergeStrategy.last
}

assemblyJarName in assembly := "sparkrdf-" + version.value + ".jar"