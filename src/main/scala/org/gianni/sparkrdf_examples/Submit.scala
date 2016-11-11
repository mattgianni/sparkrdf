package org.gianni.sparkrdf_examples

import java.io.ByteArrayInputStream
import org.apache.jena.riot.Lang
import org.apache.jena.riot.RiotReader
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.Graph.graphToGraphOps
import org.apache.spark.graphx.VertexId
import org.apache.spark.rdd.RDD
import com.hp.hpl.jena.rdf.model.Model
import com.hp.hpl.jena.rdf.model.ModelFactory
import org.gianni.sparkrdf._

object Submit {

  private val conf = new SparkConf().setMaster("local[*]").setAppName("SparkRDF")
  private val sc: SparkContext = new SparkContext(conf)

  // val fn = "/Users/mattg/Projects/Spark/sparkrdf/data/nyse.nt"
  private val fn = "/Users/mattg/Projects/Spark/sparkrdf/data/bio2rdf_10K.nq"

  def parseTriple(line: String) = {
    val trip = RiotReader.createIteratorTriples(new ByteArrayInputStream(line.getBytes), Lang.NTRIPLES, "").next
    (trip.getSubject.toString, trip.getPredicate.toString, if (trip.getObject.isLiteral) trip.getObject.getLiteralValue.toString else trip.getObject.toString)
  }

  def splitURI(uri: String): (String, String) = {
    val i = scala.math.max(uri.lastIndexOf('/'), uri.lastIndexOf('#'))
    (uri.take(i + 1), uri.takeRight(uri.length - i - 1))
  }
  
  /** Removes the trailing terminator from an n-triple string.
   * @param line an n-triple formatted string
   * @return the n-triple string without the trailing terminator
   */
  def stripTerminus(line: String): String = 
    if (line.length < 2 || line.takeRight(2) != " .") line else line.take(line.length - 2)
  
  def stripPerimeter(item: String): String = if (item.length >= 2) item.substring(1, item.length - 1) else item

 //  case class Node(dt: String, value: String)
//  def node_parse(item: String): Node = item match {
//    case x if (x.startsWith("<") && x.endsWith(">")) => Node("uri", stripPerimeter(x))
//    case x if (x.startsWith("\"") && x.endsWith("\"")) => Node("string", stripPerimeter(x))
//    case x if (x.startsWith("_:")) => Node("blank", x.substring(2))
//    case _ => Node("error", "error")
//  }
  
  def play(text: RDD[String]) = {
    text.map({
      case x => {
        val p = x.split("\\s+", 3)
        (p(0), p(1), p(2))
      }
    })
  }

  def main(args: Array[String]): Unit = {

    println("Welcome to SparkRDF")
/*

:load ../src/main/scala/org/gianni/sparkrdf/RDF.scala
:load ../src/main/scala/org/gianni/sparkrdf/Node.scala
:load ../src/main/scala/org/gianni/sparkrdf/Triple.scala

 */
    val fn = "/Users/mattg/Projects/Spark/sparkrdf/data/nyse.nt"
    val text = sc.textFile(fn)
    
    text.map( x => Triple(x) )
    

    
    val rs = text.map(parseTriple)

    val vmap = (rs.map(_._1) union rs.map(_._3)).distinct.zipWithIndex
    val vertices: RDD[(VertexId, String)] = vmap.map(x => (x._2, x._1))

    val p1 = rs.keyBy(_._1).join(vmap).map({
      case (k, ((s, p, o), si)) => (o, (si, p))
    })

    val edges: RDD[Edge[String]] = p1.join(vmap).map({
      case (k, ((si, p), oi)) => Edge(si, oi, p)
    })

    val graph = Graph(vertices, edges, "**ERROR**")

    val pr = graph.pageRank(0.00001).vertices
    val report = (pr join vertices).map({ case (k, (r, v)) => (r, v, k) }).sortBy(100 - _._1)

    report.take(100).foreach(println)

    println("Exitting.")

  }

  def readModel: Model = {
    val model = ModelFactory.createDefaultModel()
    model.read(fn)
    model
  }

}

