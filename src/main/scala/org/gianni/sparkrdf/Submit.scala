package org.gianni.sparkrdf

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.jena.riot._
import com.hp.hpl.jena.rdf.model.Model
import com.hp.hpl.jena.rdf.model.ModelFactory
import com.hp.hpl.jena.rdf.model.Statement
import com.hp.hpl.jena.graph.Triple
import java.io.InputStream
import org.apache.spark._
import org.apache.spark.graphx._

class StringInputStream(s: String) extends InputStream {
  private val bytes = s.getBytes

  private var pos = 0

  override def read(): Int = if (pos >= bytes.length) {
    -1
  } else {
    val r = bytes(pos)
    pos += 1
    r.toInt
  }
}

object Submit {

  val conf = new SparkConf().setMaster("local[*]").setAppName("SparkRDF")
  val sc: SparkContext = new SparkContext(conf)

  val fn = "/Users/mattg/Projects/Spark/sparkrdf/data/nyse.nt"

  def parseTriple(line: String) = {
    val trip = RiotReader.createIteratorTriples(new StringInputStream(line), Lang.NTRIPLES, "").next
    (trip.getSubject.toString, trip.getPredicate.toString, if (trip.getObject.isLiteral) trip.getObject.getLiteralValue.toString else trip.getObject.toString)
  }

  def main(args: Array[String]): Unit = {

    println("Welcome to SparkRDF")

    val text = sc.textFile(fn)
    val rs = text.map(parseTriple)
    
    val vmap = (rs.map(_._1) union rs.map(_._3)).distinct.zipWithIndex
    val vertices: RDD[(VertexId, String)] = vmap.map(x => (x._2, x._1))
    
    val p1 = rs.keyBy(_._1).join(vmap).map({
      case (k, ((s, p, o), si)) => (o, (si, p))
    })
    
    val edges:RDD[Edge[String]] = p1.join(vmap).map({
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

