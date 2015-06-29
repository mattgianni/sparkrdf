package org.gianni.sparkrdf_examples

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.EdgeDirection
import org.apache.spark.graphx.Graph.graphToGraphOps
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.graphx.VertexId
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.EdgeTriplet
import java.io._
import scala.Iterator

object Pregel_SSSP {
  val conf = new SparkConf().setMaster("local[*]").setAppName("SparkRDF")
  val sc: SparkContext = new SparkContext(conf)
  
  def t2s(triple: EdgeTriplet[_, _]) = {
    val v1 = triple.srcId.toString
    val a1 = triple.srcAttr.toString
    val v2 = triple.dstId.toString
    val a2 = triple.dstAttr.toString
    val et = triple.attr.toString
    val t = "\t"
    val s = " "
    v1 + s + a1 + t + et + t + v1 + s + a2
  }

  def writeSif(graph: Graph[_, _], fn: String) = {
    // val report = graph.triplets.map(e => e.srcId.toString + " " + e.attr.toString + " " + e.dstId.toString).collect
    val report = graph.triplets.map(t2s).collect
    val naked_verts = graph.vertices.map({ case (vid, at) => vid.toString + " " + at }).collect
    val pw = new PrintWriter(new File(fn))
    report.foreach(x => pw.println(x))
    naked_verts.foreach(x => pw.println(x))
    pw.close
  }

  def test = {

    val codes: RDD[(VertexId, String)] =
      sc.parallelize("ABCDEFGHI".map(_.toString).zipWithIndex.map(x => (x._2.toLong, x._1)))

    val rel: RDD[Edge[String]] =
      sc.parallelize(Array(
        Edge(0L, 1L, ":knows"), Edge(1L, 2L, ":knows"), Edge(1L, 3L, ":knows"), Edge(3L, 6L, ":knows"),
        Edge(6L, 1L, ":knows"), Edge(2L, 5L, ":knows"), Edge(2L, 4L, ":knows"), Edge(6L, 4L, ":knows")))

    val default = "*"
    val graph = Graph(codes, rel, default)
  }

  def vprog(id: VertexId, dist: Double, newDist: Double) = math.min(dist, newDist)

  def compute(triplet: EdgeTriplet[Double, String]) =
    if (triplet.srcAttr + 1.0 < triplet.dstAttr) Iterator((triplet.dstId, triplet.srcAttr + 1.0)) else Iterator.empty

  def merge(a: Double, b: Double) = math.min(a, b)

  def main(args: Array[String]) {
    val fn = "/Users/mattg/Projects/Spark/sparkrdf/tmp/out.sif"

    // A graph with edge attributes containing distances
    val graph = GraphGenerators.rmatGraph(sc, 100, 150).mapEdges(e => e.attr.toDouble)

    graph.edges.foreach(println)
    val sourceId: VertexId = 0 // The ultimate source

    // Initialize the graph such that all vertices except the root have distance infinity.
    val initialGraph = graph.mapVertices((id, _) => if (id == sourceId) 0.0 else Double.PositiveInfinity)

    val sssp = initialGraph.pregel(Double.PositiveInfinity, Int.MaxValue, EdgeDirection.Out)(

      // Vertex Program
      (id, dist, newDist) => math.min(dist, newDist),

      // Send Message
      triplet => {
        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
        } else {
          Iterator.empty
        }
      },

      //Merge Message
      (a, b) => math.min(a, b))

    println(sssp.vertices.collect.mkString("\n"))
  }
}