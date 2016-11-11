package org.gianni.sparkrdf

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

/** A collection of static utility functions associated with GraphX graphs. */
object GraphUtils {

  /**
   * Returns an XGMML representation of a GraphX graph.
   *
   * Returns an XGMML representation of a GraphX graph. This format is
   * supported by Cytoscape. 
   *
   * @param graph a GraphX graph of any type.
   * @param directed '''true''' if the graph is a directed graph.
   * @param id id of the graph.
   * @param label graph label.
   * @return XGMML representation of the GraphX graph.
   * @todo this version was reversed engineered from a sample XGMML file. Should be reviewed against
   * the documentation for the XGMML standard, assuming such a document exists.
   */
  def toXGMML(
      graph: Graph[_, _], 
      directed: Boolean = true, 
      id: Long = 0L, 
      label: String = "GraphX export") = {
    <graph
			id={ id.toString }
    	label={ label }
      directed={ if (directed) 1.toString else 0.toString }
      cy:documentVersion="3.0"
			xmlns:dc="http://purl.org/dc/elements/1.1/"
			xmlns:xlink="http://www.w3.org/1999/xlink"
			xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
			xmlns:cy="http://www.cytoscape.org"
			xmlns="http://www.cs.rpi.edu/XGMML">
      { graph.vertices.collect.map(v => <node id={ v._1.toString } label={ v._1.toString }><att name="val" type="string" value={ v._2.toString }/></node>) }
      { graph.edges.collect.map(e => <edge label={ e.attr.toString } source={ e.srcId.toString } target={ e.dstId.toString }></edge>) }
    </graph>
  }

  def saveGraph(graph: Graph[_, _], fn: String, directed: Boolean = true, id: Long = 0L, label: String = "GraphX export"): Unit = {
    val pw = new PrintWriter(new File(fn))
    pw.println("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>")
    pw.println(toXGMML(graph, directed, id, label))
    pw.close
  }

  def saveD3Json(graph: Graph[_, _], fn: String): Unit = {
    val pw = new PrintWriter(new File(fn))
    pw.println(toD3Json(graph))
    pw.close
  }

  def toD3Json(graph: Graph[_, _]): String = {
    val verts = graph.vertices.collect
    val edges = graph.edges.collect

    val nmap = verts.zipWithIndex.map(v => (v._1._1.toLong, v._2)).toMap

    val vtxt = verts.map(v =>
      "{\"val\":\"" + v._2.toString + "\"}").mkString(",\n")

    val etxt = edges.map(e =>
      "{\"source\":" + nmap(e.srcId).toString +
        ",\"target\":" + nmap(e.dstId).toString +
        ",\"value\":" + e.attr.toString + "}").mkString(",\n")

    "{ \"nodes\":[\n" + vtxt + "\n],\"links\":[" + etxt + "]\n}"
  }
}