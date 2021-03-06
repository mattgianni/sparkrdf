package org.gianni.sparkrdf

import org.apache.jena.riot.Lang
import org.apache.jena.riot.RiotReader
import java.io.InputStream

/** Static RDF related utilities. */
object RDF {

  class StringInputStream(s: String) extends InputStream {
    private val bytes = s.getBytes

    private var pos = 0

    override def read(): Int =
      if (pos >= bytes.length) {
        -1
      } else {
        val r = bytes(pos)
        pos += 1
        r.toInt
      }
  }

  /**
   * Removes the first and last characters from a string.
   * If the input string doesn't have at least 2 characters, the original
   * string is returned. This utility is usually used to remove the bracketing
   * characters from an RDF object. In most cases this are the &lt; or &gt;
   * characters that surround RDF URIs.
   *
   * {{{
   * scala> val meat = RDF.stripPerimeter("&lt;http://xmlns.com/foaf/0.1/knows&gt;")
   * meat: String = http://xmlns.com/foaf/0.1/knows
   * }}}
   *
   * @param item the input string
   * @return the inner portion of the string, with the "outside" characters removed.
   */
  def stripPerimeter(item: String): String = if (item.length >= 2) item.substring(1, item.length - 1) else item

  /**
   * Removes the line terminator from an [[http://www.w3.org/TR/n-triples/ N-Triples]] line.
   * @param line the input line.
   * @return the n-triple with the terminating space-dot removed.
   */
  def stripTerminus(line: String): String =
    if (line.length < 2 || line.takeRight(2) != " .") line else line.take(line.length - 2)

  // TODO: creating an iterator for each line seems wasteful.
  def parseTriple(line: String) = {
    val trip = RiotReader.createIteratorTriples(new StringInputStream(line), Lang.NTRIPLES, "").next
    (trip.getSubject.toString,
      trip.getPredicate.toString,
      if (trip.getObject.isLiteral) trip.getObject.getLiteralValue.toString else trip.getObject.toString)
  }

  def parseQuad(line: String) = {
    try {
      val quad = RiotReader.createIteratorQuads(new StringInputStream(line), Lang.NQUADS, "").next
      val sub = quad.getSubject.toString
      val pred = quad.getPredicate.toString
      val obj = if (quad.getObject.isLiteral) quad.getObject.getLiteralValue.toString else quad.getObject.toString
      val graph = quad.getGraph.toString
      (sub, pred, obj, graph, true, "")
    } catch {
      case e: Throwable => { ("sub", "pred", "obj", line, false, e.getMessage) }
    }
  }

  def hardParseTriple(line: String) = {
    try {
      val ss = 0
      val se = line.indexOf(62) + 1

      val ps = se + 1
      val pe = line.indexOf(62, ps) + 1

      val oe = line.lastIndexOf(32)

      val os = pe + 1

      val sub = line.substring(ss, se)
      val pred = line.substring(ps, pe)
      val obj = line.substring(os, oe)

      (sub, pred, obj)
    } catch {
      case e: Throwable => ("error", "error", e.getMessage)
    }
  }

  def hardParseQuad(line: String) = {
    try {
      val ss = 0
      val se = line.indexOf(32)

      val ps = se + 1
      val pe = line.indexOf(32, ps)

      val ge = line.lastIndexOf(32)
      val gs = line.lastIndexOf(32, ge - 1) + 1

      val os = pe + 1
      val oe = gs - 1

      val sub = line.substring(ss, se)
      val pred = line.substring(ps, pe)
      val obj = line.substring(os, oe)
      val graph = line.substring(gs, ge)

      (sub, pred, obj, graph)
    } catch {
      case e: Throwable => ("error", "error", line, e.getMessage)
    }
  }
}