package org.gianni.sparkrdf

class Triple(val s: Node, val p: Node, val o: Node) extends Serializable {
  override def toString = s + " " + p + " " + o
}

/** Factory method for [[Triple]] instances. */
object Triple {
  def apply(nt: String) = {
    val parts = RDF.stripTerminus(nt).split("\\s+", 3)
    new Triple(Node(parts(0)), Node(parts(1)), Node(parts(2)))
  }
}