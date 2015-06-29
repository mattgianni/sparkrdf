package org.gianni.sparkrdf

/* 
 
:load ../src/main/scala/org/gianni/sparkrdf/RDF.scala
:load ../src/main/scala/org/gianni/sparkrdf/Node.scala
:load ../src/main/scala/org/gianni/sparkrdf/Triple.scala

*/
class Node(val dt: String, val value: String, val lang: String) extends Serializable {  
  override def toString: String = dt match {
    case "string" => if (lang != null) "%s[%s]".format(value, lang) else value
    case "uri" => "<" + value + ">"
    case _ => value
  }
}

object Node {
  def apply(item: String) = {
    item.trim match {
      case x if (x.startsWith("<") && x.endsWith(">")) => new Node("uri", RDF.stripPerimeter(x), null)
      case x if (x.startsWith("\"") && x.endsWith("\"")) => new Node("string", RDF.stripPerimeter(x), null)
      case x if (x.startsWith("_:")) => new Node("blank", x.substring(2), null)
      case x => new Node("error", "error", "error")
    }
  }
}

