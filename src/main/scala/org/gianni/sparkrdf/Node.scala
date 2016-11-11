package org.gianni.sparkrdf

// TODO: there should be different node classes based on type. 

/*  
:load ../src/main/scala/org/gianni/sparkrdf/RDF.scala
:load ../src/main/scala/org/gianni/sparkrdf/Node.scala
:load ../src/main/scala/org/gianni/sparkrdf/Triple.scala
*/

/** A class to represent an RDF node.
 *  This includes the datatype, value and language.
 *  
 *  @constructor creates a new [[Node]] from a datatype, value and language.
 *  @param dt the datatype as a string.
 *  @param value the value as a string, if the dt is uri this will be an unbracketed string.
 *  @param lang the language tag from the node.
 *  @todo the datatype should really be something like an enumeration ... just lazy.
 */
class Node(val dt: String, val value: String, val lang: String) extends Serializable {  
  override def toString: String = dt match {
    case "string" => if (lang != null) "%s[%s]".format(value, lang) else value
    case "uri" => "<" + value + ">"
    case _ => value
  }
}

/** Static factory for generating [[Node]] objects. */ 
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

