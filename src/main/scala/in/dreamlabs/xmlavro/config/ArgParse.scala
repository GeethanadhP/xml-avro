package in.dreamlabs.xmlavro.config

import javax.xml.namespace.QName

import scala.collection.immutable.List
import scala.collection.mutable
import scala.reflect.io.Path
import scala.reflect.runtime.universe._

/**
  * Created by Royce on 02/02/2017.
  */
class ArgParse(args: Seq[String]) {
  private val argsMap = {
    val map = mutable.Map[String, List[String]]()
    val len = args.length
    var i = 0
    while (i < len) {
      val arg = args(i)
      if (arg.startsWith("--") || (arg.startsWith("-") && arg.length == 2)) {
        val name = arg stripPrefix "-" stripPrefix "-"
        val values = mutable.ListBuffer[String]()
        while (i + 1 < len && !args(i + 1).startsWith("-")) {
          i += 1
          values += args(i)
        }
        map += (name -> values.toList)
      }
      i += 1
    }
    map
  }


  def opt[T: TypeTag](name: String, short: Char): Option[T] = {
    if (argsMap.contains(name) || argsMap.contains(short + "")) {
      val key = if (argsMap contains name) name else short + ""
      val values = argsMap(key)
      try
        Some(value[T](values))
      catch {
        case e: IllegalArgumentException => throw new IllegalArgumentException(s"${e.getMessage} for $key", e)
      }
    } else None
  }

  private def value[T: TypeTag](original: List[String]): T = {
    typeOf[T] match {
      case t if t =:= typeOf[String] => original.fetch().asInstanceOf[T]
      case t if t =:= typeOf[Int] => original.fetch().toInt.asInstanceOf[T]
      case t if t =:= typeOf[Double] => original.fetch().toDouble.asInstanceOf[T]
      case t if t =:= typeOf[Boolean] => original.fetch().toBoolean.asInstanceOf[T]
      case t if t =:= typeOf[Path] => Path(original.fetch()).asInstanceOf[T]
      case t if t =:= typeOf[List[String]] => original.validate().asInstanceOf[T]
      case t if t =:= typeOf[List[Int]] => original.validate().map(value => value.toInt).asInstanceOf[T]
      case t if t =:= typeOf[List[Double]] => original.validate().map(value => value.toDouble).asInstanceOf[T]
      case t if t =:= typeOf[List[Boolean]] => original.validate().map(value => value.toBoolean).asInstanceOf[T]
      case t if t =:= typeOf[List[Path]] => original.validate().map(value => Path(value)).asInstanceOf[T]
      case t if t =:= typeOf[QName] => QName.valueOf(original.fetch()).asInstanceOf[T]
      case other => throw new IllegalArgumentException(s"Type $other is not yet supported")
    }
  }
  def toggle(name: String, short: Char): Option[Boolean] = {
    if (argsMap.contains(name) || argsMap.contains(short + "")) {
      val key = if (argsMap contains name) name else short + ""
      val values = argsMap(key)
      if (values.nonEmpty)
        throw new IllegalArgumentException(s"Too many values provided for $key")
      else
        Some(true)
    } else None
  }

  implicit class MyList[String](list: List[String]) {
    def fetch(): String = {
      if (list.length > 1)
        throw new IllegalArgumentException(s"Too many values provided")
      else if (list.isEmpty)
        throw new IllegalArgumentException(s"Too less values provided")
      else list.head
    }

    def validate(): List[String] = {
      if (list.isEmpty)
        throw new IllegalArgumentException(s"Too less values provided")
      else list
    }
  }

}
