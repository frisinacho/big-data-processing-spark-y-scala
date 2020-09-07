package io.keepcoding.exercise5

object Implicits {

  /* Reference:
    class ExternalClass {
      private val list: List[String] = List(.....)
      def getSize(): Int = list.size
    }

    implicit class EnrichExternalClass(ex: ExternalClass){
      def isEmpty: Boolean =ex.getSize == 0
    }
  */

  // Target: "Hello world!".toPassword == "H3ll0 w0rld!"

  implicit class PimpMyString(s: String) {

    def toPassword =
      s.replace('a', '4')
        .replace('e', '3')
        .replace('i', '1')
        .replace('o', '0')
  }

}
