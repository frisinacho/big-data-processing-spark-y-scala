package io.keepcoding.exercise5

import org.scalatest.{FlatSpec, MustMatchers}

class ImplicitsTest extends FlatSpec with MustMatchers {

  behavior of "exercise 5"

  it must "get password from string" in {

    import Implicits._

    "Hello World!".toPassword must be("H3ll0 W0rld!")
    //fail("Pending to be implemented. Uncomment previous line and make it works!")
  }
}
