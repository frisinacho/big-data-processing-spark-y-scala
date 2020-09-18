package io.keepcoding.exercise2

import org.scalatest.{FlatSpec, MustMatchers}

class MathsTest extends FlatSpec with MustMatchers {

  behavior of "Fibonacci"

  it must "works with 0" in {
    Maths.fibonacci(0) must be (0)
  }

  it must "works with 1" in {
    Maths.fibonacci(1) must be (1)
  }

  it must "works with 8" in {
    Maths.fibonacci(8) must be (21)
  }

  it must "works with 30" in {
    Maths.fibonacci(30) must be (832040)
  }

  it must "throw an exception if the number is less than 0" in {
    an [RuntimeException] should be thrownBy Maths.fibonacci(-1)
  }

}
