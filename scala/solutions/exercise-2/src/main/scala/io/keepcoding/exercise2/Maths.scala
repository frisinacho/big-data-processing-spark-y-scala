package io.keepcoding.exercise2

object Maths {

  def fibonacci(n: Int): Int = n match {
    case n if n < 0 => throw new IllegalArgumentException(s"Invalid input value $n")
    case 0 => 0
    case 1 => 1
    case _ => fibonacci(n - 1) + fibonacci(n - 2)
  }

}
