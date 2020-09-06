package io.keepcoding.exercise2

sealed trait Tree
case object Leaf extends Tree
case class Node(left: Tree, value: Int, right: Tree) extends Tree


object Tree {

  def inOrder(node: Tree): List[Int] = ???

}
