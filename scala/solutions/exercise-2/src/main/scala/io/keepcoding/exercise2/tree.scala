package io.keepcoding.exercise2

sealed trait Tree
case object Leaf extends Tree
case class Node(left: Tree, value: Int, right: Tree) extends Tree


object Tree {

  def inOrder(node: Tree): List[Int] = node match {
    case Leaf => List.empty
    case Node(left, value, right) => inOrder(left) ::: List(value) ::: inOrder(right)
  }

  def preOrder(node: Tree): List[Int] = node match {
    case Leaf => List.empty
    case Node(left, value, right) => List(value) ::: preOrder(left) :::  preOrder(right)
  }

  def postOrder(node: Tree): List[Int] = node match {
    case Leaf => List.empty
    case Node(left, value, right) =>  postOrder(left) :::  postOrder(right) ::: List(value)
  }

}
