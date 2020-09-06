package io.keepcoding.exercise2

import org.scalatest.{FlatSpec, MustMatchers}

class TreeTest extends FlatSpec with MustMatchers {

  import TreeTest._

  behavior of "Tree - InOrder"

  it must "works with sample 1" in {
    Tree.inOrder(tree1) must be (List(4, 2, 5, 1, 3))
  }

  it must "works with sample 2" in {
    Tree.inOrder(tree2) must be (List(6, 4, 1, 3, 0, 5, 9, 2, 11, 7, 10))
  }

  it must "works with sample 3" in {
    Tree.inOrder(tree3) must be (List(3, 5, 6, 7, 7, 5, 6, 8, 9, 10))
  }
}

object TreeTest {

  val tree1: Tree = Node(Node(Node(Leaf, 4, Leaf), 2, Node(Leaf, 5, Leaf)), 1, Node(Leaf, 3, Leaf))
  val tree2: Tree = Node(Node(Node(Leaf, 6, Leaf), 4, Node(Node(Leaf, 1, Leaf), 3, Node(Leaf, 0, Leaf))), 5,
    Node(Node(Leaf, 9, Leaf), 2, Node(Node(Leaf, 11, Leaf), 7, Node(Leaf, 10, Leaf))))
  val tree3: Tree = Node(Node(Node(Leaf, 3, Leaf), 5, Leaf), 6, Node(Node(Leaf, 7, Leaf), 7,
    Node(Node(Node(Node(Leaf, 5, Leaf), 6, Leaf), 8, Leaf), 9, Node(Leaf, 10, Leaf))))

}