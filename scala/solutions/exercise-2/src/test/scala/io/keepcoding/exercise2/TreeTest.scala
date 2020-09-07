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

  behavior of "Tree - PreOrder"

  it must "works with sample 1" in {
    Tree.preOrder(tree1) must be (List(1, 2, 4, 5, 3))
  }

  it must "works with sample 2" in {
    Tree.preOrder(tree2) must be (List(5, 4, 6, 3, 1, 0, 2, 9, 7, 11, 10))
  }

  it must "works with sample 3" in {
    Tree.preOrder(tree3) must be (List(6, 5, 3, 7, 7, 9, 8, 6, 5, 10))
  }

  behavior of "Tree - PostOrder"

  it must "works with sample 1" in {
    Tree.postOrder(tree1) must be (List(4, 5, 2, 3, 1))
  }

  it must "works with sample 2" in {
    Tree.postOrder(tree2) must be (List(6, 1, 0, 3, 4, 9, 11, 10, 7, 2, 5))
  }

  it must "works with sample 3" in {
    Tree.postOrder(tree3) must be (List(3, 5, 7, 5, 6, 8, 10, 9, 7, 6))
  }
}

object TreeTest {

  val tree1: Tree = Node(Node(Node(Leaf, 4, Leaf), 2, Node(Leaf, 5, Leaf)), 1, Node(Leaf, 3, Leaf))
  val tree2: Tree = Node(Node(Node(Leaf, 6, Leaf), 4, Node(Node(Leaf, 1, Leaf), 3, Node(Leaf, 0, Leaf))), 5,
    Node(Node(Leaf, 9, Leaf), 2, Node(Node(Leaf, 11, Leaf), 7, Node(Leaf, 10, Leaf))))
  val tree3: Tree = Node(Node(Node(Leaf, 3, Leaf), 5, Leaf), 6, Node(Node(Leaf, 7, Leaf), 7,
    Node(Node(Node(Node(Leaf, 5, Leaf), 6, Leaf), 8, Leaf), 9, Node(Leaf, 10, Leaf))))

}