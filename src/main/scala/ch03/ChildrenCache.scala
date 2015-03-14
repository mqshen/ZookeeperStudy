package ch03

import scala.collection.mutable.ArrayBuffer

/**
 * Created by goldratio on 3/13/15.
 */
class ChildrenCache {
  val children = new scala.collection.mutable.ArrayBuffer[String]()

  def addedAndSet(newChildren: Seq[String]): Seq[String] = {

    if(children.isEmpty) {
      children ++ newChildren
      children
    }
    else {
      val diff = new scala.collection.mutable.ArrayBuffer[String]()
      newChildren.foldLeft(diff){ case (diff, s) =>
        if (!children.contains(s)) {
          diff ++ s
        }
        diff
      }
      this.children.clear()
      this.children ++= newChildren
      diff
    }
  }

  def removedAndSet(newChildren: Seq[String]): Seq[String] = {
    val diff = new scala.collection.mutable.ArrayBuffer[String]()
    children.foldLeft(diff) { case (diff, s) =>
      if(!newChildren.contains( s )) {
        diff ++ s
      }
      diff
    }
    this.children.clear()
    this.children ++= newChildren
    diff
  }

  def size = this.children.size

  def get(i: Int) = this.children(i)

}
