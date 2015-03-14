package ch03

import org.apache.zookeeper.KeeperException.{ NodeExistsException, ConnectionLossException, NoNodeException }
import org.apache.zookeeper.ZooDefs.Ids
import org.apache.zookeeper._
import org.apache.zookeeper.data.Stat

import scala.util.Random

/**
 * Created by goldratio on 3/11/15.
 */
class Master(hostPort: String) extends Watcher {

  val zk = new ZooKeeper(hostPort, 150000, this)

  val serverId = Integer.toHexString(Random.nextInt())

  override def process(watchedEvent: WatchedEvent): Unit = {
    print(watchedEvent)
  }

  def runForMaster(): Boolean = {
    try {
      zk.create("/master", serverId.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
      true
    } catch {
      case e: NodeExistsException =>
        false
      case e: ConnectionLossException =>
        val isLeader = checkMaster()
        if (isLeader)
          true
        else
          runForMaster()
    }
  }

  def checkMaster(): Boolean = {
    try {
      val stat = new Stat()
      val data = zk.getData("/master", false, stat)
      if (new String(data) == serverId)
        true
      else
        false
    } catch {
      case e: NoNodeException =>
        false
      case e: ConnectionLossException =>
        checkMaster()
    }
  }

  def stop(): Unit = {
    zk.close()
  }

}

object Master extends App {

  val master = new Master("101.251.195.186:2181,101.251.195.187:2181,101.251.195.188:2181")
  //val master = new Master("101.251.195.186:2181")
  val isLeader = master.runForMaster()
  if (isLeader) {
    println("I'm the leader")
    Thread.sleep(600000)
  } else {
    println("Someone else is the leader")
  }
  master.stop()

}
