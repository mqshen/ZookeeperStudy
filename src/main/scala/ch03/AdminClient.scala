package ch03

import java.util.Date

import org.apache.zookeeper.KeeperException.{NoNodeException, ConnectionLossException, NodeExistsException}
import org.apache.zookeeper.ZooDefs.Ids
import org.apache.zookeeper.data.Stat
import org.apache.zookeeper.{CreateMode, WatchedEvent, ZooKeeper, Watcher}
import util.Logging
import scala.collection.JavaConversions._
import scala.util.Random

/**
 * Created by goldratio on 3/12/15.
 */
class AdminClient(hostPort: String) extends Watcher with Logging {

  val zk = new ZooKeeper(hostPort, 150000, this)

  val serverId = Integer.toHexString(Random.nextInt())

  override def process(watchedEvent: WatchedEvent): Unit = {
    print(watchedEvent)
  }

  def listState() = {
    try {
      val stat = new Stat()
      val masterData = zk.getData("/master", false, stat)
      val startDate = new Date(stat.getCtime())
      logInfo("Master: " + new String(masterData) + " since " + startDate)
    }
    catch  {
      case e: NoNodeException =>
        logError("notMaster")
    }
    logInfo("Workers:")
    zk.getChildren("/workers", false).toList.foreach { worker =>
      val data = zk.getData("/workers/" + worker, false, null)
      val state = new String(data)
      logInfo("\t" + worker + ": " + state)
    }
    logInfo("Tasks:")
    zk.getChildren("/assign", false).toList.foreach { assign =>
      logInfo("\t" + assign)
    }

  }

  def stop(): Unit = {
    zk.close()
  }
}

object AdminClient extends App {
  val worker = new AdminClient("101.251.195.186:2181,101.251.195.187:2181,101.251.195.188:2181")
  Thread.sleep(3000)
  worker.listState()
  Thread.sleep(300000)
  worker.stop()
}