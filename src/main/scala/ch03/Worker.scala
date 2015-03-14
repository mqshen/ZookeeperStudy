package ch03

import java.util.concurrent.{ConcurrentSkipListSet, Executors}

import org.apache.zookeeper.AsyncCallback.{DataCallback, ChildrenCallback, StatCallback, StringCallback}
import org.apache.zookeeper.KeeperException.Code
import org.apache.zookeeper.Watcher.Event.EventType
import org.apache.zookeeper._
import org.apache.zookeeper.ZooDefs.Ids
import org.apache.zookeeper.data.Stat
import util.Logging

import scala.collection.JavaConversions._
import scala.util.Random

/**
 * Created by goldratio on 3/12/15.
 */
class Worker(hostPort: String) extends Watcher with Logging {
  val zk = new ZooKeeper(hostPort, 15000, this)
  val serverId = Integer.toHexString(Random.nextInt())
  var status: String = null

  val executor = Executors.newScheduledThreadPool(5)

  val onGoingTasks = new ConcurrentSkipListSet[String]()

  override def process(watchedEvent: WatchedEvent): Unit = {
    logInfo(watchedEvent.toString() + ", " + hostPort)
  }

  val createWorkerCallback = new StringCallback {
    override def processResult(rc: Int, path: String, ctx: scala.Any, name: String): Unit = {
      Code.get(rc) match {
        case Code.CONNECTIONLOSS =>
          register()
        case Code.OK =>
          logInfo("Registered successfully: " + serverId)
        case Code.NODEEXISTS =>
          logWarning("Already registered: " + serverId)
        case _ =>
          logError("Something went wrong: " + KeeperException.create(Code.get(rc), path))
      }
    }
  }

  val newTaskWatcher = new Watcher {
    override def process(watchedEvent: WatchedEvent): Unit = {
      if(watchedEvent.getType() == EventType.NodeChildrenChanged) {
        assert(new String("/assign/worker-"+ serverId) == watchedEvent.getPath() )
        getTasks()
      }
    }
  }

  class TaskRunnable(tasks: Seq[String], cb: DataCallback) extends Runnable with Logging {
    override def run(): Unit = {
      logInfo("Looping into tasks")
      tasks.foreach { task =>
        if(!onGoingTasks.contains( task )) {
          logTrace(s"New task: $task")
          zk.getData(s"/assign/worker-$serverId/$task", false, cb, task)
          onGoingTasks.add( task )
        }
      }
    }
  }

  val taskDataCallback = new DataCallback {
    override def processResult(rc: Int, path: String, ctx: scala.Any, data: Array[Byte], stat: Stat): Unit = {
      Code.get(rc) match {
        case Code.CONNECTIONLOSS =>
          logInfo("connection lost!")
        case Code.OK =>
          logInfo("task run success!")
        case _ =>
          logError("Error when trying to get task data.", KeeperException.create(Code.get(rc), path))
      }
    }
  }

  val tasksGetChildrenCallback = new ChildrenCallback {
    override def processResult(rc: Int, path: String, ctx: scala.Any, tasks: java.util.List[String]): Unit = {
      Code.get(rc) match {
        case Code.CONNECTIONLOSS =>
          getTasks()
        case Code.OK =>
          if(tasks != null) {
            executor.execute(new TaskRunnable(tasks.toList, taskDataCallback))
          }
          logInfo("Registered successfully: " + serverId)
          logWarning("Already registered: " + serverId)
        case _ =>
          logError("getChildren failed: " + KeeperException.create(Code.get(rc), path))
      }
    }
  }

  def getTasks(): Unit = {
    zk.getChildren("/assign/worker-" + serverId, newTaskWatcher, tasksGetChildrenCallback, null)
  }


  val statusUpdateCallback = new StatCallback {
    override def processResult(rc: Int, path: String, ctx: scala.Any, stat: Stat): Unit = {
      Code.get(rc) match {
        case Code.CONNECTIONLOSS =>
          updateStatus(ctx.asInstanceOf[String])
      }
    }
  }

  def updateStatus(status: String): Unit = synchronized {
    if (status == this.status) {
      zk.setData("/workers/worker-" + serverId, status.getBytes(), -1,
        statusUpdateCallback, status)
    }
  }

  def setStatus(status: String ) {
    this.status = status
    updateStatus(status)
  }

  def register(): Unit = {
    zk.create("/workers/worker-" + serverId,
      "Idle".getBytes(),
      Ids.OPEN_ACL_UNSAFE,
      CreateMode.EPHEMERAL,
      createWorkerCallback,
      null)
  }

  def createAssignCallback = new StringCallback {
    override def processResult(rc: Int, path: String, ctx: scala.Any, name: String): Unit = {
      Code.get(rc) match {
        case Code.CONNECTIONLOSS =>
          createAssignNode()
        case Code.OK =>
          logInfo("Assign node created")
        case Code.NODEEXISTS =>
          logWarning("Assign node already registered")
        case _ =>
          logError("Something went wrong: " + KeeperException.create(Code.get(rc), path))
      }
    }
  }

  def createAssignNode(){
    zk.create("/assign/worker-" + serverId,
      new Array[Byte](0),
      Ids.OPEN_ACL_UNSAFE,
      CreateMode.PERSISTENT,
      createAssignCallback,
      null)
  }

  def bootstrap(){
    createAssignNode()
  }

  def stop(): Unit = {
    zk.close()
  }
}

object Worker extends App {
  val worker = new Worker("101.251.195.186:2181,101.251.195.187:2181,101.251.195.188:2181")
  worker.register()
  worker.bootstrap()
  worker.getTasks()
  Thread.sleep(300000)
  worker.stop()
}