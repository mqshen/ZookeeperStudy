package ch03

import java.util.concurrent.{ConcurrentHashMap, CountDownLatch}

import org.apache.zookeeper.AsyncCallback.{VoidCallback, DataCallback, StatCallback, StringCallback}
import org.apache.zookeeper.KeeperException.{Code, ConnectionLossException, NodeExistsException}
import org.apache.zookeeper.Watcher.Event.EventType
import org.apache.zookeeper.ZooDefs.Ids
import org.apache.zookeeper._
import org.apache.zookeeper.data.Stat
import util.Logging

import scala.util.Random

/**
 * Created by goldratio on 3/12/15.
 */
class Client(hostPort: String) extends Watcher with Logging {

  val zk = new ZooKeeper(hostPort, 150000, this)

  val serverId = Integer.toHexString(Random.nextInt())

  val  ctxMap = new ConcurrentHashMap[String, scala.Any]()

  override def process(watchedEvent: WatchedEvent): Unit = {
    print(watchedEvent)
  }

  def queueCommand(command: String): String = {
    try {
      val name = zk.create("/tasks/task-", command.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL)
      name
    }
    catch {
      case e: NodeExistsException =>
        throw new Exception(command+ " already appears to be running")
      case e: ConnectionLossException =>
        queueCommand(command)
    }
  }

  class TaskObject(var task: String, var taskName: String) extends Logging{
    var done = false
    var succesful = false
    val latch = new CountDownLatch(1)

    def setStatus(status: Boolean){
      succesful = status
      done = true
      latch.countDown()
    }

    def waitUntilDone () {
      try{
        latch.await()
      }
      catch {
        case e: InterruptedException =>
          logWarning("InterruptedException while waiting for task to get done")
      }
    }

    def isDone() = {
      done
    }

    def isSuccesful() = {
      succesful
    }

  }

  val createTaskCallback = new StringCallback {
    override def processResult(rc: Int, path: String, ctx: scala.Any, name: String): Unit = {
      Code.get(rc) match {
        case Code.CONNECTIONLOSS =>
          submitTask( ctx.asInstanceOf[TaskObject].task);
        case Code.OK =>
          logInfo("My created task name: " + name)
          ctx.asInstanceOf[TaskObject].taskName = name
          watchStatus("/status/" + name.replace("/tasks/", ""), ctx)
        case _ =>
          logError("Something went wrong: " + KeeperException.create(Code.get(rc), path))
      }
    }
  }

  val taskDeleteCallback: VoidCallback = new VoidCallback {
    override def processResult(rc: Int, path: String, o: scala.Any): Unit = {
      Code.get(rc) match {
        case Code.CONNECTIONLOSS =>
          zk.delete(path, -1, taskDeleteCallback, null)
        case Code.OK =>
          logInfo("Successfully deleted " + path)
        case _ =>
          logError("Something went wrong: " + KeeperException.create(Code.get(rc), path))
      }
    }
  }

  val getDataCallback: DataCallback = new DataCallback {
    override def processResult(rc: Int, path: String, ctx: scala.Any, data: Array[Byte], stat: Stat): Unit = {
      Code.get(rc) match {
        case Code.CONNECTIONLOSS =>
          zk.getData(path, false, getDataCallback, ctxMap.get(path))
        case Code.OK =>
          val taskResult = new String(data)
          logInfo("Task " + path + ", " + taskResult)

          assert(ctx != null)
          ctx.asInstanceOf[TaskObject].setStatus(taskResult.contains("done"))

          zk.delete(path, -1, taskDeleteCallback, null)
          ctxMap.remove(path);
        case Code.NONODE =>
          logWarning("Status node is gone!")
        case _ =>
          logError("Something went wrong: " + KeeperException.create(Code.get(rc), path))
      }
    }
  }

  val existsCallback = new StatCallback {
    override def processResult(rc: Int, path: String, ctx: scala.Any, stat: Stat): Unit = {
      Code.get(rc) match {
        case Code.CONNECTIONLOSS =>
          watchStatus(path, ctx)
        case Code.OK =>
          if(stat != null) {
            zk.getData(path, false, getDataCallback, null);
          }
        case Code.NONODE =>

        case _ =>
          logError("Something went wrong: " + KeeperException.create(Code.get(rc), path))
      }
    }
  }

  val statusWatcher = new Watcher() {
    override def process(watchedEvent: WatchedEvent): Unit = {
      if (watchedEvent.getType() == EventType.NodeCreated) {
        assert(watchedEvent.getPath().contains("/status/task-"))
        zk.getData(watchedEvent.getPath(), false, getDataCallback, ctxMap.get(watchedEvent.getPath()))
      }
    }
  }
  def watchStatus(path: String , ctx: scala.Any) {
    ctxMap.put(path, ctx)
    zk.exists(path, statusWatcher, existsCallback, ctx)
  }

  def submitTask(task: String ) {
    val taskCtx = new TaskObject(task, "")
    zk.create("/tasks/task-", task.getBytes(),
      Ids.OPEN_ACL_UNSAFE,
      CreateMode.PERSISTENT_SEQUENTIAL,
      createTaskCallback,
      taskCtx)
  }


  def stop(): Unit = {
    zk.close()
  }

}
object Client extends App {
  val worker = new Client("101.251.195.186:2181,101.251.195.187:2181,101.251.195.188:2181")
  Thread.sleep(3000)
  worker.submitTask("test for test")
  Thread.sleep(300000)
  worker.stop()
}