package ch03


import org.apache.zookeeper.AsyncCallback._
import org.apache.zookeeper.KeeperException.{ ConnectionLossException, NoNodeException, Code }
import org.apache.zookeeper.Watcher.Event.EventType
import org.apache.zookeeper.ZooDefs.Ids
import org.apache.zookeeper.data.Stat
import org.apache.zookeeper._
import util.Logging
import scala.collection.JavaConversions._
import scala.util.Random

/**
 * Created by goldratio on 3/12/15.
 */
class AsyncMaster(hostPort: String) extends Watcher with Logging {

  val zk = new ZooKeeper(hostPort, 15000, this)

  val serverId = Integer.toHexString(Random.nextInt())

  //var workers: List[String] = List.empty

  var workersCache = new ChildrenCache()

  override def process(watchedEvent: WatchedEvent): Unit = {
    print(watchedEvent)
  }

  val masterCreateCallback = new StringCallback() {
    override def processResult(rc: Int, path: String, o: scala.Any, s1: String): Unit = {
      Code.get(rc) match {
        case Code.CONNECTIONLOSS =>
          checkMaster()
        case Code.OK =>
          takeLeadership()
        case Code.NODEEXISTS =>
          masterExists()
        case _ =>
          logError("Something went wrong when running for master.", KeeperException.create(Code.get(rc), path))
      }
    }
  }

  val masterExistsWatcher = new Watcher {
    override def process(watchedEvent: WatchedEvent): Unit = {
      if(watchedEvent.getType == EventType.NodeDeleted) {
        assert("/master" == watchedEvent.getPath)
        runForMaster()
      }
    }
  }

  val masterExistsCallback = new StatCallback {
    override def processResult(rc: Int, path: String, ctx: scala.Any, stat: Stat): Unit = {
      Code.get(rc) match {
        case Code.CONNECTIONLOSS =>
          masterExists()
        case Code.OK =>
        case Code.NONODE =>
          runForMaster()
          logInfo("It sounds like the previous master is gone, so let's run for master again.")
        case _ =>
          checkMaster()
      }
    }
  }

  def takeLeadership() = {
    logInfo("Going for list of workers")
    getWorkers()
    //TODO recovery
    getTasks()
  }

  def masterExists(): Unit = {
    zk.exists("/master", masterExistsWatcher, masterExistsCallback, null)
  }

  def runForMaster() = {
    zk.create("/master", serverId.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, masterCreateCallback, None)
  }

  val workersChangeWatcher = new Watcher {
    override def process(watchedEvent: WatchedEvent): Unit = {
      if(watchedEvent.getType() == EventType.NodeChildrenChanged) {
        assert("/workers" == watchedEvent.getPath() )
        getWorkers()
      }
    }
  }

  def reassignAndSet(workers: Seq[String]) = {
    logInfo( "Removing and setting" )
    val toProcess = workersCache.removedAndSet( workers )

    if(!toProcess.isEmpty) {
      toProcess.foreach { worker =>
        getAbsentWorkerTasks(worker)
      }
    }
  }

  val workerAssignmentCallback = new ChildrenCallback() {
    override def processResult(rc: Int, path: String, o: scala.Any, children: java.util.List[String]): Unit = {
      Code.get(rc) match {
        case Code.CONNECTIONLOSS =>
          getAbsentWorkerTasks(path)
        case Code.OK =>
          logInfo("Succesfully got a list of assignments: "
            + children.size()
            + " tasks")
          children.foreach { task =>
            getDataReassign(path + "/" + task, task)
          }
        case _ =>
          logError("getChildren failed",  KeeperException.create(Code.get(rc), path))
      }
    }
  }

  case class RecreateTaskCtx(path: String, task: String, data: Array[Byte])

  val getDataReassignCallback = new DataCallback {
    override def processResult(rc: Int, path: String, ctx: scala.Any, data: Array[Byte], stat: Stat): Unit = {
      Code.get(rc) match {
        case Code.CONNECTIONLOSS =>
          getDataReassign(path, ctx.asInstanceOf[String])
        case Code.OK =>
          recreateTask(new RecreateTaskCtx(path,  ctx.asInstanceOf[String], data))
        case _ =>
          logError("Something went wrong when getting data ", KeeperException.create(Code.get(rc)))
      }
    }
  }

  val recreateTaskCallback = new StringCallback {
    override def processResult(rc: Int, path: String, ctx: scala.Any, name: String): Unit = {
      Code.get(rc) match {
        case Code.CONNECTIONLOSS =>
          recreateTask(ctx.asInstanceOf[RecreateTaskCtx]);
        case Code.OK =>
          deleteAssignment(ctx.asInstanceOf[RecreateTaskCtx].path)
        case Code.NODEEXISTS =>
          logInfo("Node exists already, but if it hasn't been deleted, " +
            "then it will eventually, so we keep trying: " + path)
          recreateTask(ctx.asInstanceOf[RecreateTaskCtx])
        case _ =>
          logError("Something went wrong when getting data ", KeeperException.create(Code.get(rc)))
      }
    }
  }

  val taskDeletionCallback = new VoidCallback {
    override def processResult(rc: Int, path: String, rtx: scala.Any): Unit = {
      Code.get(rc) match {
        case Code.CONNECTIONLOSS =>
          deleteAssignment(path)
        case Code.OK =>
          logInfo("Task correctly deleted: " + path)
        case _ =>
          logError("Failed to delete task data" + KeeperException.create(Code.get(rc), path))
      }
    }
  }

  def deleteAssignment(path: String ){
    zk.delete(path, -1, taskDeletionCallback, null)
  }

  def recreateTask(ctx: RecreateTaskCtx ) {
    zk.create("/tasks/" + ctx.task,
      ctx.data,
      Ids.OPEN_ACL_UNSAFE,
      CreateMode.PERSISTENT,
      recreateTaskCallback,
      ctx)
  }


  def getDataReassign(path: String , task: String ) {
    zk.getData(path,
      false,
      getDataReassignCallback,
      task)
  }

  def getAbsentWorkerTasks(worker: String ){
    zk.getChildren(s"/assign/$worker", false, workerAssignmentCallback, null)
  }

  val workersGetChildrenCallback = new ChildrenCallback {
    override def processResult(rc: Int, path: String, ctx: scala.Any, children: java.util.List[String]): Unit = {
      Code.get(rc) match {
        case Code.CONNECTIONLOSS =>
          getWorkers()
        case Code.OK =>
          logInfo("Succesfully got a list of workers: " + children.size() + " workers")
          reassignAndSet(children.toList)
        case _ =>
          logError("getChildren failed", KeeperException.create(Code.get(rc), path))
      }
    }
  }

  def getWorkers(): Unit = {
    zk.getChildren("/workers", workersChangeWatcher, workersGetChildrenCallback, null)
  }

  val tasksChangeWatcher = new Watcher {
    override def process(watchedEvent: WatchedEvent): Unit = {
      if(watchedEvent.getType() == EventType.NodeChildrenChanged) {
        assert("/tasks" ==  watchedEvent.getPath() )
        getTasks()
      }
    }
  }

  val tasksGetChildrenCallback = new ChildrenCallback {
    override def processResult(rc: Int, path: String, ctx: scala.Any, tasks: java.util.List[String]): Unit = {
      Code.get(rc) match {
        case Code.CONNECTIONLOSS =>
          getTasks()
        case Code.OK =>
          if(tasks!= null) {
            assignTasks(tasks.toList)
          }
        case _ =>
          logError("getChildren failed.", KeeperException.create(Code.get(rc), path))
      }

    }
  }

  def assignTasks(tasks: Seq[String]) = {
    tasks.foreach { task =>
      getTaskData(task)
    }
  }

  def getTasks() {
    zk.getChildren("/tasks", tasksChangeWatcher, tasksGetChildrenCallback, null)
  }

  val taskDataCallback = new DataCallback {
    override def processResult(rc: Int, path: String, ctx: scala.Any, data: Array[Byte], stat: Stat): Unit = {
      Code.get(rc) match {
        case Code.CONNECTIONLOSS =>
          getTaskData(ctx.asInstanceOf[String])
        case Code.OK =>
          val worker = Random.nextInt(workersCache.size)
          val designatedWorker = workersCache.get(worker)
          val assignmentPath = "/assign/" + designatedWorker + "/" + ctx.asInstanceOf[String]
          createAssignment(assignmentPath, data)
        case _ =>
          logError("Error when trying to get task data.", KeeperException.create(Code.get(rc), path))
      }
    }
  }

  val assignTaskCallback = new StringCallback {
    override def processResult(rc: Int, path: String, ctx: scala.Any, name: String): Unit = {
      Code.get(rc) match {
        case Code.CONNECTIONLOSS =>
          createAssignment(path, ctx.asInstanceOf[Array[Byte]])
        case Code.OK =>
          logInfo("Task assigned correctly: " + name)
          deleteTask(name.substring( name.lastIndexOf("/") + 1 ))
        case Code.NODEEXISTS =>
          logWarning("Task already assigned")
        case _ =>
          logError("Error when trying to assign task.", KeeperException.create(Code.get(rc), path))
      }
    }
  }

  def createAssignment(path: String, data: Array[Byte]): Unit = {
    zk.create(path,
      data, Ids.OPEN_ACL_UNSAFE,
      CreateMode.PERSISTENT,
      assignTaskCallback,
      data)
  }

  def deleteTask(task: String) = {
    zk.delete(s"/tasks/$task", -1)
  }

  def getTaskData(task: String ) {
    zk.getData("/tasks/" + task, false, taskDataCallback, task)
  }

  def bootstrap() {
    createParent("/workers", Array[Byte](0))
    createParent("/assign", Array[Byte](0))
    createParent("/tasks", Array[Byte](0))
    createParent("/status", Array[Byte](0))
  }

  val createParentCallback = new StringCallback {
    override def processResult(rc: Int, path: String, ctx: scala.Any, name: String): Unit = {
      Code.get(rc) match {
        case Code.CONNECTIONLOSS =>
          createParent(path, ctx.asInstanceOf[Array[Byte]])
        case Code.OK =>
          logInfo("Parent registered: " + path)
        case Code.NODEEXISTS =>
          logWarning("Parent already registered: " + path)
        case _ =>
          logError("Something went wrong: ", KeeperException.create(Code.get(rc), path))
      }
    }
  }

  def createParent(path: String, data: Array[Byte]): Unit = {

    zk.create(path,
      data,
      Ids.OPEN_ACL_UNSAFE,
      CreateMode.PERSISTENT,
      createParentCallback,
      data)
  }


  val masterCheckCallback = new DataCallback {
    override def processResult(rc: Int, path: String, ctx: scala.Any, data: Array[Byte], stat: Stat): Unit = {
      Code.get(rc) match {
        case Code.CONNECTIONLOSS =>
          checkMaster()
        case Code.NONODE =>
          runForMaster()
        case Code.OK =>
          if( serverId.equals( new String(data) ) ) {
            takeLeadership()
          }
          else {
            masterExists()
          }
      }
    }
  }

  def checkMaster(): Unit = {

    zk.getData("/master", false, masterCheckCallback, null)
  }

  val assignCallBack = new ChildrenCallback {
    override def processResult(rc: Int, path: String, o: scala.Any, list: java.util.List[String]): Unit = {
      Code.get(rc) match {
        case Code.OK =>
          list.foreach{ worker =>
            val data = zk.getData("/workers" + worker, false, null)
            if("Idle" == new String(data)){
              zk.create("/assign/" + worker,  o.asInstanceOf[String].getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT )
            }
          }
        case Code.CONNECTIONLOSS =>
          startAssign()
        case Code.NODEEXISTS =>
          logInfo("work all assign")
        case _ =>
          logError("Something went wrong: ", KeeperException.create(Code.get(rc), path))
      }
    }
  }

  val taskCallback = new ChildrenCallback {
    override def processResult(rc: Int, path: String, o: scala.Any, list: java.util.List[String]): Unit = {
      Code.get(rc) match {
        case Code.OK =>
          list.toList.foreach { task =>
              zk.getChildren("/workers", false, assignCallBack, task)
          }
        case _ =>
          logError("Something went wrong: ", KeeperException.create(Code.get(rc), path))
      }
      startAssign()
    }
  }

  def startAssign(): Unit = {
    zk.getChildren("/tasks", false, taskCallback, null)
  }

  def stop(): Unit = {
    zk.close()
  }

}

object AsyncMaster extends App {

  val master = new AsyncMaster("101.251.195.186:2181,101.251.195.187:2181,101.251.195.188:2181")

  master.runForMaster()


  Thread.sleep(600000)
}