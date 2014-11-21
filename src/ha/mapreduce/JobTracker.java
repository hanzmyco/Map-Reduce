package ha.mapreduce;

import ha.IO.DataNodeInterface;
import ha.IO.NameNodeInterface;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Master node
 */

public class JobTracker implements JobTrackerInterface {
  private NameNodeInterface nameNode;

  private List<JobInProgress> jobs;

  private Map<TaskConf, Boolean> mapTasks, reduceTasks;

  private Map<InetSocketAddress, TaskTrackerInterface> taskTrackers;

  private Map<InetSocketAddress, List<TaskConf>> taskList;

  // private HashMap<TaskTrackerInterface, Boolean> statusList;

  public JobTracker(NameNodeInterface nameNode) {
    jobs = new ArrayList<JobInProgress>();
    mapTasks = new LinkedHashMap<TaskConf, Boolean>();
    reduceTasks = new LinkedHashMap<TaskConf, Boolean>();
    this.nameNode = nameNode;
    taskTrackers = new HashMap<InetSocketAddress, TaskTrackerInterface>();
    taskList = new HashMap<InetSocketAddress, List<TaskConf>>();
    // statusList=new HashMap<TaskTrackerInterface, Boolean>();

  }

  public int startJob(JobConf jf) throws IOException, InterruptedException {
    jf.setJobID(jobs.size());
    JobInProgress jp = new JobInProgress(jf, nameNode);

    for (TaskConf task : jp.getMapTasks(mapTasks.size() + reduceTasks.size())) {
      mapTasks.put(task, true);
    }

    jobs.add(jp);
    return jf.getJobID();
  }

  public String getStatuses() throws RemoteException {
    StringBuilder sb = new StringBuilder();

    for (Map.Entry<InetSocketAddress, TaskTrackerInterface> entry : taskTrackers.entrySet()) {
      

      Boolean judge = false;
      try {
        entry.getValue().sayhello();
      } catch (RemoteException e) {
        System.out.println("[JOB TRACKER] slave node " + entry.getKey().toString() + " is down");
        judge = true;
      
      }

      if (judge == false) {
        sb.append(entry.getKey() + ":\n");
        for (TaskStatus taskStatus : entry.getValue().getTaskStatuses()) {
          sb.append("  " + taskStatus + "\n");
        }
      }
    }

    int assigned = 0, unassigned = 0;
    for (Boolean unAssigned : mapTasks.values()) {
      if (unAssigned)
        unassigned++;
      else
        assigned++;
    }
    sb.append("\nMap tasks: " + assigned + " assigned, " + unassigned + " unassigned.\n");

    assigned = 0;
    unassigned = 0;
    for (Boolean unAssigned : reduceTasks.values()) {
      if (unAssigned)
        unassigned++;
      else
        assigned++;
    }
    sb.append("Reduce tasks: " + assigned + " assigned, " + unassigned + " unassigned.\n");

    return sb.toString();
  }

  public List<TaskConf> getTasks(InetSocketAddress slave, Map<TaskConf, Boolean> tasks,
          int tasksAvailable) {
    System.out.println("[JOB TRACKER] Received request for " + tasksAvailable + " map tasks");
    List<TaskConf> temp = new ArrayList<TaskConf>();

    for (Map.Entry<TaskConf, Boolean> task : tasks.entrySet()) {
      if (task.getValue()) {
        System.out.println("[JOB TRACKER] Allocating task " + task.getKey().getTaskID()
                + " from job " + task.getKey().getJobID());
        temp.add(task.getKey());

        taskList.get(slave).add(task.getKey());

        task.setValue(false);
        tasksAvailable--;
      }
      if (tasksAvailable <= 0)
        break;
    }

    System.out.println("[JOB TRACKER] Returning " + temp.size() + " task confs");
    return temp;
  }

  @Override
  public List<TaskConf> getMapTasks(InetSocketAddress slave, int tasksAvailable)
          throws RemoteException {
    return getTasks(slave, mapTasks, tasksAvailable);
  }

  @Override
  public List<TaskConf> getReduceTasks(InetSocketAddress slave, int tasksAvailable)
          throws RemoteException {
    return getTasks(slave, reduceTasks, tasksAvailable);
  }

  @Override
  public void registerAsSlave(String rmiName, InetSocketAddress rmi_location)
          throws RemoteException {
    try {
      taskTrackers.put(
              rmi_location,
              (TaskTrackerInterface) LocateRegistry.getRegistry(rmi_location.getHostString(),
                      rmi_location.getPort()).lookup(rmiName));
      taskList.put(rmi_location, new ArrayList<TaskConf>());
    } catch (NotBoundException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void markAsDone(TaskConf tc) {
    System.out
            .println("[JOB TRACKER] Received request to mark task " + tc.getTaskID() + " as done");
    if (mapTasks.remove(tc) != null) {
      JobInProgress jip = jobs.get(tc.getJobID());
      if (jip.mapTaskFinished()) {
        try {
          jip.sortMapOutput();
          for (TaskConf task : jip.getReduceTasks(mapTasks.size() + reduceTasks.size())) {
            reduceTasks.put(task, true);
          }
        } catch (IOException e) {
          System.err.println("Can't retrieve sorted file for job " + tc.getJobID());
          e.printStackTrace();
        }
      }
    }
    reduceTasks.remove(tc);
  }

  public void heartBeat() throws RemoteException {
    for (Map.Entry<InetSocketAddress, TaskTrackerInterface> pairs : taskTrackers.entrySet()) {
      TaskTrackerInterface t = pairs.getValue();

      try {
        System.out.println(t.sayhello());
      } catch (RemoteException e) {
        // do somehting
        // set the task to do again

        List<TaskConf> tconfList = taskList.get(pairs.getKey());
        for (TaskConf tf : tconfList) {
          if (mapTasks.containsKey(tf)) {
            mapTasks.put(tf, true);
          }
          if (reduceTasks.containsKey(tf)) {
            reduceTasks.put(tf, true);
          }
        }

      }
      System.out.println("[JOB TRACKER] slave node " + pairs.getKey().toString() + " is down");
      System.out.println("[JOB TRACKER] reassign all the task in the tasknode to other tasknode");
      System.out.println(pairs.getKey() + " = " + pairs.getValue());
    }
  }

}
