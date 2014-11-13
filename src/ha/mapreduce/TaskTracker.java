package ha.mapreduce;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * it handle one machine, how many mapper(map tasks), how many reducer tasks it will construct the
 * TaskInProgress the JobTracker will use rmi or rpc to call the for tasktracker and then
 * tasktracker will generate taskinprogress
 * 
 * @author hanz
 *
 */
public class TaskTracker implements Runnable {
  private List<TaskInProgress> mapTasks, reduceTasks;

  private JobTrackerInterface jobTracker;

  private InetSocketAddress thisMachine;

  public TaskTracker(InetSocketAddress thisMachine, JobTrackerInterface jobTracker, int numMappers,
          int numReducers) {
    mapTasks = new ArrayList<TaskInProgress>();
    reduceTasks = new ArrayList<TaskInProgress>();
    for (int i = 0; i < numMappers; i++) {
      TaskInProgress tp = new TaskInProgress();
      new Thread(tp).start();
      mapTasks.add(tp);
    }
    for (int i = 0; i < numReducers; i++) {
      TaskInProgress tp = new TaskInProgress();
      new Thread(tp).start();
      reduceTasks.add(tp);
    }
    this.jobTracker = jobTracker;
    this.thisMachine = thisMachine;
  }

  public Map<Integer, Status> getTaskStatuses() {
    Map<Integer, Status> statuses = new HashMap<Integer, Status>();
    for (TaskInProgress task : mapTasks) {
      statuses.put(task.getJobID(), task.getStatus());
    }
    return statuses;
  }
  
  private List<TaskInProgress> availableTasksInProgress(List<TaskInProgress> tasks) {
    List<TaskInProgress> availableTasks = new ArrayList<TaskInProgress>();
    for (TaskInProgress task : mapTasks) {
      if (task.getStatus() == Status.AVAILABLE) {
        availableTasks.add(task);
      }
    }
    return availableTasks;
  }

  private void askForNewMapTasks() throws RemoteException {
    List<TaskInProgress> availableTasks = availableTasksInProgress(mapTasks);
    System.out.println("[TASK TRACKER] Asking for " + availableTasks.size() + " new map tasks");
    for (TaskConf tc : jobTracker.getMapTasks(thisMachine, availableTasks.size())) {
      Mapper newMapper;
      try {
        newMapper = (Mapper) tc.getTaskClass().newInstance();
        availableTasks.get(0).setupNewTask(newMapper);
        availableTasks.remove(0);
      } catch (Exception e) {
        System.err.println("[TASK TRACKER] Unable to create task #" + tc.getJobID());
        e.printStackTrace();
      }
    }
  }
  
  private void askForNewReduceTasks() throws RemoteException {
    List<TaskInProgress> availableTasks = availableTasksInProgress(reduceTasks);
    System.out.println("[TASK TRACKER] Asking for " + availableTasks.size() + " new reduce tasks");
    for (TaskConf tc : jobTracker.getMapTasks(thisMachine, availableTasks.size())) {
      try {
        Reducer newReducer = (Reducer) tc.getTaskClass().newInstance();
        availableTasks.get(0).setupNewTask(newReducer);
        availableTasks.remove(0);
      } catch (Exception e) {
        System.err.println("[TASK TRACKER] Unable to create task #" + tc.getJobID());
        e.printStackTrace();
      }
    }
  }

  @Override
  public void run() {
    while (true) {
      try {
        askForNewMapTasks();
        askForNewReduceTasks();
        Thread.sleep(5000);
      } catch (RemoteException e) {
        System.err.println("[TASK TRACKER] Unable to ask for new tasks!");
        e.printStackTrace();
      } catch (InterruptedException e) {
        System.err.println("[TASK TRACKER] Cannot sleep thread!");
        e.printStackTrace();
      }
    }
  }
}
