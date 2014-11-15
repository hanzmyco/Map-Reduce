package ha.mapreduce;

import ha.IO.NameNodeInterface;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
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

  @SuppressWarnings("unchecked")
  private void askForNewTasks(List<TaskInProgress> existingJobs, String jobType, Method jobGetter)
          throws RemoteException, IllegalAccessException, IllegalArgumentException,
          InvocationTargetException {
    List<TaskInProgress> availableTasks = availableTasksInProgress(existingJobs);
    System.out.println("[TASK TRACKER] Asking for " + availableTasks.size() + " new " + jobType
            + " tasks...");
    int tasksReceived = 0;
    if (jobTracker == null) System.err.println("arst");
    if (thisMachine == null) System.err.println("brst");
    if (jobGetter == null) System.err.println("crst");
    for (TaskConf tc : (List<TaskConf>) jobGetter.invoke(jobTracker, thisMachine, availableTasks.size())) {
      try {
        Task newTask = (Task) tc.getTaskClass().newInstance();
        availableTasks.get(0).setupNewTask(newTask);
        availableTasks.remove(0);
        tasksReceived++;
      } catch (Exception e) {
        System.err.println("[TASK TRACKER] Unable to create task #" + tc.getJobID());
        e.printStackTrace();
      }
    }
    System.out.println("[TASK TRACKER] Received " + tasksReceived + " new " + jobType
            + " tasks.");
  }

  private void askForNewMapTasks() throws RemoteException {
    try {
      askForNewTasks(mapTasks, "map", JobTrackerInterface.class.getMethod("getMapTasks",
              InetSocketAddress.class, int.class));
    } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException
            | NoSuchMethodException | SecurityException e) {
      e.printStackTrace();
    }
  }

  private void askForNewReduceTasks() throws RemoteException {
    try {
      askForNewTasks(reduceTasks, "reduce", JobTrackerInterface.class.getMethod("getReduceTasks",
              InetSocketAddress.class, int.class));
    } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException
            | NoSuchMethodException | SecurityException e) {
      e.printStackTrace();
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
