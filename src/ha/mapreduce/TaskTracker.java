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
 */
public class TaskTracker implements TaskTrackerInterface, Runnable {
  private List<TaskInProgress> mappers, reducers;

  private NameNodeInterface nameNode;

  private JobTrackerInterface jobTracker;

  private InetSocketAddress thisMachine;

  public TaskTracker(InetSocketAddress thisMachine, NameNodeInterface nameNode,
          JobTrackerInterface jobTracker, int numMappers, int numReducers) {
    mappers = new ArrayList<TaskInProgress>();
    reducers = new ArrayList<TaskInProgress>();
    for (int i = 0; i < numMappers; i++) {
      TaskInProgress tp = new TaskInProgress(i);
      new Thread(tp).start();
      mappers.add(tp);
    }
    for (int i = 0; i < numReducers; i++) {
      TaskInProgress tp = new TaskInProgress(i);
      new Thread(tp).start();
      reducers.add(tp);
    }
    this.nameNode = nameNode;
    this.jobTracker = jobTracker;
    this.thisMachine = thisMachine;
  }

  public List<TaskStatus> getTaskStatuses() {
    List<TaskStatus> statuses = new ArrayList<TaskStatus>();
    for (TaskInProgress tip : mappers) {
      statuses.add(new TaskStatus(tip, "Mapper"));
    }
    for (TaskInProgress tip : reducers) {
      statuses.add(new TaskStatus(tip, "Reducer"));
    }
    return statuses;
  }

  private List<TaskInProgress> availableTasksInProgress(List<TaskInProgress> tasks) {
    List<TaskInProgress> availableTasks = new ArrayList<TaskInProgress>();
    for (TaskInProgress tip : mappers) {
      if (tip.getStatus() == Status.AVAILABLE) {
        if (tip.getTask() != null) {
          try {
            jobTracker.markAsDone(tip.getTask().taskConf);
            tip.clearTask();
          } catch (RemoteException e) {
            System.err.println("[TASK TRACKER] Can't tell job tracker that task " + tip.getTask().getTaskID() + " is done");
            e.printStackTrace();
          }
        }
        availableTasks.add(tip);
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
    for (TaskConf tc : (List<TaskConf>) jobGetter.invoke(jobTracker, thisMachine,
            availableTasks.size())) {
      try {
        Task newTask = (Task) tc.getTaskClass().newInstance();
        newTask.setup(tc, nameNode);
        availableTasks.get(0).setupNewTask(newTask);
        availableTasks.remove(0);
        tasksReceived++;
      } catch (Exception e) {
        System.err.println("[TASK TRACKER] Unable to create task #" + tc.getJobID());
        e.printStackTrace();
      }
    }
    System.out.println("[TASK TRACKER] Received " + tasksReceived + " new " + jobType + " tasks.");
  }

  private void askForNewMapTasks() throws RemoteException {
    try {
      askForNewTasks(mappers, "map", JobTrackerInterface.class.getMethod("getMapTasks",
              InetSocketAddress.class, int.class));
    } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException
            | NoSuchMethodException | SecurityException e) {
      e.printStackTrace();
    }
  }

  private void askForNewReduceTasks() throws RemoteException {
    try {
      askForNewTasks(reducers, "reduce", JobTrackerInterface.class.getMethod("getReduceTasks",
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

  @Override
  public String sayhello() throws RemoteException {
    return "I'm good, dude";

  }

}
