package ha.mapreduce;

import java.io.IOException;

public class TaskInProgress implements Runnable {
  private Task task;

  private Status status;
  
  private int tipID;

  public TaskInProgress(int id) {
    status = Status.AVAILABLE;
    tipID = id;
  }

  public void setupNewTask(Task task) {
    this.task = task;
    status = Status.ASSIGNED;

    System.out.println("[TASK TRACKER] Received task " + task.getTaskID() + " of " + task.getClass() + " for job " + task.getJobID());
  }
  
  public int getID() {
    return tipID;
  }
  
  /**
   * @return the status
   */
  public Status getStatus() {
    return status;
  }

  public Integer getJobID() {
    return task.getJobID();
  }
  
  public Task getTask() {
    return task;
  }
  
  public void clearTask() {
    task = null;
  }

  @Override
  public void run() {
    while (true) {
      while (status == Status.ASSIGNED && this.task != null) {
        status = Status.BUSY;

        try {
          task.process();
        } catch (IOException e) {
          System.err.println("[TASK " + task.getJobID() + "] IO problems for task of "
                  + task.getClass());
          e.printStackTrace();
        }
        System.out.println("[TASK " + task.getJobID() + "] Finished processing "
                + task.taskConf.getOutputFilename());
        status = Status.AVAILABLE;
      }
      
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }
}
