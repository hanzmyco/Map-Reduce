package ha.mapreduce;

import java.io.IOException;

public class TaskInProgress implements Runnable {
  private Task task;

  private Status status;

  public TaskInProgress() {
    status = Status.AVAILABLE;

  }

  public void setupNewTask(Task task) {
    this.task = task;
    status = Status.ASSIGNED;

    System.out.println("[TASK TRACKER] Received task " + task.getTaskID() + " of " + task.getClass() + " for job " + task.getJobID());
  }

  @Override
  public void run() {
    while (true) {
      while (this.task != null) {
        status = Status.BUSY;

        try {
          task.process();
        } catch (IOException e) {
          System.err.println("[TASK " + task.getJobID() + "] IO problems for task of "
                  + task.getClass());
          e.printStackTrace();
        }
        System.out.println("[TASK " + task.getJobID() + "] Finished processing "
                + task.getCollector().getOutputFile());
        status = Status.AVAILABLE;
        
        task = null;
      }
      
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
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
}
