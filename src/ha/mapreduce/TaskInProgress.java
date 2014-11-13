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

    System.out.println("[TASK] Received new task of " + task.getClass());
  }

  @Override
  public void run() {
    while (this.task != null) {
      status = Status.BUSY;

      try {
        task.process();
      } catch (IOException e) {
        System.err.println("[TASK] IO problems for task of " + task.getClass());
        e.printStackTrace();
      }

      status = Status.AVAILABLE;
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
