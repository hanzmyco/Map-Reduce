package ha.mapreduce;

import java.io.IOException;

public class TaskInProgress implements Runnable {
  private Task task;
  
  public TaskInProgress(Task task) throws IOException {
    this.task=task;
    
    System.out.println("[TASK] Received new task of " + task.getClass());
  }
  
  @Override
  public void run() {
    try {
      task.process();
    } catch (IOException e) {
      System.err.println("[TASK] IO problems for task of " + task.getClass());
      e.printStackTrace();
    }
  }
}
