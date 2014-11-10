package ha.mapreduce;

import java.io.IOException;

public class TaskInProgress implements Runnable {
  private Task task;
  
  public TaskInProgress(Task task) throws IOException {
    this.task=task;
    
    System.out.println("[TASK] Received new task of class " + task.getClass());
  }
  
  @Override
  public void run() {
    // call task.process(...)
  }
}
