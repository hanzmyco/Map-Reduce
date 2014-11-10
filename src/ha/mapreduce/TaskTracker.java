package ha.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * it handle one machine, how many mapper(map tasks), how many reducer tasks
 * it will construct the TaskInProgress
 * the JobTracker will use rmi or rpc to call the for tasktracker and then tasktracker will generate taskinprogress
 * @author hanz
 *
 */
public class TaskTracker {
  private List<TaskInProgress> tasks;

  public TaskTracker() {
    tasks = new ArrayList<TaskInProgress>();
  }

  public void startTask(Integer count, Class<Task> taskClass) throws IOException, InstantiationException, IllegalAccessException {
    for (int i = 0; i < count ; i++) {
      TaskInProgress tp = new TaskInProgress(taskClass.newInstance());
      tasks.add(tp);
      
      new Thread(tp).start();
    }
  }
}
