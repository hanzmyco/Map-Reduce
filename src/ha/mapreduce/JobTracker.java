package ha.mapreduce;

import ha.IO.NameNodeInterface;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.rmi.RemoteException;
import java.util.ArrayList;
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

  public JobTracker(NameNodeInterface nameNode) {
    jobs = new ArrayList<JobInProgress>();
    mapTasks = new LinkedHashMap<TaskConf, Boolean>();
    reduceTasks = new LinkedHashMap<TaskConf, Boolean>();
    this.nameNode = nameNode;
  }

  public int startJob(JobConf jf) throws IOException, InterruptedException {
    jf.setJobID(jobs.size());
    JobInProgress jp = new JobInProgress(jf, nameNode);

    for (TaskConf task : jp.getMapTasks(mapTasks.size())) {
      mapTasks.put(task, true);
    }
    for (TaskConf task : jp.getReduceTasks()) {
      reduceTasks.put(task, true);
    }

    jobs.add(jp);
    return jf.getJobID();
  }

  public String updateInformation(int JobID) throws RemoteException {

    return "what's up biatch!!";

  }

  @Override
  public List<TaskConf> getMapTasks(InetSocketAddress slave, int tasksAvailable)
          throws RemoteException {
    System.out.println("[JOB TRACKER] Received request for " + tasksAvailable + " map tasks");
    List<TaskConf> temp = new ArrayList<TaskConf>();

    for (Map.Entry<TaskConf, Boolean> task : mapTasks.entrySet()) {
      if (task.getValue()) {
        System.out.println("[JOB TRACKER] Allocating task " + task.getKey().getTaskID()
                + " from job " + task.getKey().getJobID());
        temp.add(task.getKey());
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
  public List<TaskConf> getReduceTasks(InetSocketAddress slave, int tasksAvailable)
          throws RemoteException {
    // TODO HANZ
    return new ArrayList<TaskConf>();
  }
}
