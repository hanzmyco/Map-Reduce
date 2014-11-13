package ha.mapreduce;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.List;

/**
 * Master node
 */
public class JobTracker implements JobTrackerInterface {
  private HashMap<Integer, JobInProgress> jobs;

  public JobTracker() {
    jobs = new HashMap<Integer, JobInProgress>();
  }

  public int startJob(JobConf jf) throws IOException, InterruptedException {
    JobInProgress jp = new JobInProgress(jf);
    int len = jobs.size();
    jobs.put(len, jp);
    Thread.sleep(20000);
    new Thread(jp).start();
    return len;
  }

  public String updateInformation(int JobID) throws RemoteException {

    return "what's up biatch!!";

  }

  @Override
  public List<TaskConf> getMapTasks(InetSocketAddress slave, int jobsAvailable) throws RemoteException {
    // TODO HANZ
    return null;
  }

  @Override
  public List<TaskConf> getReduceTasks(InetSocketAddress slave, int jobsAvailable) throws RemoteException {
    // TODO HANZ
    return null;
  }
}
