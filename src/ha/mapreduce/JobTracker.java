package ha.mapreduce;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Master node
 */

public class JobTracker implements JobTrackerInterface {
  private HashMap<Integer, JobInProgress> jobs;

  int currentJob;

  private HashMap<Integer, JobInProgress> reducejobs;

  int currentReduce;

  public JobTracker() {
    jobs = new HashMap<Integer, JobInProgress>();
    currentJob = 0;
    reducejobs = new HashMap<Integer, JobInProgress>();
    currentReduce = 0;

  }

  public int startJob(JobConf jf) throws IOException, InterruptedException {
    JobInProgress jp = new JobInProgress(jf);
    int len = jobs.size();
    jobs.put(len, jp);
    int len1 = reducejobs.size();
    reducejobs.put(len1, jp);

    // Thread.sleep(20000);
    // new Thread(jp).start();
    return len;
  }

  public String updateInformation(int JobID) throws RemoteException {

    return "what's up biatch!!";

  }

  @SuppressWarnings("rawtypes")
  @Override
  public List<TaskConf> getMapTasks(InetSocketAddress slave, int tasksAvailable)
          throws RemoteException {
    // TODO HANZ
    List<TaskConf> temp = new ArrayList<TaskConf>();
    JobInProgress jp = jobs.get(currentJob);
    while (true) {
      while (tasksAvailable > 0 && jp.getNextSplit() <= jp.getInputSplit() * jp.getlineperSplit()) {
        @SuppressWarnings("unchecked")
        TaskConf tf = new TaskConf(jp.getJc().getInputFile(), jp.getNextSplit(), jp.getNextSplit()
                + jp.getlineperSplit(), 0, 0, (Class<Task>) (Class) jp.getJc().getMapperClass(),
                currentJob);
        tasksAvailable--;
        jp.setNextSplit(jp.getNextSplit() + jp.getlineperSplit());

      }
      // if current job is finished
      if (jp.getNextSplit() > jp.getInputSplit() * jp.getlineperSplit()){
        currentJob++;
      }
      // if still slave is still available
      if (tasksAvailable >0){
        if(currentJob>=jobs.size()){
          break;
        }
        else
          continue;
      }
      
    }

    return temp;
  }

  @Override
  public List<TaskConf> getReduceTasks(InetSocketAddress slave, int tasksAvailable)
          throws RemoteException {
    // TODO HANZ
    return null;
  }
}
