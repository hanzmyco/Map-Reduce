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
  private HashMap<Integer, JobInProgress> mapJobs;

  int currentMap;

  private HashMap<Integer, JobInProgress> reducejobs;

  int currentReduce;

  public JobTracker() {
    mapJobs = new HashMap<Integer, JobInProgress>();
    currentMap = 0;
    reducejobs = new HashMap<Integer, JobInProgress>();
    currentReduce = 0;

  }

  public int startJob(JobConf jf) throws IOException, InterruptedException {
    JobInProgress jp = new JobInProgress(jf);
    int len = mapJobs.size();
    mapJobs.put(len, jp);
    int len1 = reducejobs.size();
    reducejobs.put(len1, jp);
    return len;
  }

  public String updateInformation(int JobID) throws RemoteException {

    return "what's up biatch!!";

  }

  @SuppressWarnings("rawtypes")
  @Override
  public List<TaskConf> getMapTasks(InetSocketAddress slave, int tasksAvailable)
          throws RemoteException {
    List<TaskConf> temp = new ArrayList<TaskConf>();
    JobInProgress jp = mapJobs.get(currentMap);
    if (jp == null)
      return temp;
    while (true) {
      while (tasksAvailable > 0 && jp.getNextSplit() <= jp.getInputSplit() * jp.getlineperSplit()) {
        @SuppressWarnings("unchecked")
        TaskConf tf = new TaskConf(jp.getJc().getInputFile(), jp.getNextSplit(), jp.getNextSplit()
                + jp.getlineperSplit(), 0, 0, (Class<Task>) (Class) jp.getJc().getMapperClass(),
                currentMap);
        tasksAvailable--;
        jp.setNextSplit(jp.getNextSplit() + jp.getlineperSplit());

      }
      // if current job is finished
      if (jp.getNextSplit() > jp.getInputSplit() * jp.getlineperSplit()){
        currentMap++;
      }
      // if still slave is still available
      if (tasksAvailable >0){
        if(currentMap>=mapJobs.size()){
          break;
        }
        else
          continue;
      }
      else
        break;
     
      
    }

    return temp;
  }

  @Override
  public List<TaskConf> getReduceTasks(InetSocketAddress slave, int tasksAvailable)
          throws RemoteException {
    // TODO HANZ
    return new ArrayList<TaskConf>();
  }
}
