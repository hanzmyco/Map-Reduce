package ha.mapreduce;

import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


/**
 * Master node
 */
public class JobTracker  implements JobTrackerInterface{
  private HashMap<Integer,JobInProgress> jobs;
  int currentJob;


  public JobTracker() {
    jobs = new HashMap<Integer,JobInProgress>();
  }

  public int startJob(JobConf jf) throws IOException, InterruptedException {
    JobInProgress jp = new JobInProgress(jf);
    int len=jobs.size();
    jobs.put(len,jp);
    Thread.sleep(20000);
    //new Thread(jp).start();
    return len;
  }
  
  public String updateInformation(int JobID)throws RemoteException{
    
    return "what's up biatch!!";
    
  }
  
  
}
