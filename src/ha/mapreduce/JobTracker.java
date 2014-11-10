package ha.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Master node
 */
public class JobTracker {
  private List<JobInProgress> jobs;

  public JobTracker() {
    jobs = new ArrayList<JobInProgress>();
  }

  public void startJob(JobConf jf) throws IOException, InterruptedException {
    JobInProgress jp = new JobInProgress(jf);
    jobs.add(jp);
    Thread.sleep(20000);
    new Thread(jp).start();
  }
}
