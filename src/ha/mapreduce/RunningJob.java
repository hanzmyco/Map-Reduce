package ha.mapreduce;

/**
 * 
 * @author hanz
 *
 */
public class RunningJob extends Job {
  
  private void linkJobTracker(){
    /*
     * use socket to link to jobtracker and check the status of the job, jobtracker will write to the port periodically to update the job status
     */
  }
  public RunningJob(JobConf jconf){
    this.setJobID(jconf.getJobID());
    this.setJobIP(jconf.getJobIP());
    this.setJobName(jconf.getJobName());
    this.setJobPort(jconf.getJobPort());
    
  }
  public String checkPeriod(){
    return null;
  }
  

}
