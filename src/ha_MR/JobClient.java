package ha_MR;

import java.net.InetSocketAddress;

import Hadoop.Configuration;



public class JobClient {
  public static RunningJob running;
  private JobConf jconf;
  public JobClient(){
    
  }
  @Deprecated
  public JobClient(Configuration conf){
    
  }
  public JobClient(InetSocketAddress jobTrackAddr, Configuration conf) {
    
  }
  public JobClient(JobConf conf) {
    this.jconf=conf;
    
  }
  
  @Deprecated
  public static RunningJob runJob(JobConf job){
    JobClient jc = new JobClient(job);
    running = jc.submitJob(job);
    return null;
    
  }
  public RunningJob runJob()
  {
    return submitJob(jconf);
  }
  
  private String getRemoteID(){
    return null;
  }
  private void uploadResources(){
    
  }
  private void tellJobTracker(){
    
  }
  private RunningJob submitJob(JobConf conf){
    /**
     * 向JobTracker请求一个新的job ID
      *检测此job的output配置
      *计算此job的input split
      *将Job运行所需的资源拷贝到JobTracker的文件系统中的文件夹中，包括job.jar文件、job.xml配置文件，input splits
      *通知JobTracker此Job已经可以运行了
      *提交任务后，runJob每隔一秒钟轮询一次job的进度，将进度返回到命令行，直到任务运行完毕。
     */
    conf.setJobID(getRemoteID());
    /*
     * get output configuration, compute input split
     */
    uploadResources();
    
    tellJobTracker();
    
    return new RunningJob(conf);
    
    
    
    
    
    
  }
  RunningJob submitJob(String jobFile){
    return null;
    
  }
  public static void main(String[] args) {
    // TODO Auto-generated method stub
    String conf="configuration";
    JobClient client=new JobClient(new JobConf(conf));
    RunningJob rjob=client.runJob();
    
    while(true){
      rjob.checkPeriod();
    }

  }

  
}

