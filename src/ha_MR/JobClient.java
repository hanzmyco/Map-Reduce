package ha_MR;

import java.net.InetSocketAddress;

import Hadoop.Configuration;



public class JobClient {
  public static RunningJob running;
  public JobClient(){
    
  }
  
  public JobClient(Configuration conf){
    
  }
  public JobClient(InetSocketAddress jobTrackAddr, Configuration conf) {
    
  }
  public JobClient(JobConf conf) {
    
  }
  public static RunningJob runJob(JobConf job){
    JobClient jc = new JobClient(job);
    running = jc.submitJob(job);
    return null;
    
  }
  RunningJob submitJob(JobConf conf){
    /**
     * 向JobTracker请求一个新的job ID
      *检测此job的output配置
      *计算此job的input split
      *将Job运行所需的资源拷贝到JobTracker的文件系统中的文件夹中，包括job.jar文件、job.xml配置文件，input splits
      *通知JobTracker此Job已经可以运行了
      *提交任务后，runJob每隔一秒钟轮询一次job的进度，将进度返回到命令行，直到任务运行完毕。
     */
    return null;
    
  }
  RunningJob submitJob(String jobFile){
    return null;
    
  }

}
