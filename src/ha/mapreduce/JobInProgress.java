package ha.mapreduce;

import java.net.InetSocketAddress;
import java.util.List;

public class JobInProgress {
  private JobConf jc;
  public JobInProgress(JobConf jc){
    this.jc=jc;
  }
  public void runJob(){
    List<InetSocketAddress> slaveList=jc.getSlaves();
    int mapperNum=slaveList.size()*jc.getMappersPerSlave();
    int reduceNum=slaveList.size()*jc.getReducersPerSlave();
    
    // generate the number of TaskTracker the task track will launch mapper task and reducer task
    
    
    
    
    
  }
  

}
