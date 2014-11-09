package ha.mapreduce;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

public class JobInProgress implements Runnable {
  private JobConf jc;
  public JobInProgress(JobConf jc) throws IOException {
    this.jc=jc;
    
    System.err.println("[JOB] Received new job conf as such:");
    System.err.println(jc);
  }
  
  public void run() {
    List<InetSocketAddress> slaveList=jc.getSlaves();
    int mapperNum=slaveList.size()*jc.getMappersPerSlave();
    int reduceNum=slaveList.size()*jc.getReducersPerSlave();
    // generate the number of TaskTracker the task track will launch mapper task and reducer task
    
    
    
    
    
  }
  

}
