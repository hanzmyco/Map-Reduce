package ha.mapreduce;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

public class JobInProgress {
  private JobConf jc;
  public JobConf getJc() {
    return jc;
  }

  public void setJc(JobConf jc) {
    this.jc = jc;
  }

  private int inputSplit;  //how many splits
  private int nextSplit;   // coming split index, starts from 1,
  private int lineperSplit;    //
  
  public int getInputSplit() {
    return inputSplit;
  }

  public void setInputSplit(int inputSplit) {
    this.inputSplit = inputSplit;
  }

  public int getNextSplit() {
    return nextSplit;
  }

  public void setNextSplit(int nextSplit) {
    this.nextSplit = nextSplit;
  }

  public int getlineperSplit() {
    return lineperSplit;
  }

  public void setLastline(int lastline) {
    this.lineperSplit = lastline;
  }


  
  public JobInProgress(JobConf jc) throws IOException {
    this.jc=jc;
    
    System.err.println("[JOB] Received new job conf as such:");
    System.err.println(jc);
  }
  
  public int inputSplit(String filename){  
    // check if the file exceeds a certain amoung of data
    return 0;
    
  }
  
  /*
  public void run() {    
    // generate the number of TaskTracker the task track will launch mapper task and reducer task
    
    for (InetSocketAddress slave : jc.getSlaves()) {
      try {
        System.out.println("[JOB-IN-PROGRESS] Connecting to slave at " + slave.getAddress() + ":" + slave.getPort() + "...");
        Socket slaveSocket = new Socket(slave.getAddress(), slave.getPort());
        ObjectOutputStream oos = new ObjectOutputStream(slaveSocket.getOutputStream());
        oos.writeObject(jc.getMappersPerSlave());
        oos.writeObject(jc.getMapperClass());
        
        Thread.sleep(1000);
        
        System.out.println("[JOB-IN-PROGRESS] Sent " + jc.getMappersPerSlave() + " mappers of " + jc.getMapperClass() + " over to slave.");
        
        oos.close();
        slaveSocket.close();
        
        // Check that map tasks have finished first
        
        slaveSocket = new Socket(slave.getAddress(), slave.getPort());
        oos = new ObjectOutputStream(slaveSocket.getOutputStream());
        
        oos.writeObject(jc.getReducersPerSlave());
        oos.writeObject(jc.getReducerClass());
        
        Thread.sleep(1000);
        
        System.out.println("[JOB-IN-PROGRESS] Sent " + jc.getReducersPerSlave() + " reducers of " + jc.getReducerClass() + " over to slave.");
        
        oos.close();
        slaveSocket.close();
      } catch (IOException e) {
        System.err.println("TODO: Check that failure here can be taken care of");
        e.printStackTrace();
      } catch (InterruptedException e) {
        System.err.println("Cannot sleep thread!");
        e.printStackTrace();
      }
    }
  }*/
  

}
