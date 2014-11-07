package ha.mapreduce;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;



/**
 * https://github.com/michaelzhhan1990/hadoop-mapreduce/blob/HDFS-641/src/java/org/apache/hadoop/
 * mapred/JobTracker.java
 * 
 * @author hanz
 *
 */
public class JobTracker {

  private ServerSocket serverSocket;
  private JobConf jf;
  private List<JobInProgress> jlist;

  public JobTracker(int commandPort) {
    try {
      serverSocket = new ServerSocket(commandPort);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    jlist=new ArrayList<JobInProgress>();
    
  }

  private void listenForJob() {
    try {
      Socket acceptedSocket = serverSocket.accept();

      ObjectInputStream ois = new ObjectInputStream(acceptedSocket.getInputStream());

      jf = (JobConf) ois.readObject();
      System.out.println(jf.getMaster().getAddress());      
      Thread.sleep(500);
      ois.close();
      JobInProgress jp=new JobInProgress (jf);
      jlist.add(jp);
      
    } catch (IOException e) {
      System.err.println("Error encountered while listening for client!");
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      System.err.println("Error encountered while trying to read object from client!");
      e.printStackTrace();
    } catch (Exception e) {
      System.err.println(e.getMessage());
    }
  }
  private void launchJob(){
    while(!jlist.isEmpty()){
      JobInProgress jp=jlist.remove(0);
      jp.runJob();
    }
  }
  
  

  public static void main(String[] args) {
    if (args.length != 1)
    {
        System.out.println("USAGE: java ha.mapreduce.RegistryServer <port>");
    }
    else
    {
        JobTracker master = new JobTracker(Integer.parseInt(args[0]));
        
        while (true)
        {
            master.listenForJob();
        }
    }
    

  }

}
