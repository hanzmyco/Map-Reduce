package ha.mapreduce;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * https://github.com/michaelzhhan1990/hadoop-mapreduce/blob/HDFS-641/src/java/org/apache/hadoop/
 * mapred/JobTracker.java
 * 
 * @author hanz
 *
 */
public class JobTracker {
  /*
   * 当JobTracker收到submitJob调用的时候，将此任务放到一个队列中，job调度器� �从队列中获�?�任务并�?始化任务。
   * �?始化首先创建一个对象�?��?装job�?行的tasks�?status以�?�progress。 在创建task�
   * ���?，job调度器首先从共享文件系统中获得JobClient计算出的input split。 其为�?个input split创建一个map task。�?个task被分�?一个ID。
   */

  private ServerSocket serverSocket;

  public JobTracker(int commandPort) {
    try {
      serverSocket = new ServerSocket(commandPort);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  private void listenForConnections() {
    try {
      Socket acceptedSocket = serverSocket.accept();

      ObjectInputStream ois = new ObjectInputStream(acceptedSocket.getInputStream());

      JobConf jf = (JobConf) ois.readObject();
      
      Thread.sleep(500);
      ois.close();
      
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

  public static void main(String[] args) {
    if (args.length != 1)
    {
        System.out.println("USAGE: java ha.rmi.RegistryServer <port>");
    }
    else
    {
        JobTracker master = new JobTracker(Integer.parseInt(args[0]));
        
        while (true)
        {
            master.listenForConnections();
        }
    }
    

  }

}
