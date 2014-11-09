package ha.mapreduce;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class Master {
  public static void main(String[] args) throws NumberFormatException, IOException, ClassNotFoundException {
    if (args.length != 1) {
      System.out.println("USAGE: java ha.mapreduce.Master <port>");
    } else {
      @SuppressWarnings("resource")
      ServerSocket newJobsSocket = new ServerSocket(Integer.parseInt(args[0]));
      
      
      JobTracker jobTracker = new JobTracker();
      new Thread(jobTracker).start();

      while (true) {
        System.out.println("[MASTER] Waiting for new job on port " + args[0] + "...");
        Socket jobsSocket = newJobsSocket.accept();
        ObjectInputStream newJobsStream = new ObjectInputStream(jobsSocket.getInputStream());
        
        System.out.println("[MASTER] Got job configuration, starting a new job with it...");
        JobConf jc = (JobConf) newJobsStream.readObject();
        jobTracker.startJob(jc);
        
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          System.err.println("Can't sleep thread!");
          e.printStackTrace();
        }
        
        newJobsStream.close();
        jobsSocket.close();
      }
    }
  }
}
