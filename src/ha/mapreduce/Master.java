package ha.mapreduce;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.AlreadyBoundException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;







public class Master {  
  public static void main(String[] args) throws NumberFormatException, IOException, ClassNotFoundException, InterruptedException, AlreadyBoundException {
    if (args.length != 1) {
      System.out.println("USAGE: java ha.mapreduce.Master <port>");
    } else {
      @SuppressWarnings("resource")
      ServerSocket newJobsSocket = new ServerSocket(Integer.parseInt(args[0]));
      
      Master m=new Master();
      JobTracker jobTracker = new JobTracker();
      //JobTrackerInterface jobTracker=new JobTracker();
      //JobTracker jobTracker = new JobTracker();
      JobTrackerInterface jt=(JobTrackerInterface)UnicastRemoteObject.exportObject(jobTracker, 0);
      Registry registry = LocateRegistry.createRegistry(1111);
      registry.bind("Hello", jt);
      
      

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
