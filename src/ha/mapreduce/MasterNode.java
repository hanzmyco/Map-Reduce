package ha.mapreduce;

import ha.DFS.NameNode;
import ha.DFS.NameNodeInterface;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.server.UnicastRemoteObject;

/**
 * 
 * @author hanz@amos
 * This is the main class used by system master and namenode computer, in our framework , we put master node and name node in one machine.
 */
public class MasterNode {
  public static void main(String[] args) throws NumberFormatException, IOException,
          ClassNotFoundException, InterruptedException, AlreadyBoundException, NotBoundException {
    if (args.length != 1) {
      System.out.println("USAGE: java ha.mapreduce.Master <conf file> ");
      System.exit(0);

    } else {
      // load system configuration file
      JobConf dc = new JobConf(args[0]);
      @SuppressWarnings("resource")
      ServerSocket newJobsSocket = new ServerSocket(dc.getMaster().getPort());

      // create RMI server
      Registry registry = LocateRegistry.createRegistry(dc.getRmiServer().getPort());

      // create and bind namenode to RMI server
      NameNode nameNodeOrigin = new NameNode(dc.getReplicaPerFile());
      NameNodeInterface nameNode = nameNodeOrigin;
      registry.bind("NameNode", (NameNodeInterface) UnicastRemoteObject.exportObject(nameNode, 0));
      new Thread(nameNodeOrigin).start();

      // create and bind jobtracker to RMI server
      JobTracker jobTracker = new JobTracker(nameNode);
      registry.bind("JobTracker",
              (JobTrackerInterface) UnicastRemoteObject.exportObject(jobTracker, 0));
      new Thread(jobTracker).start();

      while (true) {
        // keep asking for jobs 
        System.out.println("[MASTER] Waiting for new job on port " + "...");
        Socket jobsSocket = newJobsSocket.accept();
        ObjectInputStream newJobsStream = new ObjectInputStream(jobsSocket.getInputStream());
        ObjectOutputStream oos = new ObjectOutputStream(jobsSocket.getOutputStream());

        System.out.println("[MASTER] Got job configuration, starting a new job with it...");
        JobConf jc = (JobConf) newJobsStream.readObject();
        int JobID = jobTracker.startJob(jc);

        System.out.println("[MASTER] Writing back new job ID...");
        oos.writeInt(JobID);

        oos.flush();

        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          System.err.println("Can't sleep thread!");
          e.printStackTrace();
        }

        newJobsStream.close();
        jobsSocket.close();
        oos.close();
      }
    }
  }
}
