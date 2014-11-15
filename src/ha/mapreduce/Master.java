package ha.mapreduce;

import ha.IO.NameNode;
import ha.IO.NameNodeInterface;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.AlreadyBoundException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;

public class Master {
  public static void main(String[] args) throws NumberFormatException, IOException,
          ClassNotFoundException, InterruptedException, AlreadyBoundException {
    if (args.length !=1) {
      System.out.println("USAGE: java ha.mapreduce.Master <conf file> ");
      System.exit(0);

    } else {
      @SuppressWarnings("resource")
      // local default file
      JobConf dc = new JobConf(args[0]);
      ServerSocket newJobsSocket = new ServerSocket(dc.getMaster().getPort());

      // bind jobtracker to rmi server
      JobTracker jobTracker = new JobTracker();
      JobTrackerInterface jt = (JobTrackerInterface) UnicastRemoteObject
              .exportObject(jobTracker, 0);
      Registry registry = LocateRegistry.createRegistry(dc.getRmiServer().getPort());
      registry.bind("JobTracker", jt);

      // start namenode, bind it to rmi server
      // set tup namenode
      // InetSocketAddress namenode=jc.getNamenode();
      NameNode namenode = new NameNode();
      ArrayList<InetSocketAddress> ls = (ArrayList<InetSocketAddress>) dc.getDatanodes();
      HashMap<Integer, InetSocketAddress> temp = new HashMap<Integer, InetSocketAddress>();
      for (int i = 0; i < ls.size(); i++) {
        temp.put(i, ls.get(i));

      }
      namenode.setNodeList(temp);
      NameNodeInterface nf = (NameNodeInterface) UnicastRemoteObject.exportObject(namenode, 0);
      registry.bind("NameNode", nf);
      System.out.println("namenode ready");

      while (true) {
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
