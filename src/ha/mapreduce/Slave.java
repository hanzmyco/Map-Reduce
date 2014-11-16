package ha.mapreduce;

import ha.IO.DataNode;
import ha.IO.DataNodeInterface;
import ha.IO.NameNodeInterface;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

public class Slave {
  public static void main(String[] args) throws NumberFormatException, IOException {
    if (args.length != 2) {
      System.out.println("USAGE: java ha.mapreduce.Slave <config file> <host:port>");
      System.exit(0);
    }

    JobConf conf = new JobConf(args[0]);
    InetSocketAddress thisMachine = JobConf.getInetSocketAddress(args[1]);

    try {
      
      
      
      
      
      Registry registry = (Registry) LocateRegistry.getRegistry(
              conf.getRmiServer().getHostString(), conf.getRmiServer().getPort());

      // every datanode(slave) has a namenode stub
      NameNodeInterface nameNode = (NameNodeInterface) registry.lookup("NameNode");

      
      Registry registry2 = LocateRegistry.createRegistry(thisMachine.getPort());
      DataNode dn=new DataNode(thisMachine.toString() + " data node",registry2,thisMachine,nameNode);
      System.out.println("finished datanode registry");
      
      
      
      
      
      

      new Thread(new TaskTracker(thisMachine, (JobTrackerInterface) registry.lookup("JobTracker"),
              conf.getMappersPerSlave(), conf.getReducersPerSlave())).start();
    } catch (Exception e) {
      System.err.println("[SLAVE] Error getting stub for JobTracker " + e.toString());
      e.printStackTrace();
    }
  }
}
