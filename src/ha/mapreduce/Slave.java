package ha.mapreduce;

import ha.IO.DataNode;
import ha.IO.NameNodeInterface;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

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

      new DataNode(thisMachine.toString() + " data node", registry, nameNode);

      new Thread(new TaskTracker(thisMachine, (JobTrackerInterface) registry.lookup("JobTracker"),
              conf.getMappersPerSlave(), conf.getReducersPerSlave())).start();
    } catch (Exception e) {
      System.err.println("[SLAVE] Error getting stub for JobTracker " + e.toString());
      e.printStackTrace();
    }
  }
}
