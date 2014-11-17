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

      Registry registry = conf.getRegistry();

      // every datanode(slave) has a namenode stub
      NameNodeInterface nameNode = (NameNodeInterface) registry.lookup("NameNode");

      Registry registry2 = LocateRegistry.createRegistry(thisMachine.getPort());
      String dataNodeName = thisMachine.toString() + " data node";
      new DataNode(dataNodeName, registry2, thisMachine);
      nameNode.register(dataNodeName, thisMachine, true);
      System.out.println("finished datanode registry");

      JobTrackerInterface jt = (JobTrackerInterface) registry.lookup("JobTracker");
      TaskTracker tt = new TaskTracker(thisMachine, nameNode, jt, conf.getMappersPerSlave(),
              conf.getReducersPerSlave());
      new Thread(tt).start();
      registry2.bind("tt", (TaskTrackerInterface) UnicastRemoteObject.exportObject(tt, 0));
      jt.registerAsSlave("tt", thisMachine);
    } catch (Exception e) {
      System.err.println("[SLAVE] Error getting stub for JobTracker " + e.toString());
      e.printStackTrace();
    }
  }
}
