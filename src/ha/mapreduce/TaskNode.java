package ha.mapreduce;

import ha.DFS.DataNode;
import ha.DFS.DataNodeInterface;
import ha.DFS.NameNodeInterface;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
/**
 * run on task node, which is so called slave
 * @author hanz& amos
 *
 */
public class TaskNode {
  public static void main(String[] args) throws NumberFormatException, IOException {
    if (args.length != 2) {
      System.out.println("USAGE: java ha.mapreduce.TaskNode <config file> <host:port>");
      System.exit(0);
    }

    JobConf conf = new JobConf(args[0]);
    InetSocketAddress thisMachine = JobConf.getInetSocketAddress(args[1]);

    try {
            
      Registry registry = conf.getRegistry();
      // get name node stub first
      NameNodeInterface nameNode = (NameNodeInterface) registry.lookup("NameNode");
      //register the task node on rmi registry server
      Registry registry2 = LocateRegistry.createRegistry(thisMachine.getPort());
      //get jobtracker stub
      JobTrackerInterface jt = (JobTrackerInterface) registry.lookup("JobTracker");
      TaskTracker tt = new TaskTracker(thisMachine, nameNode, jt, conf.getMappersPerSlave(),
              conf.getReducersPerSlave());
      // launch task tracker, each slave has one task tracker
      new Thread(tt).start();
      registry2.bind("tt", (TaskTrackerInterface) UnicastRemoteObject.exportObject(tt, 0));
      //register the slave in master node using master stub
      jt.registerAsSlave("tt", thisMachine);
    } catch (Exception e) {
      System.err.println("[SLAVE] Error getting stub for JobTracker " + e.toString());
      e.printStackTrace();
    }
  }
}
