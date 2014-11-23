package ha.testing.IO;

import ha.DFS.NameNodeInterface;
import ha.mapreduce.JobConf;

import java.io.IOException;
import java.net.UnknownHostException;
import java.rmi.NotBoundException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class Read {
  public static void main(String[] args) throws NotBoundException, UnknownHostException,
          IOException {
    if (args.length != 1) {
      System.out.println("USAGE: java ha.testing.IO.Write <conf file> ");
      System.exit(0);
    }

    JobConf conf = new JobConf(args[0]);

    Registry registry = LocateRegistry.getRegistry(conf.getRmiServer().getHostString(), conf
            .getRmiServer().getPort());

    // every datanode(slave) has a namenode stub
    NameNodeInterface stub = (NameNodeInterface) registry.lookup("NameNode");
    
    System.out.println(stub.read("hello.txt", 4, 6));
  }
}
