package ha.testing.IO;

import ha.IO.MR_IO;
import ha.IO.NameNodeInterface;
import ha.mapreduce.JobConf;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class test_Filewrite {
  
  public static void main(String []args) throws NotBoundException, UnknownHostException, IOException{
    if (args.length != 1) {
      System.out.println("USAGE: java ha.mapreduce.JobClient <conf file> ");
      System.exit(0);
    }

    JobConf conf = new JobConf(args[0]);
    System.out.println("[CLIENT] Setting up new job as such:");
    System.out.println(conf);
 
    
    Registry registry = LocateRegistry.getRegistry(conf.getRmiServer().getHostString(), conf
            .getRmiServer().getPort());
    
    // every datanode(slave) has a namenode stub 
    NameNodeInterface stub=(NameNodeInterface)registry.lookup("NameNode");
    stub.loopupReplicaSlave(0, "fuck");
    System.out.println("finished use namenode");
    
    
    MR_IO io=new MR_IO(stub,0,conf);
    InetSocketAddress this_node=conf.getDatanodes().get(0);
    InetSocketAddress other_node=conf.getDatanodes().get(1);
    String localfile="../test";
    System.out.println(other_node.toString());
    System.out.println(this_node.toString());
    //io.acceptWriteReplica(other_node.getPort());
    io.requestWriteReplica(other_node, localfile,1,3);
    

  
  }
  
  
  

}
