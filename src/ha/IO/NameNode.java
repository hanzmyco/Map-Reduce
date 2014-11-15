package ha.IO;

import ha.mapreduce.JobConf;
import ha.mapreduce.JobTracker;
import ha.mapreduce.JobTrackerInterface;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.rmi.AlreadyBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;

public class NameNode implements NameNodeInterface {
  
  
  /*private InetSocketAddress namenodeRMI;
  
  public InetSocketAddress getNamenodeRMI() {
    return namenodeRMI;
  }

  public void setNamenodeRMI(InetSocketAddress namenodeRMI) {
    this.namenodeRMI = namenodeRMI;
  }
*/

  // table of slave id, slave ip, key is slave id, value is slave id
  private HashMap<Integer, InetSocketAddress> nodeList;

  // key is slave id, value is a list of file in the slave
  private HashMap<Integer, ArrayList<String>> fileList;

  // key is slave id, value is slave status, 1 is alive, 0 is down
  private HashMap<Integer, Integer> statusList;
  
  // get the ip and port of the replica, can be use in write or read
  public InetSocketAddress loopupReplicaSlave(int slaveID,String filename)throws RemoteException{
    
    if (slaveID <nodeList.size()-1){
      return nodeList.get(slaveID++);
    }
    else{
      return nodeList.get(0);
    }
    //System.out.println("fuck");
    //return null;
   
  }
  
  public NameNode(/*InetSocketAddress namenodeRMI*/){
    //this.namenodeRMI=namenodeRMI;
    nodeList=new HashMap<Integer,InetSocketAddress>();
    fileList=new HashMap<Integer,ArrayList<String>>();
    statusList=new HashMap<Integer,Integer>();
    //nodeList.put(0, null);
  }
  public HashMap<Integer, InetSocketAddress> getNodeList() {
    return nodeList;
  }


  public void setNodeList(HashMap<Integer, InetSocketAddress> nodeList) {
    this.nodeList = nodeList;
  }


  public HashMap<Integer, ArrayList<String>> getFileList() {
    return fileList;
  }


  public void setFileList(HashMap<Integer, ArrayList<String>> fileList) {
    this.fileList = fileList;
  }


  public HashMap<Integer, Integer> getStatusList() {
    return statusList;
  }


  public void setStatusList(HashMap<Integer, Integer> statusList) {
    this.statusList = statusList;
  }
  
  public static void main(String []args) throws RemoteException, AlreadyBoundException{
    JobConf dc = new JobConf(args[0]);
   

 
    Registry registry = LocateRegistry.createRegistry(dc.getRmiServer().getPort());
    

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
    
  }
  
  
  
  
  
  
  
  
}
