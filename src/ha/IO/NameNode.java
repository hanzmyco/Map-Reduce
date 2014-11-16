package ha.IO;

import ha.mapreduce.JobConf;
import ha.mapreduce.JobTracker;
import ha.mapreduce.JobTrackerInterface;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;

public class NameNode implements NameNodeInterface {

  // table of slave id, slave ip, key is slave id, value is slave id

  private HashMap<String, DataNodeInterface> stubMap;

  private HashMap<String, String> filelocation;

  // key is datanode id, value is slave status, 1 is alive, 0 is down
  private HashMap<String, Integer> statusList;

  private Registry r;

  public HashMap<String, String> getFilelocation() {
    return filelocation;
  }

  public void setFilelocation(HashMap<String, String> filelocation) {
    this.filelocation = filelocation;
  }

  public HashMap<String, DataNodeInterface> getStubMap() {
    return stubMap;
  }

  public void setStubMap(HashMap<String, DataNodeInterface> stubMap) {
    this.stubMap = stubMap;
  }

  // get the ip and port of the replica, can be use in write or read
  /*
   * public InetSocketAddress loopupReplicaSlave(int slaveID, String filename) throws
   * RemoteException {
   * 
   * if (slaveID < nodeList.size() - 1) { return nodeList.get(slaveID++); } else { return
   * nodeList.get(0); } // System.out.println("fuck"); // return null;
   * 
   * }
   */
  public NameNode(Registry r) {
    this.r=r;
    
    stubMap=new HashMap<String,DataNodeInterface>();
    filelocation = new HashMap<String, String>();
    statusList = new HashMap<String, Integer>();

  }

  public HashMap<String, Integer> getStatusList() {
    return statusList;
  }

  public void setStatusList(HashMap<String, Integer> statusList) {
    this.statusList = statusList;
  }

  @Override
  public String read(String filename, int start, int end) throws RemoteException {

    return stubMap.get(filelocation.get(filename)).read(filename, start, end);

  }

  @Override
  public void write(String filename, String stuff) throws RemoteException {

    stubMap.get(filelocation.get(filename)).write(filename, stuff);

  }

  @Override
  public void open(String filename) throws RemoteException {
    stubMap.get(filelocation.get(filename)).open(filename);

  }

  @Override
  public void put(String filename, String rmiName) throws RemoteException {
    filelocation.put(filename, rmiName);

  }

  @Override
  public long getFileSize(String filename) throws RemoteException {

    return stubMap.get(filelocation.get(filename)).getFileSize(filename);
  }

  @Override
  public void register(String rmiName) throws RemoteException, NotBoundException {
    stubMap.put(rmiName, (DataNodeInterface) r.lookup(rmiName));

  }

  /*
   * public static void main(String[] args) throws RemoteException, AlreadyBoundException { JobConf
   * dc = new JobConf(args[0]);
   * 
   * Registry registry = LocateRegistry.createRegistry(dc.getRmiServer().getPort());
   * 
   * // start namenode, bind it to rmi server // set tup namenode // InetSocketAddress
   * namenode=jc.getNamenode(); NameNode namenode = new NameNode(); ArrayList<InetSocketAddress> ls
   * = (ArrayList<InetSocketAddress>) dc.getDatanodes(); HashMap<Integer, InetSocketAddress> temp =
   * new HashMap<Integer, InetSocketAddress>(); for (int i = 0; i < ls.size(); i++) { temp.put(i,
   * ls.get(i));
   * 
   * } namenode.setNodeList(temp); NameNodeInterface nf = (NameNodeInterface)
   * UnicastRemoteObject.exportObject(namenode, 0); registry.bind("NameNode", nf);
   * System.out.println("namenode ready");
   * 
   * }
   */
}
