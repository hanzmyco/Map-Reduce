package ha.IO;

import ha.mapreduce.JobConf;
import ha.mapreduce.JobTracker;
import ha.mapreduce.JobTrackerInterface;

import java.io.FileNotFoundException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

public class NameNode implements NameNodeInterface {

  // table of slave id, slave ip, key is slave id, value is slave id

  private HashMap<String, DataNodeInterface> stubMap;

  private HashMap<String, List<DataNodeInterface>> filelocations;

  // key is datanode id, value is slave status, 1 is alive, 0 is down
  private HashMap<DataNodeInterface, Integer> statusList;

  // private Registry r;



  public NameNode(JobConf jc) throws RemoteException, NotBoundException {


    stubMap = new HashMap<String, DataNodeInterface>();
    filelocations = new HashMap<String, List<DataNodeInterface>>();
    statusList = new HashMap<DataNodeInterface, Integer>();


  }

  private void addToFileLocations(String filename, DataNodeInterface dataNode) {
    if (!filelocations.containsKey(filename)) {
      filelocations.put(filename, new ArrayList<DataNodeInterface>());
    }
    filelocations.get(filename).add(dataNode);
  }

  private DataNodeInterface getStubFor(String filename) {
    List<DataNodeInterface> stubs = filelocations.get(filename);
    if (stubs == null) {
      System.err.println("[NAME NODE] Location for file " + filename + " is null!");
      return null; // pass the NullPointerException on, but at least we know the culprit
    } else {
      return stubs.get(0);
    }
  }

  @Override
  public String read(String filename, int start, int end) throws RemoteException {
    return getStubFor(filename).read(filename, start, end);
  }

  private void allocateDataNodes(String filename, int n) {
    Iterator<DataNodeInterface> dnit = stubMap.values().iterator();
    // write to first two by default
    addToFileLocations(filename, dnit.next());
    addToFileLocations(filename, dnit.next());
  }

  @Override
  public void write(String filename, String stuff) throws RemoteException {
    if (filelocations.containsKey(filename)) {
      for (DataNodeInterface dataNode : filelocations.get(filename)) {
        dataNode.write(filename, stuff);
      }
    } else { // make sure it exists then
      allocateDataNodes(filename, 2);
      write(filename, stuff);
    }
  }

  @Override
  public void open(String filename) throws RemoteException {
    if (filelocations.containsKey(filename)) {
      for (DataNodeInterface dataNode : filelocations.get(filename)) {
        dataNode.open(filename);
      }
    } else { // make sure it exists then
      allocateDataNodes(filename, 2);
      open(filename);
    }
  }

  // what if have existing file
  @Override
  public void put(String filename, String rmiName) throws RemoteException {
    addToFileLocations(filename, stubMap.get(rmiName));
  }

  @Override
  public long getFileSize(String filename) throws RemoteException {
    return getStubFor(filename).getFileSize(filename);
  }

  @Override
  public void register(String rmiName, InetSocketAddress rmi_location) throws RemoteException,
          NotBoundException {

    stubMap.put(
            rmiName,
            (DataNodeInterface) LocateRegistry.getRegistry(rmi_location.getHostString(),
                    rmi_location.getPort()).lookup(rmiName));

  }
}
