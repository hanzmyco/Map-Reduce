package ha.IO;


import java.net.InetSocketAddress;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

public class NameNode implements NameNodeInterface {

  // table of slave id, slave ip, key is slave id, value is slave id

  private HashMap<String, DataNodeInterface> stubMap;
  
  private HashMap<DataNodeInterface, Boolean> writables;

  private HashMap<String, List<DataNodeInterface>> filelocations;

  // key is datanode id, value is slave status, 1 is alive, 0 is down
  private HashMap<DataNodeInterface, Integer> statusList;

  // private Registry r;

  public NameNode() throws RemoteException, NotBoundException {

    stubMap = new HashMap<String, DataNodeInterface>();
    writables = new HashMap<DataNodeInterface, Boolean>();
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
  public byte[] read(String filename, long start, int length) throws RemoteException {
    return getStubFor(filename).read(filename, start, length);
  }

  private void allocateDataNodes(String filename, int n) {
    Iterator<DataNodeInterface> dnit = stubMap.values().iterator();
    while (filelocations.containsKey(filename) && filelocations.get(filename).size() >= n) {
      if (!dnit.hasNext()) {
        System.err.println("Not enough data nodes to allocate!");
        break;
      }
      
      DataNodeInterface dni = dnit.next();
      if (writables.get(dni)) {
        addToFileLocations(filename, dni);
      }
    }
  }
  
  @Override
  public void write(String filename, byte[] stuff) throws RemoteException {
    allocateDataNodes(filename, 2);
    for (DataNodeInterface dataNode : filelocations.get(filename)) {
      dataNode.write(filename, stuff);
    }
  }

  @Override
  public void write(String filename, String stuff) throws RemoteException {
    allocateDataNodes(filename, 2);
    for (DataNodeInterface dataNode : filelocations.get(filename)) {
      dataNode.write(filename, stuff);
    }
  }

  @Override
  public void open(String filename) throws RemoteException {
    allocateDataNodes(filename, 2);
    for (DataNodeInterface dataNode : filelocations.get(filename)) {
      dataNode.open(filename);
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
  public void register(String rmiName, InetSocketAddress rmi_location, boolean writable) throws RemoteException {
    try {
      stubMap.put(
              rmiName,
              (DataNodeInterface) LocateRegistry.getRegistry(rmi_location.getHostString(),
                      rmi_location.getPort()).lookup(rmiName));
      writables.put(stubMap.get(rmiName), writable);
    } catch (NotBoundException e) {
      e.printStackTrace();
    }

  }
}
