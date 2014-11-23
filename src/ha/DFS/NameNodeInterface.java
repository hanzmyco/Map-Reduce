package ha.DFS;

import java.net.InetSocketAddress;
import java.rmi.Remote;
import java.rmi.RemoteException;

public interface NameNodeInterface extends DataNodeInterface, Remote {
  public void put(String filename, String rmiName) throws RemoteException;

  public void register(String rmiName, InetSocketAddress rmi_location, boolean writable) throws RemoteException;
  
  public void heartBeat() throws RemoteException;
  
  
}
