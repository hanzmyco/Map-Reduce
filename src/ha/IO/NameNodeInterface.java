package ha.IO;

import java.net.InetSocketAddress;
import java.rmi.Remote;
import java.rmi.RemoteException;

public interface NameNodeInterface extends DataNodeInterface, Remote {
  public void put(String filename, InetSocketAddress host) throws RemoteException;

  public void register(String rmiName) throws RemoteException;
}
