package ha.IO;

import java.net.InetSocketAddress;
import java.rmi.Remote;
import java.rmi.RemoteException;

public interface NameNodeInterface extends Remote {
  public InetSocketAddress loopupReplicaSlave(int slaveID,String filename)throws RemoteException;

}
