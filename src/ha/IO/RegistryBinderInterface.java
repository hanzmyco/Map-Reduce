package ha.IO;

import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;

public interface RegistryBinderInterface extends Remote {
  public void bind(String name, Remote thing) throws RemoteException, AlreadyBoundException;
  
  public Remote lookup(String name) throws RemoteException, NotBoundException;
}
