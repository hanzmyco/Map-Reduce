package ha.IO;

import java.net.InetSocketAddress;
import java.rmi.Remote;
import java.rmi.RemoteException;

public interface NameNodeInterface extends Remote {
  public String read(String filename, int start, int end) throws RemoteException;
  
  public void write(String filename, String stuff) throws RemoteException;
  
  public void open(String filename) throws RemoteException;
  
  public void put(String filename, InetSocketAddress host) throws RemoteException;
  
  public Integer getFileSize(String filename) throws RemoteException;
  
  public void register(String rmiName) throws RemoteException;
}
