package ha.IO;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface DataNodeInterface extends Remote {
  public byte[] read(String filename, long start, int length) throws RemoteException;

  public void write(String filename, String stuff) throws RemoteException;
  
  public void write(String filename, byte[] key, byte[] value) throws RemoteException;

  public void open(String filename) throws RemoteException;

  public long getFileSize(String filename) throws RemoteException;
  
  public String sayhello()throws RemoteException;
}
