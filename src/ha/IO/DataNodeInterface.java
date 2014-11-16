package ha.IO;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface DataNodeInterface extends Remote {
  public String read(String filename, int start, int end) throws RemoteException;

  public void write(String filename, String stuff) throws RemoteException;

  public void open(String filename) throws RemoteException;

  public long getFileSize(String filename) throws RemoteException;
}
