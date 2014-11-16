package ha.IO;

import java.net.InetSocketAddress;
import java.rmi.RemoteException;

public interface DataNodeInterface {
  public String read(String filename, int start, int end) throws RemoteException;

  public void write(String filename, String stuff) throws RemoteException;

  public void open(String filename) throws RemoteException;

  public Integer getFileSize(String filename) throws RemoteException;
}
