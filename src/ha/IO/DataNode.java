package ha.IO;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.rmi.AlreadyBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

public class DataNode implements DataNodeInterface {

  public DataNode(String myName, Registry registry, InetSocketAddress thisMachine) {
    try {
      registry.bind(myName, (DataNodeInterface) UnicastRemoteObject.exportObject(this, 0));
    } catch (RemoteException | AlreadyBoundException e) {
      System.err.println("Cannot bind " + myName);
      e.printStackTrace();
    }
  }

  public DataNode() {

  }

  @Override
  public byte[] read(String filename, long start, int length) throws RemoteException {
    try {
      FileInputStream fr = new FileInputStream(filename);
      byte[] characters = new byte[length];
      fr.skip(start);
      fr.read(characters, 0, length);
      fr.close();
      return characters;
    } catch (IOException e) {
      System.err.println("Can't read from local file " + filename);
      e.printStackTrace();
    }
    return null;
  }

  @Override
  public void write(String filename, String stuff) throws RemoteException {
    try {
      new File(filename).getParentFile().mkdirs();
      FileWriter fw = new FileWriter(filename, true);
      fw.append(stuff);
      fw.close();
    } catch (IOException e) {
      System.err.println("Can't write to local file " + filename);
      e.printStackTrace();
    }
  }

  @Override
  public void open(String filename) throws RemoteException {
    try {
      new File(filename).getParentFile().mkdirs();
      FileWriter fw = new FileWriter(filename);
      fw.close();
    } catch (IOException e) {
      System.err.println("Can't open local file " + filename);
      e.printStackTrace();
    }
  }

  @Override
  public long getFileSize(String filename) throws RemoteException {
    return new File(filename).length();
  }
}
