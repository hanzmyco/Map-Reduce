package ha.IO;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

public class DataNode implements DataNodeInterface {
  public DataNode(String myName, Registry registry, NameNodeInterface nameNode) {
    try {
      registry.bind(myName, (DataNodeInterface) UnicastRemoteObject.exportObject(this, 0));
      nameNode.register(myName);
    } catch (RemoteException | AlreadyBoundException | NotBoundException e) {
      System.err.println("Cannot bind " + myName);
      e.printStackTrace();
    }
  }

  public DataNode() {

  }

  @Override
  public String read(String filename, int start, int end) throws RemoteException {
    try {
      FileReader fr = new FileReader(filename);
      int length = end - start;
      System.out.println(length);
      char[] characters = new char[length];
      fr.skip(start);
      fr.read(characters, 0, length);
      fr.close();
      return new String(characters);
    } catch (IOException e) {
      System.err.println("Can't read from local file " + filename);
      e.printStackTrace();
    }
    return null;
  }

  @Override
  public void write(String filename, String stuff) throws RemoteException {
    try {
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