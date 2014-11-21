package ha.IO;

import ha.mapreduce.JobConf;
import ha.mapreduce.JobTrackerInterface;
import ha.mapreduce.TaskTracker;
import ha.mapreduce.TaskTrackerInterface;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.rmi.AlreadyBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
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
  public void write(String filename, byte[] key, byte[] value) throws RemoteException {
    write(filename, new String(key) + new String(value));
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

  @Override
  public String sayhello() throws RemoteException {
    return "I'm good, dude";
  }

  public static void main(String[] args) throws NumberFormatException, IOException {
    if (args.length != 2) {
      System.out.println("USAGE: java ha.mapreduce.DataNode <config file> <host:port>");
      System.exit(0);
    }

    JobConf conf = new JobConf(args[0]);
    InetSocketAddress thisMachine = JobConf.getInetSocketAddress(args[1]);

    try {

      Registry registry = conf.getRegistry();

      // every datanode(slave) has a namenode stub
      NameNodeInterface nameNode = (NameNodeInterface) registry.lookup("NameNode");

      Registry registry2 = LocateRegistry.createRegistry(thisMachine.getPort());
      String dataNodeName = thisMachine.toString() + " data node"; // this is what datanode name
                                                                   // look like, ip:port data node
      new DataNode(dataNodeName, registry2, thisMachine);
      nameNode.register(dataNodeName, thisMachine, true);
      System.out.println("finished datanode registry");

    } catch (Exception e) {
      System.err.println("[SLAVE] Error getting stub for JobTracker " + e.toString());
      e.printStackTrace();
    }
  }
}
