package ha.IO;

import java.io.IOException;
import java.io.OutputStream;
import java.rmi.RemoteException;

public class DistributedOutputStream extends OutputStream {
  String outputFilename;
  NameNodeInterface nameNode;
  
  public DistributedOutputStream(String filename, NameNodeInterface nameNode) {
    outputFilename = filename;
    this.nameNode = nameNode;
    try {
      nameNode.open(filename);
    } catch (RemoteException e) {
      System.err.println("Can't open file " + filename + "!");
      e.printStackTrace();
    }
  }
  
  @Override
  public void write(int arg0) throws IOException {
    byte[] arg = new byte[1];
    arg[0] = (byte) arg0;
    nameNode.write(outputFilename, new String(arg));
  }

  @Override
  public void write(byte[] arg0) throws IOException {
    nameNode.write(outputFilename, new String(arg0));
  }
  
  public void write(byte[] key, byte[] value) throws IOException {
    write(key);
    write(value);
  }
}
