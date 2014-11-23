package ha.DFS;

import java.io.IOException;
import java.io.OutputStream;
import java.rmi.RemoteException;
/**
 * the IO used by dfs
 * @author hanz& amos
 *
 */
public class DistributedOutputStream extends OutputStream {
  /**
   * file we're writing to
   */
  String outputFilename;
  /**
   * name node to write to
   */
  NameNodeInterface nameNode;
  
  /**
   * creates that file if it doesn't already exist; overwrites everything
   */
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
  
  /**
   * write a key/value pair to DFS
   */
  public void write(byte[] key, byte[] value) throws IOException {
    write(key);
    write(value);
  }
}
