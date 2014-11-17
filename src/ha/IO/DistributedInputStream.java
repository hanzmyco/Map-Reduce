package ha.IO;

import java.io.IOException;
import java.io.InputStream;

public class DistributedInputStream extends InputStream {
  long position;

  String filename;

  NameNodeInterface nameNode;

  public DistributedInputStream(String filename, NameNodeInterface nameNode) {
    this.filename = filename;
    this.nameNode = nameNode;
    this.position = 0;
  }

  @Override
  public int read() throws IOException {
    return nameNode.read(filename, position++, 1).charAt(0);
  }

  @Override
  public int read(byte[] arg0) throws IOException {
    System.arraycopy(nameNode.read(filename, position, arg0.length).toCharArray(), 0, arg0, 0,
            arg0.length);
    return arg0.length;
  }

  @Override
  public long skip(long n) {
    position += n;
    return n;
  }
}
