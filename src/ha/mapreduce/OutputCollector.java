package ha.mapreduce;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStreamWriter;
import java.net.InetSocketAddress;
import java.net.Socket;

public class OutputCollector {
  private static BufferedWriter bw;
  private String outputFile;
  private InetSocketAddress master;
  public OutputCollector(String outputFile){
    this.setOutputFile(outputFile);
  }
  
  public static void collect(String key, String value) throws IOException {
    bw.write(key+value);
    
    
  }

  public String getOutputFile() {
    return outputFile;
  }

  public void setOutputFile(String outputFile) {
    this.outputFile = outputFile;
  }
  public void write2Master(){
    Socket s = new Socket(master.getAddress(), master.getPort());
    BufferedWriter oos = new BufferedWriter(new OutputStreamWriter(s.getOutputStream()));
    
    Thread.sleep(500);    
    oos.write();
    oos.close();
    s.close();
    
  }
  
  
}
