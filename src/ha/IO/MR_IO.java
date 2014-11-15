package ha.IO;

import ha.mapreduce.JobConf;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.rmi.RemoteException;

public class MR_IO {
  private NameNodeInterface stub;
  private int slaveID;
  private JobConf jc;
  
  public void requestWriteReplica(String localfile) throws UnknownHostException, IOException{
    InetSocketAddress remoteAddr=stub.loopupReplicaSlave(slaveID, localfile);
    requestWriteReplica(remoteAddr,localfile);
    
  }
  
  
  // write to the addr, and filename
  public void requestWriteReplica(InetSocketAddress addr, String localfile) throws UnknownHostException,
          IOException {
    Socket s = new Socket(addr.getHostName(), addr.getPort());
    OutputStream o = s.getOutputStream();
    o.write(localfile.getBytes()); // send filename first

    BufferedReader br = new BufferedReader(new FileReader(localfile));
    String line;

    while ((line = br.readLine()) != null) {
      o.write(line.getBytes());
    }

  }
  // in the slave side, accept and write the replica
  public void acceptWriteReplica(int listen_Port) throws IOException {
    ServerSocket server = new ServerSocket(listen_Port);
    Socket s = server.accept();
    InputStream in = s.getInputStream();
    BufferedReader reader = new BufferedReader(new InputStreamReader(in));
    String outputfile;
    String line;
    int num = 0;
    BufferedWriter bw;
    line = reader.readLine();
    
    //line may be null
     bw=new BufferedWriter(new FileWriter(new File(line)));
    
    while ((line = reader.readLine()) != null) {
      
        bw.write(line);

    }

  }


}
