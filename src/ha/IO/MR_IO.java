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
import java.io.OutputStreamWriter;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.rmi.RemoteException;

public class MR_IO  {
  private NameNodeInterface stub;

  private int slaveID;

  private JobConf jc;

  public MR_IO(NameNodeInterface stub, int slaveID, JobConf jc) {
    this.stub = stub;
    this.slaveID = slaveID;
    this.jc = jc;

  }
  public MR_IO(){
    
  }

  // write the local file to next node as replica
  public void requestWriteReplica(String localfile) throws UnknownHostException, IOException {
    //InetSocketAddress remoteAddr = stub.loopupReplicaSlave(slaveID, localfile);
    //requestWriteReplica(remoteAddr, localfile);

  }

  // write to the addr, and filename, this can be use as write any file through network
  public void requestWriteReplica(InetSocketAddress addr, String localfile)
          throws UnknownHostException, IOException {
    Socket s = new Socket(addr.getHostName(), addr.getPort());
   
    OutputStream o = s.getOutputStream();
    BufferedWriter bufOut = new BufferedWriter( new OutputStreamWriter( o ) );
    //write filename first
    bufOut.write( localfile + ".rep" );
    bufOut.newLine(); //
    bufOut.flush();

    BufferedReader br = new BufferedReader(new FileReader(localfile));
    String line;

    while ((line = br.readLine()) != null) {
      bufOut.write(line);
      bufOut.newLine();
      bufOut.flush();
    }

  }

  // write part of the file to desination , can be used by jobclient to write
  public void requestWriteReplica(InetSocketAddress addr, String localfile,int begin_line, int end_line)
          throws UnknownHostException, IOException {
    Socket s = new Socket(addr.getHostName(), addr.getPort());
    
    OutputStream o = s.getOutputStream();
    BufferedWriter bufOut = new BufferedWriter( new OutputStreamWriter( o ) );
    //write filename first
    bufOut.write( localfile + ".rep" );
    bufOut.newLine(); //
    bufOut.flush();
    
    
   

    BufferedReader br = new BufferedReader(new FileReader(localfile));
    String line;
    int index=1;

    while ((line = br.readLine()) != null) {
      if (index>=begin_line && index<=end_line){
        bufOut.write(line);
        bufOut.newLine();
        bufOut.flush();
      }
      index++;
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

    // line may be null
    bw = new BufferedWriter(new FileWriter(new File(line)));

    while ((line = reader.readLine()) != null) {

      bw.write(line);
      bw.newLine();
      bw.flush();

    }

  }



}
