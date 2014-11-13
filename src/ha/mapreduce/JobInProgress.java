package ha.mapreduce;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class JobInProgress {
  private JobConf jc;

  private int inputSplit; // how many splits

  private int nextSplit; // coming split index, starts from 1,

  private int lineperSplit; //
  
  // key is taskID, value is split integer
  private HashMap<Integer, Integer> taskMappings;

  public HashMap<Integer, Integer> getRecord() {
    return taskMappings;
  }

  public void setRecord(HashMap<Integer, Integer> record) {
    this.taskMappings = record;
  }

  public JobConf getJc() {
    return jc;
  }

  public void setJc(JobConf jc) {
    this.jc = jc;
  }

  public int getInputSplit() {
    return inputSplit;
  }

  public void setInputSplit(int inputSplit) {
    this.inputSplit = inputSplit;
  }

  public int getNextSplit() {
    return nextSplit;
  }

  public void setNextSplit(int nextSplit) {
    this.nextSplit = nextSplit;
  }

  public int getlineperSplit() {
    return lineperSplit;
  }

  public void setlineperSplit(int lastline) {
    this.lineperSplit = lastline;
  }

  public JobInProgress(JobConf jc) throws IOException {
    this.jc = jc;
    taskMappings= new HashMap<Integer,Integer>();
    setInputSplit(inputSplit(jc.getInputFile()));
    setlineperSplit(10);
    setNextSplit(1);
    
    
    System.err.println("[JOB] Received new job conf as such:");
    System.err.println(jc);
  }

  public int inputSplit(String filename) {
    // check if the file exceeds a certain amoung of data
    return 5;

  }

}
