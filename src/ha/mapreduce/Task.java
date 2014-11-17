package ha.mapreduce;

import ha.IO.NameNodeInterface;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;

public abstract class Task {
  protected NameNodeInterface nameNode;
  
  protected String filename;

  protected OutputCollector collector;

  protected int recordStart;

  protected int recordCount;

  protected int keySize;

  protected int valueSize;

  protected Integer jobID = null;

  public void setup(TaskConf tc) throws FileNotFoundException {
    this.nameNode = tc.getNameNode();
    this.filename = tc.getFilename();
    this.recordStart = tc.getRecordStart();
    this.recordCount = tc.getRecordCount();
    this.keySize = tc.getKeySize();
    this.valueSize = tc.getValueSize();
    this.jobID = tc.getJobID();
    this.collector = new OutputCollector(filename + "_" + jobID + ".map",
            tc.getRmiName(), nameNode, keySize, valueSize);
  }

  protected abstract void process() throws IOException;

  @Deprecated
  public void run() throws IOException {
    process();
    collector.write2Disk();

  }

  public Integer getJobID() {
    return jobID;
  }
}
