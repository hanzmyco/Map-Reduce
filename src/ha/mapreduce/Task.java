package ha.mapreduce;

import ha.IO.MR_IO;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;

public abstract class Task {
  protected InputStreamReader isr;
  
  protected OutputCollector collector;
  public OutputCollector getCollector() {
    return collector;
  }

  public void setCollector(OutputCollector collector) {
    this.collector = collector;
  }

  protected int recordStart;
  protected int recordCount;
  protected int keySize;
  protected int valueSize;
  protected Integer jobID = null;
  
  
  public void setup(TaskConf tc) throws FileNotFoundException {
    isr = new InputStreamReader(new FileInputStream(tc.getFilename()));
    this.recordStart = tc.getRecordStart();
    this.recordCount = tc.getRecordCount();
    this.keySize = tc.getKeySize();
    this.valueSize = tc.getValueSize();
    this.jobID = tc.getJobID();
    this.collector = new OutputCollector(tc.getFilename() + "_" + recordStart + ".map", keySize, valueSize);
    
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
