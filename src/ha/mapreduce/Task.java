package ha.mapreduce;

import ha.IO.DistributedInputStream;
import ha.IO.NameNodeInterface;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;

public abstract class Task {
  protected DistributedInputStream isr;

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

  public void setup(TaskConf tc, NameNodeInterface nameNode) throws FileNotFoundException {
    isr = new DistributedInputStream(tc.getFilename(), nameNode);
    this.recordStart = tc.getRecordStart();
    this.recordCount = tc.getRecordCount();
    this.keySize = tc.getKeySize();
    this.valueSize = tc.getValueSize();
    this.jobID = tc.getJobID();
    this.collector = new OutputCollector(tc.getFilename() + "_" + tc.getJobID() + ".map", nameNode,
            keySize, valueSize);

  }

  protected abstract void process() throws IOException;

  public Integer getJobID() {
    return jobID;
  }
}
