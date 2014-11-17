package ha.mapreduce;

import ha.IO.DistributedInputStream;
import ha.IO.NameNodeInterface;

import java.io.FileNotFoundException;
import java.io.IOException;

public abstract class Task {
  protected DistributedInputStream isr;

  protected OutputCollector collector;
  
  protected TaskConf taskConf;

  public OutputCollector getCollector() {
    return collector;
  }

  public void setCollector(OutputCollector collector) {
    this.collector = collector;
  }

  public void setup(TaskConf tc, NameNodeInterface nameNode) throws FileNotFoundException {
    isr = new DistributedInputStream(tc.getFilename(), nameNode);
    taskConf = tc;
    this.collector = new OutputCollector(tc.getFilename() + "_" + tc.getTaskID() + ".map", nameNode,
            tc.getKeySize(), tc.getValueSize());
  }

  protected abstract void process() throws IOException;

  public Integer getJobID() {
    return taskConf.getJobID();
  }
  
  public Integer getTaskID() {
    return taskConf.getTaskID();
  }
}
