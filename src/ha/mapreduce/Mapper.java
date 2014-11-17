package ha.mapreduce;

import java.io.IOException;

public abstract class Mapper extends Task {
  public abstract void map(String key, String value, OutputCollector collector);

  @Override
  public void process() throws IOException {
    byte[] key = new byte[taskConf.getKeySize()], value = new byte[taskConf.getValueSize()];
    for (int offset = taskConf.getStart(); offset < taskConf.getEnd(); offset += taskConf.getRecordSize()) {
      System.out.println("[MAPPER " + taskConf.getTaskID() + "] Reading from " + offset + " to " + (offset + taskConf.getRecordSize()));
      if (isr.read(key, offset, taskConf.getKeySize()) == -1) break;
      if (isr.read(value, offset, taskConf.getValueSize()) == -1) break;
      System.out.println("[MAPPER " + taskConf.getTaskID() + "] Calling map function on (" + new String(key) + ", " + new String(value) + ")");
      map(new String(key), new String(value), collector);
    }
    collector.write2Disk();
  }
}
