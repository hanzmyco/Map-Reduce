package ha.mapreduce;

import java.io.IOException;

public abstract class Mapper extends Task {
  public abstract void map(String key, String value, OutputCollector collector);

  @Override
  public void process() throws IOException {
    byte[] key = new byte[taskConf.getKeySize()], value = new byte[taskConf.getValueSize()];
    isr.skip(taskConf.getStart());
    for (int i = 0; i < taskConf.getEndingRecord(); i++) {
      isr.read(key, value);
      System.out.println("[MAPPER " + taskConf.getTaskID() + "] Calling map function on (" + new String(key) + ", " + new String(value) + ")");
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      map(new String(key), new String(value), collector);
    }
    collector.write2Disk();
  }
}
