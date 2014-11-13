package ha.mapreduce;

import java.io.IOException;

public abstract class Mapper extends Task {
  public abstract void map(String key, String value, OutputCollector collector);

  @Override
  public void process() throws IOException {
    int recordSize = this.keySize + this.valueSize;
    char[] key = new char[keySize], value = new char[valueSize];
    for (int offset = recordStart * recordSize; offset < (recordStart + recordCount) * recordSize; offset += recordSize) {
      if (isr.read(key, offset, keySize) == -1) break;
      if (isr.read(value, offset, valueSize) == -1) break;
      map(new String(key), new String(value), collector);
    }
  }
}
