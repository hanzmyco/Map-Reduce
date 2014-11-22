package ha.mapreduce;

import ha.DFS.DistributedOutputStream;
import ha.DFS.NameNodeInterface;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class OutputCollector {
  private DistributedOutputStream os;

  private int keySize, valueSize;

  private SortedMap<String, List<String>> mappings;

  public OutputCollector(String outputFile, NameNodeInterface nameNode,
          int keySize, int valueSize) {
    this.keySize = keySize;
    this.valueSize = valueSize;
    this.mappings = new TreeMap<String, List<String>>();
    os = new DistributedOutputStream(outputFile, nameNode);
  }

  public void collect(String key, String value) {
    if (key.length() != keySize) {
      System.err.println("[COLLECTOR] Key \"" + key + "\" is not of size " + keySize + "!");
    } else if (value.length() != valueSize) {
      System.err.println("[COLLECTOR] Value \"" + value + "\" is not of size " + valueSize + "!");
    } else {
      if (!mappings.containsKey(key)) {
        mappings.put(key, new ArrayList<String>());
      }
      mappings.get(key).add(value);
    }
  }

  public void write2Disk() throws IOException {
    BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("local")));

    try {
      Thread.sleep(500);
    } catch (InterruptedException e) {
      System.err.println("Can't sleep thread!");
      e.printStackTrace();
    }

    for (Map.Entry<String, List<String>> mapping : mappings.entrySet()) {
      for (String value : mapping.getValue()) {
        bw.write(mapping.getKey() + value);
        os.write((mapping.getKey() + value).getBytes());
      }
    }

    bw.close();
  }

}
