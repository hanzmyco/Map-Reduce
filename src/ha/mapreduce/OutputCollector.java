package ha.mapreduce;

import ha.IO.NameNodeInterface;

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
  private String outputFile, rmiName;

  private NameNodeInterface nameNode;

  private int keySize, valueSize;

  private SortedMap<String, List<String>> mappings;

  public OutputCollector(String outputFile, String rmiName, NameNodeInterface nameNode,
          int keySize, int valueSize) {
    this.setOutputFile(outputFile);
    this.keySize = keySize;
    this.valueSize = valueSize;
    this.mappings = new TreeMap<String, List<String>>();
    this.nameNode = nameNode;
    this.rmiName = rmiName;
  }

  public void collect(String key, String value) throws IOException {
    if (key.length() != keySize) {
      System.err.println("Key \"" + key + "\" is not of size " + keySize + "!");
    } else if (key.length() != keySize) {
      System.err.println("Value \"" + value + "\" is not of size " + valueSize + "!");
    } else {
      if (!mappings.containsKey(key)) {
        mappings.put(key, new ArrayList<String>());
      }
      mappings.get(key).add(value);
    }
  }

  public String getOutputFile() {
    return outputFile;
  }

  public void setOutputFile(String outputFile) {
    this.outputFile = outputFile;
  }

  public void write2Disk() throws IOException {
    BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputFile)));

    try {
      Thread.sleep(500);
    } catch (InterruptedException e) {
      System.err.println("Can't sleep thread!");
      e.printStackTrace();
    }

    for (Map.Entry<String, List<String>> mapping : mappings.entrySet()) {
      for (String value : mapping.getValue()) {
        bw.write(mapping.getKey() + value);
      }
    }

    bw.close();

    nameNode.put(outputFile, rmiName);
  }

}
