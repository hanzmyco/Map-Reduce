package ha.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public abstract class Reducer extends Task {
  public abstract void reduce(String key, Collection<String> values, OutputCollector collector);
  
  @Override
  public void process() throws IOException {
    byte[] key = new byte[taskConf.getKeySize()], value = new byte[taskConf.getValueSize()];
    String previousKey = "";
    List<String> values = new ArrayList<String>();
    isr.skip(taskConf.getStart());
    for (int i = 0; i < taskConf.getRecordCount(); i++) {
      if (isr.read(key) == -1) break;
      if (isr.read(value) == -1) break;
      String currentKey = new String(key), currentValue = new String(value);
      
      if (currentKey.equals(previousKey)) { // add to list of values to reduce
        values.add(currentValue);
      } else { // reduce what we have so far
        if (!previousKey.isEmpty()) {
          reduce(previousKey, values, collector);
        }
        
        previousKey = currentKey;
        values = new ArrayList<String>();
        values.add(currentValue);
      }
    }
    
    // done with everything but last key
    if (!previousKey.isEmpty()) {
      reduce(previousKey, values, collector);
    }
  }
}
