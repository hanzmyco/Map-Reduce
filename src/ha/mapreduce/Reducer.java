package ha.mapreduce;

import java.util.Collection;

public abstract class Reducer {
  public abstract void map(String key, Collection<String> values, OutputCollector collector);
}
