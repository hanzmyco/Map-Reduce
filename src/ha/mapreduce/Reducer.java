package ha.mapreduce;

import java.util.Collection;

public abstract class Reducer extends Task {
  public abstract void reduce(String key, Collection<String> values, OutputCollector collector);
}
