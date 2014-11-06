package ha.mapreduce;

public abstract class Mapper {
  public abstract void map(String key, String value, OutputCollector collector);
}
