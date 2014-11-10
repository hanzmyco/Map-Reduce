package ha.testing.wordcount;

import ha.mapreduce.OutputCollector;

import java.util.Collection;

public class Reducer extends ha.mapreduce.Reducer {

  @Override
  public void reduce(String key, Collection<String> values, OutputCollector collector) {
    System.err.println("TODO: Implement reducer");
  }

}
