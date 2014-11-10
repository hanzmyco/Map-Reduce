package ha.testing.wordcount;

import ha.mapreduce.OutputCollector;

public class Mapper extends ha.mapreduce.Mapper {

  @Override
  public void map(String key, String value, OutputCollector collector) {
    System.err.println("TODO: implement mapper!");
  }

}
