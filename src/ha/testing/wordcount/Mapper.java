package ha.testing.wordcount;

import ha.mapreduce.OutputCollector;

public class Mapper extends ha.mapreduce.Mapper {

  @Override
  public void map(String key, String value, OutputCollector collector) {
    for (String token : value.split(" ")) {
      collector.collect(token, "1");
    }
  }

}
