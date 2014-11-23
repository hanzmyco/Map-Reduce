package ha.testing.uniquewords;

import ha.mapreduce.OutputCollector;
import ha.mapreduce.Utils;

public class Mapper extends ha.mapreduce.Mapper {

  @Override
  public void map(String key, String value, OutputCollector collector) {
    for (String token : value.split(" ")) {
      collector.collect(Utils.padRight(token, key.length()), Utils.padLeft("exists", value.length()));
    }
  }

}
