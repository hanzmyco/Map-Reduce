package ha.mapreduce;

public abstract class Mapper extends Task {
  public abstract void map(String key, String value, OutputCollector collector);
  public void setup(){
    
  }

  public void process(String inputfile, OutputCollector collector){
    // put map in loop
    
  }
  
}
