package ha.mapreduce;

public abstract class Mapper {
  public abstract void map(String key, String value, OutputCollector collector);
  public void setup(){
    
  }
  public void mapAll(String inputfile,OutputCollector collector){
    // put map in loop
    
  }
  
}
