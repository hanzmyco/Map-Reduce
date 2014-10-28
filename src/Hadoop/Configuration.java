package Hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import IO.Writable;


public class Configuration extends Object implements Iterable<Map.Entry<String, String>>,Writable{
  private boolean loadDefaults;
  private Map updatingResource;
  private ArrayList<Object> resources;
  private Properties properties;
  private Properties overlay;
  private ClassLoader classLoader;
  private Set<String> finalParameters;
  private static 
  private static ArrayList<String> defaultResources;
  
  public Configuration(){
    this(true);
  }
  public Configuration(boolean loadDefaults){
    System.out.println("Configuration(boolean loadDefaults)");
    this.loadDefaults = loadDefaults;// 选择是否加载默认配置文件，false为不加载，true加载
    System.out.println("loadDefaults: " + loadDefaults);
    updatingResource = new HashMap<String, String[]>();// 保存修改过的配置项
    
    /*
    synchronized (Configuration.class) {
        REGISTRY.put(this, null);
    }*/
    
  }
  /**
   * 
   * @param 调用其它Configuration对象的配置文件
   */
  @SuppressWarnings("unchecked")
  public Configuration(Configuration other) {
      this.resources = (ArrayList<Resource>) other.resources.clone();
      synchronized (other) {
          if (other.properties != null) {
              this.properties = (Properties) other.properties.clone();
          }

          if (other.overlay != null) {
              this.overlay = (Properties) other.overlay.clone();
          }

          this.updatingResource = new HashMap<String, String[]>(
                  other.updatingResource);
      }

      this.finalParameters = new HashSet<String>(other.finalParameters);
      synchronized (Configuration.class) {
          REGISTRY.put(this, null);
      }
      this.classLoader = other.classLoader;
      this.loadDefaults = other.loadDefaults;
      setQuietMode(other.getQuietMode());
  }
  
  
  public void addResource(String name) { // 以CLASSPATH资源为例  
    addResourceObject(name);  
  }  
  private synchronized void addResourceObject(Object resource) {  
    resources.add(resource);// 添加到成员变量resources中  
    reloadConfiguration();  
  }  
  public synchronized void reloadConfiguration() {  
    properties = null;// 会触发资源的重新加载  
    finalParameters.clear();  
  }  
  
  //public void addResource(Path file)  {}
  /*
   * local resource
   */
  public void addResource(InputStream in){
    
  }
  public void addResource(URL url){
    
  }
  
  
  
  public static synchronized void addDefaultResource(String name) {  
    if(!defaultResources.contains(name)) {  
       defaultResources.add(name);  
       for(Configuration conf : REGISTRY.keySet()) {  
          if(conf.loadDefaults) {  
             conf.reloadConfiguration(); // 触发资源的重新加载  
          }  
       }  
    }  
  } 

  
  
  
  
  
  
  
  
  
  
  
  

  @Override
  public void write(DataOutput out) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void readFields(DataInput in) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public Iterator<Entry<String, String>> iterator() {
    // TODO Auto-generated method stub
    return null;
  }

}
