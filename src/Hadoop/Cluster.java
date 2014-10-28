package Hadoop;

import ha_MR.Configuration;

import java.net.InetSocketAddress;

public class Cluster extends Object{
  private Configuration conf;
  
  Cluster(Configuration conf){
    this.conf=conf;
  }
  
  Cluster(InetSocketAddress jobTrackAddr, Configuration conf){
    
    
  }

}
