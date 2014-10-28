package ha_MR;
/**
 * https://github.com/michaelzhhan1990/hadoop-mapreduce/blob/HDFS-641/src/java/org/apache/hadoop/mapred/JobTracker.java
 * @author hanz
 *
 */
public class JobTracker {
  /*
   * 当JobTracker收到submitJob调用的时候，将此任务放到一个队列中，job调度器将从队列中获取任务并初始化任务。
   * 初始化首先创建一个对象来封装job运行的tasks、status以及progress。
   *在创建task之前，job调度器首先从共享文件系统中获得JobClient计算出的input split。
   *其为每个input split创建一个map task。每个task被分配一个ID。
   */

}
