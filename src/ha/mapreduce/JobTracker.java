package ha.mapreduce;
/**
 * https://github.com/michaelzhhan1990/hadoop-mapreduce/blob/HDFS-641/src/java/org/apache/hadoop/mapred/JobTracker.java
 * @author hanz
 *
 */
public class JobTracker {
  /*
   * 当JobTracker收到submitJob调用的时候，将此任务放到一个队列中，job调度器将从队列中获�?�任务并�?始化任务。
   * �?始化首先创建一个对象�?��?装job�?行的tasks�?status以�?�progress。
   *在创建task之�?，job调度器首先从共享文件系统中获得JobClient计算出的input split。
   *其为�?个input split创建一个map task。�?个task被分�?一个ID。
   */

}
