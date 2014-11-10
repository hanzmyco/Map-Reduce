package ha.mapreduce;

/**
 * it handle one machine, how many mapper(map tasks), how many reducer tasks
 * it will construct the TaskInProgress
 * the JobTracker will use rmi or rpc to call the for tasktracker and then tasktracker will generate taskinprogress
 * @author hanz
 *
 */
public class TaskTracker {
  private int mapperNum; //how many mapper in the node
  private int reducerNum; //how many reducer in the node
  private Class mapperClass;
  private Class reducerClass;


}
