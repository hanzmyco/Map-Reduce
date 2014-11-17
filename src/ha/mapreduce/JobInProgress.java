package ha.mapreduce;

import ha.IO.NameNodeInterface;

import java.io.IOException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class JobInProgress {
  private JobConf jc;

  private NameNodeInterface nameNode;

  private int recordsPerSplit = 2; //

  // key is taskID, value is split integer
  private HashMap<Integer, Integer> taskMappings;

  public HashMap<Integer, Integer> getRecord() {
    return taskMappings;
  }

  public void setRecord(HashMap<Integer, Integer> record) {
    this.taskMappings = record;
  }

  public JobConf getJobConf() {
    return jc;
  }

  public JobInProgress(JobConf jc, NameNodeInterface nameNode) throws IOException {
    this.jc = jc;
    taskMappings = new HashMap<Integer, Integer>();
    this.nameNode = nameNode;

    System.err.println("[JOB] Received new job conf as such:");
    System.err.println(jc);
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  public List<TaskConf> getMapTasks(int id) {
    try {
      long numRecords = nameNode.getFileSize(jc.getInputFile());
      int numSplits = (int) Math.ceil(numRecords * 1.0 / (recordsPerSplit * jc.getRecordSize()));
      List<TaskConf> tasks = new ArrayList<TaskConf>();
      for (int i = 0; i < numSplits; i++) {
        TaskConf newTask = new TaskConf(jc.getInputFile(), i * recordsPerSplit, recordsPerSplit,
                jc.getKeySize(), jc.getValueSize(), (Class<Task>) (Class) jc.getMapperClass(),
                jc.getJobID(), id++);
        tasks.add(newTask);
        System.out.println("[JOB " + jc.getJobID() + "] Created Task #" + newTask.getTaskID()
                + " responsible for records starting at " + newTask.getRecordStart());
      }
      System.out.println("[JOB " + jc.getJobID() + "] Created " + tasks.size() + " new map tasks");
      return tasks;
    } catch (RemoteException e) {
      e.printStackTrace();
      return null;
    }
  }

  public List<TaskConf> getReduceTasks() {
    return new ArrayList<TaskConf>();
  }

}
