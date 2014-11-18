package ha.mapreduce;

import java.io.Serializable;

public class TaskConf implements Serializable {
  private static final long serialVersionUID = 1L;

  private String inputFile;

  private int recordStart, recordCount, keySize, valueSize, jobID, taskID;

  private Class<Task> taskClass;

  public TaskConf(String filename, int recordStart, int recordCount, int keySize, int valueSize,
          Class<Task> taskClass, int jobID, int taskID) {
    this.inputFile = filename;
    this.recordStart = recordStart;
    this.recordCount = recordCount;
    this.keySize = keySize;
    this.valueSize = valueSize;
    this.taskClass = taskClass;
    this.jobID = jobID;
    this.taskID = taskID;
  }

  @Override
  public boolean equals(Object arg0) {
    if (getClass() == arg0.getClass()) {
      return this.taskID == ((TaskConf) arg0).taskID;
    } else
      return false;
  }
  
  @Override
  public int hashCode() {
    return taskID;
  }

  public String getInputFilename() {
    return inputFile;
  }

  public String getOutputFilename() {
    return getInputFilename() + "_" + getTaskID() + ".map";
  }

  public int getRecordStart() {
    return recordStart;
  }

  public int getRecordEnd() {
    return recordStart + recordCount;
  }

  public int getStart() {
    return getRecordStart() * getRecordSize();
  }

  public int getEnd() {
    return getRecordEnd() * getRecordSize();
  }

  public int getRecordCount() {
    return recordCount;
  }

  public int getKeySize() {
    return keySize;
  }

  public int getValueSize() {
    return valueSize;
  }

  public int getRecordSize() {
    return keySize + valueSize;
  }

  /**
   * @return the taskClass
   */
  public Class<Task> getTaskClass() {
    return taskClass;
  }

  /**
   * @return the jobID
   */
  public int getJobID() {
    return jobID;
  }

  /**
   * @return the taskID
   */
  public int getTaskID() {
    return taskID;
  }
}
