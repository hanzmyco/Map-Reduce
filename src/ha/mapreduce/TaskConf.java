package ha.mapreduce;

import java.io.Serializable;

public class TaskConf implements Serializable {
  private static final long serialVersionUID = 1L;

  private String filename;

  private int recordStart, recordCount, keySize, valueSize, jobID;

  private Class<Task> taskClass;

  public TaskConf(String filename, int recordStart, int recordCount, int keySize, int valueSize,
          Class<Task> taskClass, int jobID) {
    this.filename = filename;
    this.recordStart = recordStart;
    this.recordCount = recordCount;
    this.keySize = keySize;
    this.valueSize = valueSize;
    this.taskClass = taskClass;
    this.jobID = jobID;
  }

  public String getFilename() {
    return filename;
  }

  public int getRecordStart() {
    return recordStart;
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
}
