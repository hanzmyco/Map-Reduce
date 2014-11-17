package ha.mapreduce;

import java.io.Serializable;

public class TaskStatus implements Serializable {
  private static final long serialVersionUID = 271962648479065747L;
  
  private Integer jobID, taskID, tipID;
  private Status status;
  private String type;
  
  public TaskStatus(TaskInProgress tip, String type) {
    tipID = tip.getID();
    Task task = tip.getTask();
    if (task != null) {
      taskID = tip.getTask().getTaskID();
      jobID = tip.getTask().getJobID();
    }
    status = tip.getStatus();
    this.type = type;
  }
  
  public int getJobID() {
    return jobID;
  }
  
  public int getTaskID() {
    return taskID;
  }
  
  public int getTipID() {
    return tipID;
  }
  
  public Status getStatus() {
    return status;
  }
  
  @Override
  public String toString() {
    String upTo = jobID == null ? "AVAILABLE" : "Task " + taskID + " of job " + jobID;
    return "[" + type + " " + tipID + "] => " + upTo;
  }
}
