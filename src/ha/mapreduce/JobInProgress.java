package ha.mapreduce;

import ha.IO.DistributedInputStream;
import ha.IO.NameNodeInterface;

import java.io.IOException;
import java.io.InputStream;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class JobInProgress {
  private JobConf jc;

  private NameNodeInterface nameNode;

  private int recordsPerSplit = 2;

  List<TaskConf> mapTasks;

  public JobConf getJobConf() {
    return jc;
  }

  public JobInProgress(JobConf jc, NameNodeInterface nameNode) throws IOException {
    this.jc = jc;
    this.nameNode = nameNode;

    System.err.println("[JOB] Received new job conf as such:");
    System.err.println(jc);
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  public List<TaskConf> getMapTasks(int id) {
    try {
      long numRecords = nameNode.getFileSize(jc.getInputFile());
      int numSplits = (int) Math.ceil(numRecords * 1.0 / (recordsPerSplit * jc.getRecordSize()));
      System.out.println("[JOB " + jc.getJobID() + "] Input file has " + numRecords + " records");
      mapTasks = new ArrayList<TaskConf>();
      for (int i = 0; i < numSplits; i++) {
        TaskConf newTask = new TaskConf(jc.getInputFile(), i * recordsPerSplit, recordsPerSplit,
                jc.getKeySize(), jc.getValueSize(), (Class<Task>) (Class) jc.getMapperClass(),
                jc.getJobID(), id++);
        mapTasks.add(newTask);
        System.out.println("[JOB " + jc.getJobID() + "] Created Task #" + newTask.getTaskID()
                + " responsible for records starting at " + newTask.getRecordStart());
      }
      System.out.println("[JOB " + jc.getJobID() + "] Created " + mapTasks.size()
              + " new map tasks");
      return mapTasks;
    } catch (RemoteException e) {
      e.printStackTrace();
      return null;
    }
  }

  public String mergeFiles(String file1, String file2, String outputFile) throws IOException {
    DistributedInputStream is1 = new DistributedInputStream(file1, nameNode), is2 = new DistributedInputStream(
            file2, nameNode);
    byte[] key1 = new byte[jc.getKeySize()], key2 = new byte[jc.getKeySize()], value1 = new byte[jc
            .getValueSize()], value2 = new byte[jc.getValueSize()];
    long records1 = nameNode.getFileSize(file1) / jc.getRecordSize(), records2 = nameNode
            .getFileSize(file2) / jc.getRecordSize(), i1 = 0, i2 = 0;
    is1.read(key1, value1);
    is2.read(key2, value2);

    while (i1 < records1 && i2 < records2) {
      if (new String(key1).compareTo(new String(key2)) == -1) {
        nameNode.write(outputFile, key1);
        nameNode.write(outputFile, value1);
        is1.read(key1, value1);
      } else {
        nameNode.write(outputFile, key2);
        nameNode.write(outputFile, value2);
        is2.read(key2, value2);
      }
    }

    while (i1 < records1) {
      is1.read(key1, value1);
      nameNode.write(outputFile, key1);
      nameNode.write(outputFile, value1);
    }
    while (i2 < records2) {
      is2.read(key2, value2);
      nameNode.write(outputFile, key2);
      nameNode.write(outputFile, value2);
    }

    is1.close();
    is2.close();
    
    return outputFile;
  }

  public String getSortedFile() throws IOException {
    String baseSortedFile = jc.getInputFile() + "_sorted_", prevFile = mergeFiles(mapTasks.get(0)
            .getOutputFilename(), mapTasks.get(1).getOutputFilename(), baseSortedFile + 1);
    for (int i = 2; i < mapTasks.size(); i++) {
      prevFile = mergeFiles(prevFile, mapTasks.get(i).getOutputFilename(), baseSortedFile + i);
    }
    return prevFile;
  }

  public List<TaskConf> getReduceTasks() {
    return new ArrayList<TaskConf>();
  }

}
