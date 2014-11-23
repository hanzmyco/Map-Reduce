package ha.mapreduce;

import ha.DFS.DistributedInputStream;
import ha.DFS.DistributedOutputStream;
import ha.DFS.NameNodeInterface;

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

  private List<TaskConf> mapTasks, reduceTasks;

  private int finishedMapTasks = 0;

  private String sortedFilename;

  public JobConf getJobConf() {
    return jc;
  }

  public JobInProgress(JobConf jc, NameNodeInterface nameNode) throws IOException {
    this.jc = jc;
    this.nameNode = nameNode;
    mapTasks = new ArrayList<TaskConf>();
    reduceTasks = new ArrayList<TaskConf>();

    System.err.println("[JOB] Received new job conf as such:");
    System.err.println(jc);
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  private List<TaskConf> getTasks(int id, String inputFile, List<TaskConf> tasks, Class taskClass) throws IOException {
    try {
      long numBytes = nameNode.getFileSize(inputFile), numRecords = numBytes / jc.getRecordSize(), recordIndex = 0, prevRecordIndex = 0;
      int numSplits = (int) Math.ceil(numRecords * 1.0 / recordsPerSplit);
      System.out.println("[JOB " + jc.getJobID() + "] Input file has "
              + (numBytes / jc.getRecordSize()) + " records");
      DistributedInputStream dis = new DistributedInputStream(inputFile, nameNode);
      byte[] key = new byte[jc.getKeySize()], value = new byte[jc.getValueSize()], tempkey = new byte[jc
              .getKeySize()], tempvalue = new byte[jc.getValueSize()];
      for (int i = 0; i < numSplits && recordIndex < numRecords; i++) {
        prevRecordIndex = recordIndex;
        int j = 0;
        while(j < recordsPerSplit && recordIndex < numRecords) {
          dis.read(key, value);
          recordIndex++;
          j++;
        }
        dis.read(tempkey, tempvalue);
        recordIndex++;
        while(recordIndex < numRecords && tempkey == key) {
          System.err.println(new String(tempkey) + " and " + new String(key) + " are the same!");
          dis.read(tempkey, tempvalue);
          recordIndex++;
          j++;
        }
        if (tempkey == key) {
          j++;
        } else {
          recordIndex--;
        }
        TaskConf newTask = new TaskConf(inputFile, (int) prevRecordIndex, j,
                jc.getKeySize(), jc.getValueSize(), taskClass, jc.getJobID(), id++,
                nameNode.getFileSize(inputFile));
        tasks.add(newTask);
        System.out.println("[JOB " + jc.getJobID() + "] Created Task #" + newTask.getTaskID()
                + " responsible for records starting at " + newTask.getRecordStart());
      }
      System.out.println("[JOB " + jc.getJobID() + "] Created " + tasks.size() + " new tasks");
      return tasks;
    } catch (RemoteException e) {
      e.printStackTrace();
      return null;
    }
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  public List<TaskConf> getMapTasks(int id) {
    try {
      return getTasks(id, jc.getInputFile(), mapTasks, (Class<Task>) (Class) jc.getMapperClass());
    } catch (IOException e) {
      System.err.println("Oh no!");
      e.printStackTrace();
      return null;
    }
  }

  /**
   * Alert this JIP that some map task is finished. Returns whether or not the entire map stage is
   * finished.
   */
  public boolean mapTaskFinished() {
    finishedMapTasks++;
    return finishedMapTasks == mapTasks.size();
  }

  public String mergeFiles(String file1, String file2, String outputFile) throws IOException {
    DistributedInputStream is1 = new DistributedInputStream(file1, nameNode), is2 = new DistributedInputStream(
            file2, nameNode);
    DistributedOutputStream os = new DistributedOutputStream(outputFile, nameNode);
    byte[] key1 = new byte[jc.getKeySize()], key2 = new byte[jc.getKeySize()], value1 = new byte[jc
            .getValueSize()], value2 = new byte[jc.getValueSize()];
    long records1 = nameNode.getFileSize(file1) / jc.getRecordSize(), records2 = nameNode
            .getFileSize(file2) / jc.getRecordSize(), i1 = 0, i2 = 0;
    is1.read(key1, value1);
    is2.read(key2, value2);

    while (i1 < records1 && i2 < records2) {
      System.err.println("[JOB IN PROGRESS] Comparing \"" + new String(key1) + "\" and \""
              + new String(key2) + "\"");
      if (new String(key1).compareTo(new String(key2)) < -1) {
        os.write(key1, value1);
        if (++i1 < records1)
          is1.read(key1, value1);
      } else {
        os.write(key2, value2);
        if (++i2 < records2)
          is2.read(key2, value2);
      }
    }

    if (i1 < records1)
      os.write(key1, value1);
    while (++i1 < records1) {
      is1.read(key1, value1);
      os.write(key1, value1);
    }
    if (i2 < records2)
      os.write(key2, value2);
    while (++i2 < records2) {
      is2.read(key2, value2);
      os.write(key2, value2);
    }

    is1.close();
    is2.close();
    os.close();

    return outputFile;
  }

  public void sortMapOutput() throws IOException {
    String baseSortedFile = jc.getInputFile() + "_sorted_", prevFile = mergeFiles(mapTasks.get(0)
            .getOutputFilename(), mapTasks.get(1).getOutputFilename(), baseSortedFile + 1 + ".map");
    for (int i = 2; i < mapTasks.size(); i++) {
      prevFile = mergeFiles(prevFile, mapTasks.get(i).getOutputFilename(), baseSortedFile + i
              + ".map");
    }
    sortedFilename = prevFile;
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  public List<TaskConf> getReduceTasks(int id) {
    try {
      return getTasks(id, sortedFilename, reduceTasks, (Class<Task>) (Class) jc.getReducerClass());
    } catch (IOException e) {
      System.err.println("Aww shucks");
      e.printStackTrace();
      return null;
    }
  }

}
