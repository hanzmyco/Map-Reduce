package ha.mapreduce;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

public interface JobTrackerInterface extends Remote {
  public String updateInformation(int JobID) throws RemoteException;

  /**
   * A slave asks for a certain number of more tasks to do. A list of map tasks (up to
   * tasksAvailabe) is returned
   */
  public List<TaskConf> getMapTasks(InetSocketAddress slave, int tasksAvailable)
          throws RemoteException;

  /**
   * A slave asks for a certain number of more tasks to do. A list of reduce tasks (up to
   * tasksAvailabe) is returned
   */
  public List<TaskConf> getReduceTasks(InetSocketAddress slave, int tasksAvailable)
          throws RemoteException;
}
