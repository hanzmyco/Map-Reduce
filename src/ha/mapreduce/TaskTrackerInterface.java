package ha.mapreduce;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

public interface TaskTrackerInterface extends Remote {
  public List<TaskStatus> getTaskStatuses() throws RemoteException;
  public String sayhello()throws RemoteException;
}
