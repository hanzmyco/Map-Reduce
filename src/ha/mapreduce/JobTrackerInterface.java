package ha.mapreduce;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

public interface JobTrackerInterface extends Remote {
  public String updateInformation(int JobID) throws RemoteException;

  public List<TaskConf> getJobs(InetSocketAddress slave, int jobsAvailable) throws RemoteException;
}
