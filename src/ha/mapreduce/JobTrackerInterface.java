package ha.mapreduce;

import java.io.IOException;
import java.rmi.Remote;
import java.rmi.RemoteException;

public interface JobTrackerInterface extends Remote{
  public String updateInformation(int JobID)throws RemoteException;

}
