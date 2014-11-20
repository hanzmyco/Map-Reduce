package ha.IO;

import java.rmi.RemoteException;

public class DFSHeartbeat implements Runnable{
  private NameNodeInterface n1;
  
  public DFSHeartbeat(NameNodeInterface n1){
    this.n1=n1;
  }
  
  
  
  

  @Override
  public void run() {
    // TODO Auto-generated method stub
    while (true) {
      try {
        System.out.println("[Name Node] sending heart beat for datanodes!");
        n1.heartBeat();
        Thread.sleep(5000);
      } catch (RemoteException e) {
        System.err.println("[Name NODE] Unable to ask for status of the Datanode!");
        e.printStackTrace();
      } catch (InterruptedException e) {
        System.err.println("[Name Node] Cannot sleep thread!");
        e.printStackTrace();
      }
    }
  }

}
