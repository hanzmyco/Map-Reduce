package ha.mapreduce;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;

public class JobClient {

  private JobConf jconf;

  private int JobID;

  public JobClient(JobConf conf) {
    this.jconf = conf;
  }

  private String getRemoteID() {
    return null;
  }

  /**
   * @throws IOException
   * @throws InterruptedException
   * @throws ClassNotFoundException
   * 
   */
  private void sendConf() throws IOException, InterruptedException, ClassNotFoundException {
    String masterAddress = jconf.getMaster().getHostName();
    Integer masterPort = jconf.getMaster().getPort();
    System.out.println("[CLIENT] Connecting to master at " + masterAddress + ":" + masterPort
            + "...");
    Socket s = new Socket(masterAddress, masterPort);
    ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());

    oos.writeObject(jconf);

    Thread.sleep(500);
    oos.close();
    s.close();
  }

  /**
   * Send job over to master node and listen for
   */
  private void submitJob(JobConf conf) {
    conf.setJobID(getRemoteID());
    /*
     * get output configuration, compute input split
     */
    try {
      sendConf();
    } catch (Exception e) {
      System.out.println("error in submitting job");
      e.printStackTrace();
    }

    JobID = getJobID();

  }

  private int getJobID() {
    return 0;
  }

  private void getUpdates(JobTrackerInterface jt) throws RemoteException {
    while (true) {
      // poll for job status
      System.out.println(jt.updateInformation(1));
    }
  }

  public static void main(String[] args) {
    if (args.length != 2) {
      System.out.println("USAGE: java ha.mapreduce.JobClient <conf file> <RMI port>");
      System.exit(0);
    }

    JobConf conf = new JobConf(args[0]);
    System.out.println("[CLIENT] Setting up new job as such:");
    System.out.println(conf);
    JobClient client = new JobClient(conf);
    try {
      client.submitJob(conf);
    } catch (Exception e) {
      System.err.println("Could not send job conf over to master.");
      e.printStackTrace();
    }
    System.out.println("Sent job conf to master. Now listening for updates.");

    String port=args[1];
    try {
      
      Registry registry = LocateRegistry.getRegistry(Integer.parseInt(port));
      JobTrackerInterface stub = (JobTrackerInterface) registry.lookup("Hello");
      System.out.print("about to update");
      client.getUpdates(stub);
    } catch (Exception e) {
      System.err.println("Client exception: " + e.toString());
      e.printStackTrace();
    }
  }

}
