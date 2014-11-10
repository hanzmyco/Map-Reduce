package ha.mapreduce;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;

public class JobClient {
  public static RunningJob running;

  private JobConf jconf;

  public JobClient() {

  }

  public JobClient(JobConf conf) {
    this.jconf = conf;
  }

  public RunningJob runJob() {
    return submitJob(jconf);
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
  private RunningJob submitJob(JobConf conf) {
    conf.setJobID(getRemoteID());
    /*
     * get output configuration, compute input split
     */
    try {
      sendConf();
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    return new RunningJob(conf);
  }

  private void getUpdates() {
    while (true) {
      // poll for job status
    }
  }

  public static void main(String[] args) {
    if (args.length != 1) {
      System.out.println("USAGE: java ha.mapreduce.JobClient <conf file>");
      System.exit(0);
    }

    JobConf conf = new JobConf(args[0]);
    System.out.println("[CLIENT] Setting up new job as such:");
    System.out.println(conf);
    JobClient client = new JobClient(conf);
    try {
      client.sendConf();
    } catch (Exception e) {
      System.err.println("Could not send job conf over to master.");
      e.printStackTrace();
    }
    System.out.println("Sent job conf to master. Now listening for updates.");
    client.getUpdates();
    /*
     * while (true) { rjob.checkPeriod(); }
     */
  }

}
