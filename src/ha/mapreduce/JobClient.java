package ha.mapreduce;

public class JobClient {
  public static RunningJob running;

  private JobConf jconf;

  public JobClient() {

  }

  public JobClient(JobConf conf) {
    this.jconf = conf;

  }

  @Deprecated
  public static RunningJob runJob(JobConf job) {
    JobClient jc = new JobClient(job);
    running = jc.submitJob(job);
    return null;

  }

  public RunningJob runJob() {
    return submitJob(jconf);
  }

  private String getRemoteID() {
    return null;
  }

  private void uploadResources() {

  }

  private void tellJobTracker() {

  }

  private RunningJob submitJob(JobConf conf) {
    /**
     * å�‘JobTrackerè¯·æ±‚ä¸€ä¸ªæ–°çš„job ID æ£€æµ‹æ­¤jobçš„outputé…�ç½® è®¡ç®—æ­¤jobçš„input split
     * å°†Jobè¿�è¡Œæ‰€éœ€çš„èµ„æº�æ‹·è´�åˆ°JobTrackerçš„æ–‡ä»¶ç³»ç»Ÿä¸­çš„æ–‡ä»¶å¤¹ä¸­ï¼ŒåŒ…æ‹¬job.
     * jaræ–‡ä»¶ã€�job.xmlé…�ç½®æ–‡ä»¶ï¼Œinput splits é€šçŸ¥JobTrackeræ­¤Jobå·²ç»�å�¯ä»¥è¿�è¡Œäº†
     * æ��
     * äº¤ä»»åŠ¡å�Žï¼ŒrunJobæ¯�éš�?ä¸€ç§’é’Ÿè½®è¯¢ä¸€æ¬¡jobçš„è¿›åº¦ï¼Œå°†è¿›åº¦è¿�?å›žåˆ°å‘½ä»¤è¡
     * Œï¼Œç›´åˆ°ä»»åŠ¡è¿�è¡Œå®Œæ¯•ã€‚
     */
    conf.setJobID(getRemoteID());
    /*
     * get output configuration, compute input split
     */
    uploadResources();

    tellJobTracker();

    return new RunningJob(conf);

  }

  RunningJob submitJob(String jobFile) {
    return null;

  }

  public static void main(String[] args) {
    // TODO Auto-generated method stub
    JobConf conf = new JobConf(args[0]);
    System.out.println(conf);
    JobClient client = new JobClient(conf);
    /*
     * RunningJob rjob=client.runJob();
     * 
     * while(true){ rjob.checkPeriod(); }
     */

  }

}
