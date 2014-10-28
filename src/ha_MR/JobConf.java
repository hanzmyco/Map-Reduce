package ha_MR;


public class JobConf extends Job{
  private String JobXml;
  private String JobJar;
  private String JobSplit;
  
  public String getJobXml() {
    return JobXml;
  }

  public void setJobXml(String jobXml) {
    JobXml = jobXml;
  }

  public String getJobJar() {
    return JobJar;
  }

  public void setJobJar(String jobJar) {
    JobJar = jobJar;
  }

  public String getJobSplit() {
    return JobSplit;
  }

  public void setJobSplit(String jobSplit) {
    JobSplit = jobSplit;
  }

  /*
   * conf is the configure file, locally
   */
  public JobConf(String conf){  
    parseConf(conf);
    
    
  }
  
  /*
   * get all the information of Conf
   */
  public void parseConf(String conf){
    setJobJar(null);
    setJobXml(null);
    setJobSplit(null);
    generateFile();
    /*
     * parse the conf, get all the information generate the file
     */
    
  }
  /*
   * generate three conf_file job.xml, job.jar, job.split
   */
  public void generateFile(){
    
  }

}
