package main.com.ftel.vn.jobs_monitor;


import org.apache.hadoop.yarn.exceptions.YarnException;
import org.omg.SendingContext.RunTime;
import main.com.ftel.vn.yarn_client.YarnUtil;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.TimeUnit;


/**
 * Created by hungdv on 14/10/2017.
 */
public class MonitorStreamingJobs
{
    private Map<String,String> jobNames;
    private YarnUtil client;

    public MonitorStreamingJobs(String path, String rmIP, int rmPort) throws  IOException,YarnException{
        loadJobNames(path);
        this.client =  new YarnUtil(rmIP,rmPort);
    }

    private Map<String,String> getJobsName(){
        return this.jobNames;
    }
    private void setJobNames(Map<String,String> jobNames){
        this.jobNames = jobNames;
    }

    private void loadJobNames(String path)throws IOException{
        try {
            Map<String,String> jobNamesWithExe = new HashMap<>();

            File f = new File(path);
            BufferedReader b = new BufferedReader(new FileReader(f));
            String readLine = "";
            System.out.println("Reading file in using Buffered Reader");
            while ((readLine = b.readLine()) != null) {
                System.out.println("Add job name : " + readLine);
                String[] kv = readLine.split(",");
                if(kv.length == 2){
                    jobNamesWithExe.put(kv[0],kv[1]);
                }
            }
            this.setJobNames(jobNamesWithExe);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    private void stopClient(){
        this.client.stop();
    }

    private Set<String> getMissingJobs(){
        Set<String> tmp = getJobsName().keySet();
        try{
            Set<String> rungningApp = this.client.getAllRunningAppNames();
            tmp.removeAll(rungningApp);
        }catch(IOException e){
            e.printStackTrace();
        }catch (YarnException e){
            e.printStackTrace();
        }
        return tmp;
    }

    public void run(){
        try {
            this.client.printlnOutAllJobsInfo();
            Set<String> missing = this.getMissingJobs();
            if(missing.size() > 0){
                System.out.println("Missing jobs list : " + missing);

                for(String jobName : missing){
                    runBashFile(this.jobNames.get(jobName));
                }
            }else{
                System.out.println("There is not missing job");
            }
        }
        catch (Exception e){
            e.printStackTrace();
            throw  e;
        }finally {
            this.stopClient();
        }
    }
    private void runBashFile(String shFile){
        new Thread(){
            public void run(){
                try{
                    System.out.println("Start new thread to run file " + shFile);
                    String command = "sh " + shFile +" &";
                    Process p = Runtime.getRuntime().exec(command);
                    try {
                        p.waitFor(1, TimeUnit.MINUTES);
                        System.out.println("Restart "+  shFile + "  -  " );
                    }
                    catch(InterruptedException e){
                        e.printStackTrace();
                        System.out.println(" Process has interrupted " );
                    }finally {
                        p.destroy();
                    }
                }catch (IOException e){
                    e.printStackTrace();
                }
            }
        }.start();
    }

    public static void main(String[] args) {
        int rmPort = Integer.parseInt(args[1]);
        String rmIP = args[0];
        String path = args[2];
        //YarnUtil client = new YarnUtil("172.27.11.61",8088);
        try{
            MonitorStreamingJobs monitor = new MonitorStreamingJobs(path,rmIP,rmPort);
            monitor.run();
        }catch(IOException e){
            e.printStackTrace();
        }catch (YarnException e){
            e.printStackTrace();
        }
    }
}
