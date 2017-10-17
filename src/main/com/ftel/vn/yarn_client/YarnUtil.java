package main.com.ftel.vn.yarn_client;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.util.*;
import java.util.Map;


/**
 * Created by hungdv on 13/10/2017.
 */

/**
 * This is a helper class to interact with yarn. Some of the functionalities it provides are killing an application,
 * getting the state of an application, getting an application id given the job name and job id.
 */
public class YarnUtil {
    private static final Logger log = LoggerFactory.getLogger(YarnUtil.class);
    private CloseableHttpClient httpclient;
    private HttpHost rmServer;
    private YarnClient yarnClient;

    public YarnUtil(String rmAddress, int rmPort) {
        this.httpclient = HttpClientBuilder.create().build();
        this.rmServer = new HttpHost(rmAddress, rmPort, "http");
        log.info("setting rm server to : " + rmServer);
        System.out.println("setting rm server to : " + rmServer + " port " + rmPort);
        YarnConfiguration hConfig = new YarnConfiguration();
        hConfig.set(YarnConfiguration.RM_ADDRESS, rmAddress + ":" + YarnConfiguration.DEFAULT_RM_PORT);
        yarnClient = YarnClient.createYarnClient();
        yarnClient.init(hConfig);
        yarnClient.start();
    }

    /**
     * Queries rm for all the applications currently running and finds the application with the matching job name and id
     *
     * @param jobName the name of the job
     * @param jobID   the job id
     * @return the application id of the job running in yarn. If application id is not found, it will return null.
     */
    public String getRunningAppId(String jobName, int jobID) {

        try {
            HttpGet getRequest = new HttpGet("/ws/v1/cluster/apps");
            HttpResponse httpResponse = httpclient.execute(rmServer, getRequest);
            String applications = EntityUtils.toString(httpResponse.getEntity());
            log.debug("applications: " + applications);

            List<Map<String, String>> applicationList = parseYarnApplications(applications);
            String name = jobName + "_" + jobID;
            for (Map<String, String> application : applicationList) {
                if (application.containsKey("state") && application.containsKey("name") && application.containsKey("id")) {
                    if (application.get("state").toString().equals("RUNNING") && application.get("name").toString().equals(name)) {
                        return application.get("id").toString();
                    }
                }
            }
        } catch (NullPointerException e) {
            e.printStackTrace();
            throw new IllegalStateException("there is no valid application id for the given job name and job id. job name: " + jobName + " job id: " + jobID);
        } catch (  IOException e){
            e.printStackTrace();
            throw new IllegalStateException("there is no valid application id for the given job name and job id. job name: " + jobName + " job id: " + jobID);
        }

        return null;
    }
    public void printlnOutAllJobsInfo() {
        try{
            List<Map<String,Object>> apps = getAllAppsByState("RUNNING");
            for(Map<String,Object> app : apps){
                System.out.println(app);
            }

        }catch (IOException e){
            e.printStackTrace();
        }catch(YarnException e){
            e.printStackTrace();
        }
    }

    /**
     * Get all apps by state
     * A state can be :
     * @param state
     * @return
     * @throws YarnException
     * @throws IOException
     */
    public List<Map<String,Object>> getAllAppsByState(String state) throws YarnException,IOException{
        List<Map<String,Object>> apps = new ArrayList<Map<String,Object>>();
        List<ApplicationReport> appsReport = this.yarnClient.getApplications();
        for(ApplicationReport report: appsReport){
            if(report.getYarnApplicationState().toString().equals(state)){
                Map<String,Object> map = this.convertApplicationReport(report);
                apps.add(map);
            }

        }
        return apps;
    }

    /**
     * Get all running jobs name.
     * @return
     * @throws YarnException
     * @throws IOException
     */
    public Set<String> getAllRunningAppNames() throws YarnException,IOException{
        Set<String> appNames = new HashSet<>();
        List<ApplicationReport> appsReport = this.yarnClient.getApplications();
        for(ApplicationReport report: appsReport){
            if(report.getYarnApplicationState().toString().equals("RUNNING")){
                appNames.add(report.getName());
            }
        }
        return appNames;
    }

    /**
     * Return all apps infor.
     * Contained in a List.
     * @return
     * @throws IOException
     * @throws YarnException
     */
    public List<Map<String,Object>> getAllApps() throws  IOException,YarnException{
        List<Map<String,Object>> apps = new ArrayList<Map<String,Object>>();
        List<ApplicationReport> appsReport = this.yarnClient.getApplications();
        for(ApplicationReport report: appsReport){
            Map<String,Object> map = this.convertApplicationReport(report);
            apps.add(map);
        }
        return apps;
    }

    /**
     * What a stupid function. !!!
     * Go fuck your self.
     * Bitch.
     * @param appName
     * @return
     */
    public String getRuningAppName(String appName){

        try {
            HttpGet getRequest = new HttpGet("/ws/v1/cluster/apps");
            HttpResponse httpResponse = httpclient.execute(rmServer, getRequest);
            String applications = EntityUtils.toString(httpResponse.getEntity());
            log.debug("applications: " + applications);

            List<Map<String, String>> applicationList = parseYarnApplications(applications);
            String name = appName;
            for (Map<String, String> application : applicationList) {
                if (application.containsKey("state") && application.containsKey("name")) {
                    if (application.get("state").toString().equals("RUNNING") && application.get("name").toString().equals(name)) {
                        return application.get("name").toString();
                    }
                }
            }
        } catch (NullPointerException e) {
            e.printStackTrace();
            throw new IllegalStateException("there is no valid application id for the given job name and job id. job name: " + appName);
        } catch (  IOException e){
            e.printStackTrace();
            throw new IllegalStateException("there is no valid application id for the given job name and job id. job name: " + appName);
        }

        return null;
    }
    private  List<Map<String, String>> parseYarnApplications(String applications) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Map<String, List<Map<String, String>>>> yarnApplications = mapper.readValue(applications, new TypeReference<Map<String, Map<String, List<Map<String, Object>>>>>() {
        });
        return yarnApplications.get("apps").get("app");
    }
    /**
     * This function returns the state of a given application. This state can be on of the
     * {"NEW", "NEW_SAVING", "SUBMITTED", "ACCEPTED", "RUNNING", "FINISHED", "FAILED", "KILLED"}
     *
     * @param applicationId the application id of the application the state is being queried
     * @return the state of the application which is one of the following values: {"NEW", "NEW_SAVING", "SUBMITTED", "ACCEPTED", "RUNNING", "FINISHED", "FAILED", "KILLED"}
     * @throws IOException   Throws IO exception
     * @throws YarnException in case of errors or if YARN rejects the request due to
     *                       access-control restrictions.
     */
    public String getApplicationState(String applicationId) throws IOException, YarnException {

        return yarnClient.getApplicationReport(getApplicationIDFromString(applicationId)).getYarnApplicationState().toString();

    }
    /**
     * This function kills an application given the applicationId
     *
     * @param applicationId the application Id of the job to be killed
     * @throws IOException   Throws IO exception
     * @throws YarnException in case of errors or if YARN rejects the request due to
     *                       access-control restrictions.
     */
    public void killApplication(String applicationId) throws IOException, YarnException {

        log.info("killing job with application id: " + applicationId);

        yarnClient.killApplication(getApplicationIDFromString(applicationId));
    }
    /**
     * This function converts an application in form of a String into a {@link ApplicationId}
     *
     * @param appIDStr The application id in form of a string
     * @return the application id as an instance of ApplicationId class.
     */
    private ApplicationId getApplicationIDFromString(String appIDStr) {
        String[] parts = appIDStr.split("_");
        if (parts.length < 3) {
            throw new IllegalStateException("the application id found is not valid. application id: " + appIDStr);
        }
        long timestamp = Long.valueOf(parts[1]);
        int id = Integer.valueOf(parts[2]);
        return ApplicationId.newInstance(timestamp, id);
    }
    private Map<String,Object> convertApplicationReport(ApplicationReport report){
        Map<String,Object> map          = new HashMap<String,Object>();
        String applicationID            = report.getApplicationId().toString();
        String applicationUser          = report.getUser();
        String applicationName          = report.getName();
        String applicationMaster        = report.getHost();
        int    applicationMasterPort    = report.getRpcPort();
        String applicationState         = report.getYarnApplicationState().toString();
        String applicationStartTime     = new Date(report.getStartTime()).toString();

        map.put("AppId",applicationID);
        map.put("UserName",applicationUser);
        map.put("AppName",applicationName);
        map.put("AppMaster",applicationMaster);
        map.put("AppMasterPort",applicationMasterPort);
        map.put("AppState",applicationState);
        map.put("AppStartTime",applicationStartTime);
        return map;
    }
    /**
     * This function stops the YarnUtil by stopping the yarn client and http client.
     */
    public void stop() {
        try {
            httpclient.close();
        } catch (IOException e) {
            log.error("HTTP Client failed to close.", e);
        }
        yarnClient.stop();
    }


}
