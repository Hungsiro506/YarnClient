#!/bin/bash

rm1=$(/opt/hadoop/bin/yarn rmadmin -getServiceState rm1)
rm2=$(/opt/hadoop/bin/yarn rmadmin -getServiceState rm2)

if [ "$rm1" == "active" ]
then
    echo "rm1 active"
    /opt/jdk/bin/java -cp /home/hungvd8/yarn-client-jar-with-dependencies.jar main.com.ftel.vn.jobs_monitor.MonitorStreamingJobs 172.27.11.61 8088 /home/hungvd8/job_names.txt
fi

if [ "$rm2" == "active" ]
then
    echo "rm2 active"
    /opt/jdk/bin/java -cp home/hungvd8/yarn-client-jar-with-dependencies.jar main.com.ftel.vn.jobs_monitor.MonitorStreamingJobs 172.27.11.62 8088 /home/hungvd8/job_names.txt
fi
