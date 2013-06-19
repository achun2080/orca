STEPS TO RUN:

1) Clone and Build
 
    > git clone https://github.com/xslogic/orca.git
    > cd orca
    > mvn clean compile install package

2) Create a directory say "orca_install"

    > mkdir ~/orca_install

3) copy the orca jar to the install directory :

    > cp target/orca-1.0.0.jar ~/orca_install/

4) copy the application war file to deploy into Orca PaaS

    > cp test_app.war ~/orca_install/

5) Go to hadoop home directory :

    > cd ~/hadoop_home

6) Ensure that jetty-util-6.1.26.jar, jetty-6.1.26.jar and jcommander-1.25.jar are in the yarn CLASSPATH (This is a temporary hack.. TODO: fix this)

7) Ensure dfs and yarn is started

    > ./sbin/start-dfs.sh && ./sbin/start-yarn.sh

8) Deploy the Orca application and 

    > ./bin/yarn jar ~/orca_install/orca-1.0.0.jar org.kuttz.orca.OrcaClient -num_containers 3 -app_name test_app -war_location test_app.war -orca_dir <FULL_WORK_DIR>/orca_install/

9) If Orca has started successfully, the Orca Client will end with log messages like so :
   ...
   ...
   13/06/19 14:11:09 INFO orca.OrcaClient: Got application report from ASM for, appId=8, clientToken=null, appDiagnostics=, appMasterHost=, appQueue=default, appMasterRpcPort=0, appStartTime=1371676219295, yarnAppState=RUNNING, distributedFinalState=UNDEFINED, appTrackingUrl=x.x.x.x:8088//y.y.y.y:63059/orca_app/status, appUser=suresa1
   13/06/19 14:11:09 INFO orca.OrcaClient: Looks like Application is Running fine.. Client signing off !!
   ...

10) Extract the appTrackingUrl from the above log message (x.x.x.x:8088//y.y.y.y:63059/orca_app/status)
    For some reason, yarn mangles this url.. TODO : figure out why this is happeneing
    The interesting bit would be : y.y.y.y:63059/orca_app/status

11) Curl the status url :

    > curl "http://y.y.y.y:63059/orca_app/status"

    This will return something like :
    
    {"num_containers_running":3,"num_containers_died":0,"proxy_host":"a.b.c.d","proxy_port":8103}

12) Your application has been successfully deployed on the Orca PaaS framework. You can access your app via the proxy:

    http:/a.b.c.d:8103/test_app

