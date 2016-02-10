package org.oikw.yarn_bridge;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

public class Client {
    public void run(String[] args) throws Exception {
        ArrayList<String> files = new ArrayList<String>();
        ArrayList<String> pass_args = new ArrayList<String>();
        String appName = "Application";
        String queueName = "default";
        int amCPU = 1;
        int amMemory = 1024;
        boolean waitFlag = true;
        for (int i = 0; i < args.length; ++i) {
            if (args[i].equals("--no-wait")) {
                waitFlag = true;
            } else if (args[i].equals("--name")) {
                appName = args[++i];
            } else if (args[i].equals("--queue")) {
                queueName = args[++i];
            } else if (args[i].equals("--cpu")) {
                amCPU = Integer.parseInt(args[++i]);
            } else if (args[i].equals("--memory")) {
                amMemory = Integer.parseInt(args[++i]);
            } else if (args[i].equals("--")) {
                for (++i; i < args.length; ++i)
                    pass_args.add(args[i]);
                break;
            } else {
                files.add(args[i]);
            }
        }

        YarnConfiguration conf = new YarnConfiguration();
        YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(conf);
        yarnClient.start();

        YarnClientApplication app = yarnClient.createApplication();
        ContainerLaunchContext amContainer =
            Records.newRecord(ContainerLaunchContext.class);

        StringBuilder sb = new StringBuilder();
        sb.append("$JAVA_HOME/bin/java -Xmx256M");
        sb.append(" org.oikw.yarn_bridge.ApplicationMaster");
        for (String path : files) {
            sb.append(" \"");
            sb.append(path);
            sb.append("\"");
        }
        if (pass_args.size() > 0) {
            sb.append("--");
            for (String arg : pass_args) {
                sb.append(" \"");
                sb.append(arg);
                sb.append("\"");
            }
        }
        sb.append(" 1>");
        sb.append(ApplicationConstants.LOG_DIR_EXPANSION_VAR);
        sb.append("/stdout");
        sb.append(" 2>");
        sb.append(ApplicationConstants.LOG_DIR_EXPANSION_VAR);
        sb.append("/stderr");
        amContainer.setCommands(Collections.singletonList(sb.toString()));

        Map<String, LocalResource> appMasterResources = new HashMap<String, LocalResource>();
        for (String path : files) {
            if (path.startsWith("@"))
                path = path.substring(1);
            Utils.addResource(conf, appMasterResources, new Path(path));
        }
        amContainer.setLocalResources(appMasterResources);

        Map<String, String> appMasterEnv = new HashMap<String, String>();
        setupAppMasterEnv(conf, appMasterEnv);
        amContainer.setEnvironment(appMasterEnv);

        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(amMemory);
        capability.setVirtualCores(amCPU);

        ApplicationSubmissionContext appContext =
            app.getApplicationSubmissionContext();
        appContext.setApplicationName(appName);
        appContext.setAMContainerSpec(amContainer);
        appContext.setResource(capability);
        appContext.setQueue(queueName);

        ApplicationId appId = appContext.getApplicationId();
        System.out.println("ApplicationID: " + appId);
        yarnClient.submitApplication(appContext);

        if (waitFlag) {
            ApplicationReport appReport = yarnClient.getApplicationReport(appId);
            YarnApplicationState appState = appReport.getYarnApplicationState();
            while (appState != YarnApplicationState.FINISHED &&
                   appState != YarnApplicationState.KILLED &&
                   appState != YarnApplicationState.FAILED) {
                Thread.sleep(100);
                appReport = yarnClient.getApplicationReport(appId);
                appState = appReport.getYarnApplicationState();
            }
            System.out.println
                ("Application " + appId + " finished with" +
                 " state " + appState +
                 " at " + appReport.getFinishTime());
        }
    }

    private void setupAppMasterEnv(YarnConfiguration conf, Map<String, String> appMasterEnv) {
        String[] defaultPaths = conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                                                YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH);
        for (String c : defaultPaths) {
            Apps.addToEnvironment(appMasterEnv, Environment.CLASSPATH.name(),
                                  c.trim(), File.pathSeparator);
        }
        Apps.addToEnvironment(appMasterEnv,
                              Environment.CLASSPATH.name(),
                              Environment.PWD.$() + File.separator + "*",
                              File.pathSeparator);
    }

    public static void main(String[] args) throws Exception {
        Client c = new Client();
        c.run(args);
    }
}
