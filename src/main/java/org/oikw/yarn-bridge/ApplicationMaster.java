package org.oikw.yarn_bridge;

import java.io.File;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.net.Socket;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Records;

import org.oikw.json.JSONArray;
import org.oikw.json.JSONObject;
import org.oikw.json.Parser;

public class ApplicationMaster implements AMRMClientAsync.CallbackHandler {
    YarnConfiguration configuration;
    NMClient nmClient;
    AMRMClientAsync<ContainerRequest> rmClient;
    HashMap<String, LocalResource> defaultLocalRes;

    HashMap<String, Container> allocatedContainerMap;

    BlockingQueue<JSONObject> sendQueue;
    Process bridgeProc = null;
    Thread[] bridgeProcStrmRelay = null;

    public ApplicationMaster() {
        this.allocatedContainerMap = new HashMap<String, Container>();
        this.sendQueue = new ArrayBlockingQueue<JSONObject>(512);
        this.configuration = new YarnConfiguration();
        this.defaultLocalRes = new HashMap<String, LocalResource>();
        this.nmClient = NMClient.createNMClient();
        this.nmClient.init(this.configuration);
        this.nmClient.start();
    }

    public void onContainersAllocated(List<Container> containers) {
        JSONArray ary = new JSONArray();
        JSONObject msg = new JSONObject()
            .put("method", "onContainersAllocated")
            .put("params", new JSONObject().put("containers", ary));
        for (Container container : containers) {
            synchronized (this.allocatedContainerMap) {
                this.allocatedContainerMap.put(container.getId().toString(),
                                               container);
            }
            JSONObject c = new JSONObject()
                .put("container_id", container.getId().toString())
                .put("cpu", container.getResource().getVirtualCores())
                .put("memory", container.getResource().getMemory())
                .put("priority", container.getPriority().getPriority());
            ary.add(c);
            synchronized (System.out) {
                System.out.println("[AM-Bridge] Allocated container " + container.getId());
            }
        }
        try {
            this.sendQueue.put(msg);
        } catch (Exception e) {}
    }

    public void onContainersCompleted(List<ContainerStatus> statuses) {
        JSONArray ary = new JSONArray();
        JSONObject msg = new JSONObject()
            .put("method", "onContainersCompleted")
            .put("params", new JSONObject().put("statuses", ary));
        for (ContainerStatus status : statuses) {
            JSONObject s = new JSONObject()
                .put("container_id", status.getContainerId().toString())
                .put("diagnostic", status.getDiagnostics())
                .put("exit_status", status.getExitStatus());
            ary.add(s);
            synchronized (this.allocatedContainerMap) {
                this.allocatedContainerMap.remove(status.getContainerId().toString());
            }
            synchronized (System.out) {
                System.out.println("[AM-Bridge] Completed container " + status.getContainerId());
            }
        }
        try {
            this.sendQueue.put(msg);
        } catch (Exception e) {}
    }

    public void onNodesUpdated(List<NodeReport> updated) {
    }

    public void onShutdownRequest() {
        try {
            this.sendQueue.put(new JSONObject().put("method", "onShutdownRequest"));
        } catch (Exception e) {}
    }

    public void onError(Throwable t) {
        try {
            this.sendQueue.put(new JSONObject().put("method", "onError"));
        } catch (Exception e) {}
    }

    public float getProgress() {
        return 0;
    }

    public static void main(String[] args) throws Exception {
        ApplicationMaster master = new ApplicationMaster();
        master.runMainLoop(args);
    }

    public void runMainLoop(String[] args) throws Exception {
        // RPC用のTCP接続を確立する(Loopback+エフェメラルポートを利用)
        ServerSocket listenSock = new ServerSocket(0, 1, null);
        execBridgeEnv(args, listenSock.getLocalPort());
        Socket sock = listenSock.accept();
        listenSock.close();

        this.rmClient = AMRMClientAsync.createAMRMClientAsync(50, this);
        this.rmClient.init(this.configuration);
        this.rmClient.start();

        synchronized (System.out) {
            System.out.println("[AM-Bridge] begin:registerApplicationMaster");
        }
        this.rmClient.registerApplicationMaster("", 0, "");
        synchronized (System.out) {
            System.out.println("[AM-Bridge] end:registerApplicationMaster");
        }

        for (String path : args) {
            if (path.equals("--"))
                break;
            if (path.startsWith("--"))
                continue;
            if (path.startsWith("@"))
                path = path.substring(1);
            Utils.addResource(this.configuration,
                              this.defaultLocalRes, new Path(path));
        }

        Thread sendThread = new JsonSendThread(sock.getOutputStream(),
                                               this.sendQueue);
        sendThread.start();
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(sock.getInputStream(), "utf-8"));
            while (true) {
                JSONObject msg = (JSONObject)Parser.parse(reader);
                JSONObject res = null;
                synchronized (System.out) {
                    System.out.printf("[AM-Bridge] %s\n", msg);
                }
                if (msg.containsKey("id")) {
                    long req_id = (long)msg.getNumber("id");
                    res = new JSONObject().put("id", req_id);
                    try {
                        res.put("result", this.processRequest(msg));
                    } catch (Exception e) {
                        res.put("error", new JSONObject()
                                .put("code", -32000)
                                .put("message", e.toString()));
                    }
                }
                if (res != null)
                    this.sendQueue.put(res);
            }
        } catch (Exception e) {}

        this.sendQueue.clear();
        this.sendQueue.put(new JSONObject());
        sendThread.join();

        this.bridgeProc.waitFor();
        for (Thread t : this.bridgeProcStrmRelay)
            t.join();

        synchronized (System.out) {
            System.out.println("[AM-Bridge] begin:unregisterApplicationMaster");
        }
        this.rmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "", "");
        synchronized (System.out) {
            System.out.println("[AM-Bridge] end:unregisterApplicationMaster");
        }
    }

    private Object processRequest(JSONObject req) throws Exception
    {
        String method = req.getString("method");
        if (method.equals("ping"))
            return null;
        JSONObject params = null;
        if (req.containsKey("params"))
            params = req.getObject("params");
        if (method.equals("addContainerRequest")) {
            Resource capability = Records.newRecord(Resource.class);
            Priority priority = Records.newRecord(Priority.class);
            capability.setMemory((int)params.getNumber("memory"));
            capability.setVirtualCores((int)params.getNumber("cpu"));
            priority.setPriority((int)params.getNumber("priority"));
            this.rmClient.addContainerRequest(new ContainerRequest(capability, null, null, priority));
            return null;
        } else if (method.equals("startContainer")) {
            Container container;
            synchronized (this.allocatedContainerMap) {
                container = this.allocatedContainerMap.get(params.getString("container_id"));
            }
            ContainerLaunchContext ctx =
                Records.newRecord(ContainerLaunchContext.class);
            if (this.defaultLocalRes.size() > 0)
                ctx.setLocalResources(this.defaultLocalRes);
            ctx.setCommands(Collections.singletonList(params.getString("command") +
                " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
                " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"));
            this.nmClient.startContainer(container, ctx);
            return null;
        } else if (method.equals("getAvailableResources")) {
            Resource res = this.rmClient.getAvailableResources();
            if (res == null)
                return null;
            return new JSONObject()
                .put("memory", res.getMemory())
                .put("cpu", res.getVirtualCores());
        } else if (method.equals("getClusterNodeCount")) {
            return this.rmClient.getClusterNodeCount();
        } else if (method.equals("releaseAssignedContainer")) {
            String container_id = params.getString("container_id");
            this.rmClient.releaseAssignedContainer(ContainerId.fromString(container_id));
            synchronized (this.allocatedContainerMap) {
                this.allocatedContainerMap.remove(container_id);
            }
            return null;
        } else {
            throw new Exception("Unknown Method");
        }
    }

    private void execBridgeEnv(String[] args, int port) throws Exception {
        Path execPath = new Path(args[0]);
        File execFile = new File(execPath.getName());
        ArrayList<String> cmd = new ArrayList<String>();
        cmd.add("/bin/bash");
        cmd.add(execPath.getName());
        cmd.add(String.valueOf(port));
        for (int i = 1; i < args.length; ++i)
            cmd.add(args[i]);
        ProcessBuilder pb = new ProcessBuilder(cmd);
        this.bridgeProc = pb.start();
        this.bridgeProcStrmRelay = new Thread[] {
            new StreamRelayThread(this.bridgeProc.getInputStream(),
                                  System.out),
            new StreamRelayThread(this.bridgeProc.getErrorStream(),
                                  System.err)
        };
        for (Thread t : this.bridgeProcStrmRelay)
            t.start();
    }

    private static class JsonSendThread extends Thread {
        BufferedWriter writer;
        BlockingQueue<JSONObject> queue;

        public JsonSendThread(OutputStream out, BlockingQueue<JSONObject> queue) throws Exception {
            this.writer = new BufferedWriter(new OutputStreamWriter(out, "utf-8"));
            this.queue = queue;
        }

        @Override
        public void run() {
            while (true) {
                try {
                    JSONObject obj = this.queue.take();
                    if (obj.size() == 0)
                        return;
                    obj.put("jsonrpc", "2.0");
                    this.writer.write(obj.toString());
                    this.writer.flush();
                    System.out.printf("[AM-Bridge] send: %s\n", obj);
                } catch (Exception e) {}
            }
        }
    }

    private static class StreamRelayThread extends Thread {
        BufferedReader reader;
        PrintStream writer;

        public StreamRelayThread(InputStream in, PrintStream out) {
            this.reader = new BufferedReader(new InputStreamReader(in));
            this.writer = out;
        }

        @Override
        public void run() {
            try {
                String line;
                while ((line = this.reader.readLine()) != null) {
                    synchronized (this.writer) {
                        this.writer.println(line);
                    }
                }
            } catch (Exception e) {
            } finally {
                try {
                    this.reader.close();
                } catch (Exception e) {}
            }
        }
    }
}
