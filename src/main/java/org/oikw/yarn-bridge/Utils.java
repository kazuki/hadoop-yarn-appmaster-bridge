package org.oikw.yarn_bridge;

import java.util.ArrayList;
import java.util.Map;
import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.yarn.util.ConverterUtils;

public class Utils {
    public static void addResource(YarnConfiguration conf, Map<String, LocalResource> resources, Path path) throws IOException {
        LocalResource res = Records.newRecord(LocalResource.class);
        FileStatus stat = FileSystem.get(conf).getFileStatus(path);
        res.setResource(ConverterUtils.getYarnUrlFromPath(path));
        res.setSize(stat.getLen());
        res.setTimestamp(stat.getModificationTime());
        res.setType(LocalResourceType.FILE);
        res.setVisibility(LocalResourceVisibility.APPLICATION);
        resources.put(path.getName(), res);
    }
}
