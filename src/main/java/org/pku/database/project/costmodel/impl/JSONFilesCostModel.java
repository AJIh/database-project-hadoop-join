package org.pku.database.project.costmodel.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.pku.database.project.costmodel.CostModel;
import org.pku.database.project.costmodel.FileCostModel;
import org.pku.database.project.join.Dataset;
import org.pku.database.project.join.impl.JSONCacheReplicateJoinTool;
import org.pku.database.project.join.impl.JSONDiskReplicateJoinTool;

import java.io.IOException;

/**
 * Created by aji on 2017/1/17.
 */
public class JSONFilesCostModel extends FileCostModel {

    public JSONFilesCostModel(Configuration conf) throws IOException {
        super(conf);
    }

    public JSONFilesCostModel(Configuration conf, Long maxCacheLen) throws IOException {
        super(conf, maxCacheLen);
    }

    @Override
    public Tool selectTool(Path datasetA, Path datasetB) throws IOException {
        Long leftLen = fs.getFileStatus(datasetA).getLen();
        Long rightLen = fs.getFileStatus(datasetB).getLen();

        if (leftLen < maxCacheLength) {
            return new JSONCacheReplicateJoinTool(Dataset.LEFT);
        } else if (rightLen < maxCacheLength) {
            return new JSONCacheReplicateJoinTool(Dataset.RIGHT);
        } else {
            return new JSONDiskReplicateJoinTool();
        }
    }

}
