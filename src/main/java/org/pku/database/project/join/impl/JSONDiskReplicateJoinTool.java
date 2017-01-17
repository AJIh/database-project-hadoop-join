package org.pku.database.project.join.impl;

import org.pku.database.project.join.DiskReplicateJoinTool;
import org.pku.database.project.mapreduce.DiskReplicateJoinMapper;
import org.pku.database.project.mapreduce.impl.JSONDiskReplicateJoinMapper;

/**
 * Created by aji on 2017/1/16.
 */
public class JSONDiskReplicateJoinTool extends DiskReplicateJoinTool {

    @Override
    public Class<? extends DiskReplicateJoinMapper> getMapperClass() {
        return JSONDiskReplicateJoinMapper.class;
    }
}
