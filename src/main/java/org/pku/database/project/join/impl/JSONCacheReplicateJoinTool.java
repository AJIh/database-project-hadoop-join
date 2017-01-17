package org.pku.database.project.join.impl;

import org.pku.database.project.join.CacheReplicateJoinTool;
import org.pku.database.project.join.Dataset;
import org.pku.database.project.mapreduce.CachedReplicateJoinMapper;
import org.pku.database.project.mapreduce.impl.JSONCacheReplicateMapper;

/**
 * Created by aji on 2017/1/17.
 */
public class JSONCacheReplicateJoinTool extends CacheReplicateJoinTool {

    @Override
    public Class<? extends CachedReplicateJoinMapper> getMapperClass() {
        return JSONCacheReplicateMapper.class;
    }

    public JSONCacheReplicateJoinTool(Dataset leftOrRight) {
        super(leftOrRight);
    }
}
