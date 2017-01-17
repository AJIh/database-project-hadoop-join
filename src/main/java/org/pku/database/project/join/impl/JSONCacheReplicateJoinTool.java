package org.pku.database.project.join.impl;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.pku.database.project.condition.Condition;
import org.pku.database.project.join.CacheReplicateJoinTool;
import org.pku.database.project.mapreduce.CachedReplicateJoinMapper;
import org.pku.database.project.mapreduce.JoinConfigurationUtils;
import org.pku.database.project.mapreduce.impl.JSONCacheReplicateMapper;

/**
 * Created by aji on 2017/1/17.
 */
public class JSONCacheReplicateJoinTool extends CacheReplicateJoinTool {

    @Override
    public Class<? extends CachedReplicateJoinMapper> getMapperClass() {
        return JSONCacheReplicateMapper.class;
    }

    public static void main(String[] args) throws Exception {
        class JoinCondition implements Condition<JSONObject> {
            @Override
            public Boolean test(JSONObject a, JSONObject b) {
                return a.getInteger("age") > b.getInteger("time");
            }
        }

        Configuration conf = new Configuration();
        JoinConfigurationUtils.setJoinProductFilter(conf, JoinCondition.class);
        String[] inputAndOutput = new String[]{
                "file:///Users/haoyoufeng/Documents/git/hadoop-join/input/persons",
                "file:///Users/haoyoufeng/Documents/git/hadoop-join/input/buildings",
                "./output/personsAndBuildings",
                "left"
        };
        ToolRunner.run(conf, new JSONCacheReplicateJoinTool(), inputAndOutput);
    }
}
