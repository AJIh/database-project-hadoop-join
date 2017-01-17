package org.pku.database.project.join.impl;

import com.alibaba.fastjson.JSONObject;
import org.pku.database.project.condition.Condition;
import org.pku.database.project.join.DiskReplicateJoinTool;
import org.pku.database.project.mapreduce.DiskReplicateJoinMapper;
import org.pku.database.project.mapreduce.JoinConfigurationUtils;
import org.pku.database.project.mapreduce.impl.JSONDiskReplicateJoinMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by aji on 2017/1/16.
 */
public class JSONDiskReplicateJoinTool extends DiskReplicateJoinTool {

    @Override
    public Class<? extends DiskReplicateJoinMapper> getMapperClass() {
        return JSONDiskReplicateJoinMapper.class;
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
                "./input/persons", "./input/buildings", "./output/personAndBuildings"
        };
        ToolRunner.run(conf, new JSONDiskReplicateJoinTool(), inputAndOutput);
    }
}
