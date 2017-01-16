package join.impl;

import com.alibaba.fastjson.JSONObject;
import condition.Condition;
import join.DiskReplicateJoinTool;
import mapreduce.DiskReplicateJoinMapper;
import mapreduce.JoinConfigurationUtils;
import mapreduce.impl.JSONDiskReplicateJoinMapper;
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
