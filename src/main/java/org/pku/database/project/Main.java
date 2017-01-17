package org.pku.database.project;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.pku.database.project.condition.Condition;
import org.pku.database.project.costmodel.CostModel;
import org.pku.database.project.costmodel.impl.JSONFilesCostModel;
import org.pku.database.project.join.Dataset;
import org.pku.database.project.join.impl.JSONCacheReplicateJoinTool;
import org.pku.database.project.join.impl.JSONDiskReplicateJoinTool;
import org.pku.database.project.join.impl.ReplicateJoinWithCostModelTool;
import org.pku.database.project.mapreduce.JoinConfigurationUtils;

/**
 * Created by aji on 2017/1/17.
 */
public class Main {

    private static void testFragmentReplicateJoin() throws Exception {
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
                "file:///Users/haoyoufeng/Documents/git/hadoop-join/output/replicate-join"
        };
        ToolRunner.run(conf, new JSONDiskReplicateJoinTool(), inputAndOutput);
    }

    private static void testBroadcastJoin() throws Exception {
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
                "file:///Users/haoyoufeng/Documents/git/hadoop-join/output/broadcast-join"
        };
        ToolRunner.run(conf, new JSONCacheReplicateJoinTool(Dataset.LEFT), inputAndOutput);
    }

    private static void testJoinWithCostModel() throws Exception {
        class JoinCondition implements Condition<JSONObject> {
            @Override
            public Boolean test(JSONObject a, JSONObject b) {
                return a.getInteger("age") > b.getInteger("time");
            }
        }

        Configuration conf = new Configuration();
        JoinConfigurationUtils.setJoinProductFilter(conf, JoinCondition.class);

        CostModel costModel = new JSONFilesCostModel(conf, -1L);
        ReplicateJoinWithCostModelTool tool = new ReplicateJoinWithCostModelTool(costModel);

        String[] inputAndOutput = new String[]{
                "file:///Users/haoyoufeng/Documents/git/hadoop-join/input/persons",
                "file:///Users/haoyoufeng/Documents/git/hadoop-join/input/buildings",
                "file:///Users/haoyoufeng/Documents/git/hadoop-join/output/join-with-cost-model"
        };

        ToolRunner.run(conf, new ReplicateJoinWithCostModelTool(costModel), inputAndOutput);
    }

    private static void testMultipleJoins() throws Exception {
        class JoinConditionStep1 implements Condition<JSONObject> {
            @Override
            public Boolean test(JSONObject a, JSONObject b) {
                return a.getInteger("age") > b.getInteger("time");
            }
        }

        class JoinConditionStep2 implements Condition<JSONObject> {
            @Override
            public Boolean test(JSONObject a, JSONObject b) {
                return a.getString("country").equals(b.getString("from"));
            }
        }

        Configuration conf = new Configuration();

        CostModel costModel = new JSONFilesCostModel(conf);
        ReplicateJoinWithCostModelTool tool = new ReplicateJoinWithCostModelTool(costModel);


        String[] inputAndOutputStep1 = new String[]{
                "file:///Users/haoyoufeng/Documents/git/hadoop-join/input/persons",
                "file:///Users/haoyoufeng/Documents/git/hadoop-join/input/buildings",
                "file:///Users/haoyoufeng/Documents/git/hadoop-join/output/multiple-join/step1"
        };
        JoinConfigurationUtils.setJoinProductFilter(conf, JoinConditionStep1.class);
        ToolRunner.run(conf, new ReplicateJoinWithCostModelTool(costModel), inputAndOutputStep1);

        String[] inputAndOutputStep2 = new String[]{
                "file:///Users/haoyoufeng/Documents/git/hadoop-join/output/multiple-join/step1/part-m-00000",
                "file:///Users/haoyoufeng/Documents/git/hadoop-join/input/animals",
                "file:///Users/haoyoufeng/Documents/git/hadoop-join/output/multiple-join/result"
        };
        JoinConfigurationUtils.setJoinProductFilter(conf, JoinConditionStep2.class);
        ToolRunner.run(conf, new ReplicateJoinWithCostModelTool(costModel), inputAndOutputStep2);
    }


    public static void main(String[] args) throws Exception {
        testBroadcastJoin();

        testFragmentReplicateJoin();

        testJoinWithCostModel();

        testMultipleJoins();
    }
}
