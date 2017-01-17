package org.pku.database.project.join.impl;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.pku.database.project.condition.Condition;
import org.pku.database.project.costmodel.CostModel;
import org.pku.database.project.costmodel.impl.JSONFilesCostModel;
import org.pku.database.project.join.ReplicateJoinTool;
import org.pku.database.project.mapreduce.JoinConfigurationUtils;

/**
 * Created by aji on 2017/1/17.
 */
public class ReplicateJoinWithCostModelTool extends ReplicateJoinTool {
    private CostModel costModel;

    public ReplicateJoinWithCostModelTool(CostModel costModel) {
        this.costModel = costModel;
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            throw new RuntimeException("Usage: aPath bPath outputPath");
        }

        Tool tool = costModel.selectTool(new Path(args[0]), new Path(args[1]));
        return ToolRunner.run(getConf(), tool, args);
    }
}
