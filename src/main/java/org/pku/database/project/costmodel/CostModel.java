package org.pku.database.project.costmodel;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

/**
 * Created by aji on 2017/1/17.
 */
public interface CostModel {
    Tool selectTool(Path datasetA, Path datasetB) throws IOException, InterruptedException;
}
