package org.pku.database.project.join;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.pku.database.project.mapreduce.CachedReplicateJoinMapper;
import org.pku.database.project.mapreduce.JoinConfigurationUtils;

/**
 * Created by aji on 2017/1/16.
 */
abstract public class CacheReplicateJoinTool extends ReplicateJoinTool {

    private static final Log LOG = LogFactory.getLog(CacheReplicateJoinTool.class);

    public abstract Class<? extends CachedReplicateJoinMapper> getMapperClass();

    private String leftOrRight = null;

    public CacheReplicateJoinTool(String leftOrRight) {
        this.leftOrRight = leftOrRight;
    }

    /**
     *
     * @param args dataAPath dataBPath outputPath left/right
     * @return error de
     * @throws Exception
     */
    @Override
    public int run(String[] args) throws Exception {

        if (args.length != 3) {
            LOG.error("3 args required: dataAPath dataBPath outputPath");
            return 1;
        }

        Configuration conf = getConf();
        // 必须在这里设置好 conf，因为 job 会在创建时 clone 一次

        Job job = Job.getInstance(conf, "CacheReplicateJoinTool");
        job.setJarByClass(CacheReplicateJoinTool.class);
        job.setMapperClass(getMapperClass());
        job.setNumReduceTasks(0);

        String leftPath = args[0];
        String rightPath = args[1];
        String outputPath = args[2];


        if (leftOrRight.equalsIgnoreCase("left")) {
            JoinConfigurationUtils.cacheLeft(job, new Path(leftPath).toUri());
            TextInputFormat.setInputPaths(job, new Path(rightPath));
        } else if (leftOrRight.equalsIgnoreCase("right")) {
            JoinConfigurationUtils.cacheRight(job, new Path(rightPath).toUri());
            TextInputFormat.setInputPaths(job, new Path(leftPath));
        } else {
            LOG.error("3 args required: dataAPath dataBPath outputPath");
            return 2;
        }


        TextOutputFormat.setOutputPath(job, new Path(outputPath));


        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        if (!job.waitForCompletion(true)) {
            return 3;
        }

        return 0;
    }
}
