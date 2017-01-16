package join;

import cartesian.CartesianConfUtils;
import cartesian.CartesianInputFormat;
import mapreduce.DiskReplicateJoinMapper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Created by aji on 2017/1/16.
 */
abstract public class DiskReplicateJoinTool extends ReplicateJoinTool {

    private static final Log LOG = LogFactory.getLog(DiskReplicateJoinTool.class);

    public abstract Class<? extends DiskReplicateJoinMapper> getMapperClass();

    /**
     *
     * @param args dataAPath dataBPath outputPath
     * @return error de
     * @throws Exception
     */
    @Override
    public int run(String[] args) throws Exception {

        if (args.length != 3) {
            LOG.error("3 args required: dataAPath dataBPath outputPath");
            return 1;
        }

        Job job = Job.getInstance(getConf(), "DiskReplicateJoinTool");
        Configuration conf = job.getConfiguration(); // 必须在这里保存 conf 的拷贝，因为 job 会在创建时 clone 一次


        job.setJarByClass(DiskReplicateJoinTool.class);
        job.setMapperClass(getMapperClass());
        job.setNumReduceTasks(0);

        job.setInputFormatClass(CartesianInputFormat.class);
        CartesianConfUtils.setLeftInputInfo(conf, TextInputFormat.class, args[0]);
        CartesianConfUtils.setRightInputInfo(conf, TextInputFormat.class, args[1]);

        TextOutputFormat.setOutputPath(job, new Path(args[2]));
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        if (!job.waitForCompletion(true)) {
            return 2;
        }

        return 0;
    }
}
