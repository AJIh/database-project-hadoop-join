package cartesian;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.join.CompositeInputSplit;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by aji on 2017/1/16.
 */
@SuppressWarnings({ "static-access", "rawtypes", "unchecked" })
public class CartesianInputFormat extends FileInputFormat {
    private static final Log LOG = LogFactory.getLog(CartesianInputFormat.class);

    private List<InputSplit> getInputSplits(JobContext job, Class<? extends FileInputFormat> inputFormatClass,
                                            String inputPath) throws IOException {
        FileInputFormat inputFormat = ReflectionUtils.newInstance(inputFormatClass, job.getConfiguration());
        inputFormat.setInputPaths((Job) job, inputPath); // TODO 必须强制转换？
        return inputFormat.getSplits(job);
    }

    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {
        try {
            Configuration conf = job.getConfiguration();
            List<InputSplit> leftSplits = getInputSplits(job,
                    CartesianConfUtils.getLeftInputFormat(conf),
                    CartesianConfUtils.getLeftInputPath(conf));
            List<InputSplit> rightSplits = getInputSplits(job,
                    CartesianConfUtils.getRightInputFormat(conf),
                    CartesianConfUtils.getRightInputPath(conf));
            List<InputSplit> cartesianResultSplits = new ArrayList(leftSplits.size() * rightSplits.size());
            int i = 0;
            for (InputSplit left: leftSplits) {
                for (InputSplit right: rightSplits) {
                    CompositeInputSplit cartesianSplit = new CompositeInputSplit(2);
                    cartesianSplit.add(left);
                    cartesianSplit.add(right);
                    cartesianResultSplits.set(i, cartesianSplit);
                    i++;
                }
            }

            LOG.info("Total splits to process: " + cartesianResultSplits.size());
            return cartesianResultSplits;
        } catch (InterruptedException exception) {
            exception.printStackTrace();
            throw new IOException(exception);
        }
    }

    @Override
    public RecordReader createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new CartesianRecordReader();
    }
}


