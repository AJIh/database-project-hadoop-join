package cartesian;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.join.CompositeInputSplit;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;

@SuppressWarnings({ "static-access", "rawtypes", "unchecked" })
public class CartesianRecordReader<K1, V1, K2, V2> extends RecordReader<Text, Text> {
    private RecordReader leftRR = null, rightRR = null;

    private FileInputFormat rightFIF;
    private InputSplit rightIS;
    private TaskAttemptContext rightContext;

    private Text key = new Text(), value = new Text();

    private boolean goToNextLeft = true, allDone = false;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext jobContext) throws IOException, InterruptedException {
        CompositeInputSplit split = (CompositeInputSplit) inputSplit;
        Configuration conf = jobContext.getConfiguration();
        this.rightIS = split.get(1);
        this.rightContext = jobContext;

        Class<? extends FileInputFormat> leftInputFileFormat = CartesianConfUtils.getLeftInputFormat(conf);
        FileInputFormat leftFIF = ReflectionUtils.newInstance(leftInputFileFormat, conf);
        leftRR = leftFIF.createRecordReader(split.get(0), jobContext);

        Class<? extends FileInputFormat> rightInputFileFormat = CartesianConfUtils.getRightInputFormat(conf);
        rightFIF = ReflectionUtils.newInstance(rightInputFileFormat, conf);
        rightRR = rightFIF.createRecordReader(rightIS, jobContext);
    }

    @Override
    public void close() throws IOException {
        leftRR.close();
        rightRR.close();
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return this.leftRR.getProgress();
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        do {
            if (goToNextLeft) {
                if (!leftRR.nextKeyValue()) {
                    allDone = true;
                    break;
                } else {
                    K1 lkey = (K1) this.leftRR.getCurrentKey();
                    V1 lvalue = (V1) this.leftRR.getCurrentValue();

                    key.set(lvalue.toString());

                    goToNextLeft = allDone = false;
                    rightRR = rightFIF.createRecordReader(rightIS, rightContext);
                }
            }

            if (rightRR.nextKeyValue()) {
                K2 rkey = (K2) this.rightRR.getCurrentKey();
                V2 rvalue = (V2) this.rightRR.getCurrentValue();

                value.set(rvalue.toString());
            } else {
                goToNextLeft = true;
            }
        } while (goToNextLeft);

        return !allDone;
    }
}
