package mapreduce;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by aji on 2017/1/16.
 */
abstract public class DiskReplicateJoinMapper<T> extends ReplicateJoinMapper<T, Text, Text, NullWritable, Text> {
    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        T a = transform(key.toString());
        T b = transform(value.toString());
        if (condition.test(a, b)) {
            Text valueOut = new Text(combine(a, b));
            context.write(NullWritable.get(), valueOut);
        }
    }
}
