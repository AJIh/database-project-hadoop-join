package cartesian;

import condition.Condition;
import merger.Merger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;

/**
 * Created by aji on 2017/1/16.
 */
public class CartesianMapper extends Mapper<Text, Text, NullWritable, Text> {
    private Condition condition = new Condition() {
        @Override
        public Boolean test(Text a, Text b) {
            return true;
        }
    };

    private Merger merger = new Merger() {
        @Override
        public Text merge(Text a, Text b) {
            return new Text(a.toString() + b.toString());
        }
    };

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();

        Class<? extends Condition> conditionClass = CartesianConfUtils.getCartesianProductMapperFilter(conf);
        if (conditionClass != null) {
            condition = ReflectionUtils.newInstance(conditionClass, conf);
        }

        Class<? extends Merger> mergerClass = CartesianConfUtils.getCartesianProductMapperMerger(conf);
        if (mergerClass != null) {
            merger = ReflectionUtils.newInstance(mergerClass, conf);
        }
    }

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        if (condition.test(key, value)) {
            context.write(NullWritable.get(), merger.merge(key, value));
        }
    }
}