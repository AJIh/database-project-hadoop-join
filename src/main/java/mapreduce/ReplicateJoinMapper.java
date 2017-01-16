package mapreduce;

import condition.Condition;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;

/**
 * Created by aji on 2017/1/16.
 */
public abstract class ReplicateJoinMapper<T, KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

    protected Condition<T> condition = new Condition<T>() {
        @Override
        public Boolean test(T a, T b) {
            return null;
        }
    };

    abstract public T transform(String t);
    abstract public String combine(T a, T b);

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();

        Class<? extends Condition> conditionClass = JoinConfigurationUtils.getJoinProductFilter(conf);
        if (conditionClass != null) {
            condition = ReflectionUtils.newInstance(conditionClass, conf);
        }
    }
}