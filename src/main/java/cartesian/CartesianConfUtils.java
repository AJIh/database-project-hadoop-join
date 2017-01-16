package cartesian;

import condition.Condition;
import merger.Merger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * Created by aji on 2017/1/16.
 */
public class CartesianConfUtils {
    private static final String LEFT_INPUT_FORMAT = "cart.left.inputformat";
    private static final String LEFT_INPUT_PATH = "cart.left.path";
    private static final String RIGHT_INPUT_FORMAT = "cart.right.inputformat";
    private static final String RIGHT_INPUT_PATH = "cart.right.path";

    private static final String CARTESIAN_PRODUCT_MAPPER_FILTER = "cart.mapper.filter";

    private static final String CARTESIAN_PRODUCT_MAPPER_MERGER = "cart.mapper.merger";

    public static void setLeftInputInfo(Configuration conf, Class<? extends FileInputFormat> inputFormatClass, String inputPath) {
        conf.setClass(LEFT_INPUT_FORMAT, inputFormatClass, FileInputFormat.class);
        conf.set(LEFT_INPUT_PATH, inputPath);
    }

    static Class<? extends FileInputFormat> getLeftInputFormat(Configuration conf) {
        return conf.getClass(LEFT_INPUT_FORMAT, null, FileInputFormat.class);
    }

    static String getLeftInputPath(Configuration conf) {
        return conf.get(LEFT_INPUT_PATH);
    }

    public static void setRightInputInfo(Configuration conf, Class<? extends FileInputFormat> inputFormatClass, String inputPath) {
        conf.setClass(RIGHT_INPUT_FORMAT, inputFormatClass, FileInputFormat.class);
        conf.set(RIGHT_INPUT_PATH, inputPath);
    }

    static Class<? extends FileInputFormat> getRightInputFormat(Configuration conf) {
        return conf.getClass(RIGHT_INPUT_FORMAT, null, FileInputFormat.class);
    }

    static String getRightInputPath(Configuration conf) {
        return conf.get(RIGHT_INPUT_PATH);
    }

    public static void setCartesianProductMapperFilter(Configuration conf, Class<? extends Condition> conditionClass) {
        conf.setClass(CARTESIAN_PRODUCT_MAPPER_FILTER, conditionClass, Condition.class);
    }

    static Class<? extends Condition> getCartesianProductMapperFilter(Configuration conf) {
        return conf.getClass(CARTESIAN_PRODUCT_MAPPER_FILTER, null, Condition.class);
    }

    public static void setCartesianProductMapperMerger(Configuration conf, Class<? extends Merger> mergerClass) {
        conf.setClass(CARTESIAN_PRODUCT_MAPPER_MERGER, mergerClass, Merger.class);
    }

    static Class<? extends Merger> getCartesianProductMapperMerger(Configuration conf) {
        return conf.getClass(CARTESIAN_PRODUCT_MAPPER_MERGER, null, Merger.class);
    }
}
