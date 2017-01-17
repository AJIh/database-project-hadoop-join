package org.pku.database.project.cartesian;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * Created by aji on 2017/1/16.
 */
public class CartesianConfUtils {
    private static final String LEFT_INPUT_FORMAT = "org.pku.database.project.cart.left.inputformat";
    private static final String LEFT_INPUT_PATH = "org.pku.database.project.cart.left.path";
    private static final String RIGHT_INPUT_FORMAT = "org.pku.database.project.cart.right.inputformat";
    private static final String RIGHT_INPUT_PATH = "org.pku.database.project.cart.right.path";


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
}
