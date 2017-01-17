package org.pku.database.project.mapreduce;

import org.pku.database.project.condition.Condition;
import org.apache.hadoop.conf.Configuration;

/**
 * Created by aji on 2017/1/16.
 */
public class JoinConfigurationUtils {
    private static final String JOIN_PRODUCT_FILTER = "org.pku.database.project.mapper.filter";

    public static void setJoinProductFilter(Configuration conf, Class<? extends Condition> conditionClass) {
        conf.setClass(JOIN_PRODUCT_FILTER, conditionClass, Condition.class);
    }

    static Class<? extends Condition> getJoinProductFilter(Configuration conf) {
        return conf.getClass(JOIN_PRODUCT_FILTER, null, Condition.class);
    }
}
