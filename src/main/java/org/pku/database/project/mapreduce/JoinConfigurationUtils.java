package org.pku.database.project.mapreduce;

import org.apache.hadoop.mapreduce.Job;
import org.pku.database.project.condition.Condition;
import org.apache.hadoop.conf.Configuration;

import java.net.URI;

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

    public static void cacheLeft(Job job, URI uri) {
        CachedReplicateJoinMapper.cacheLeft(job, uri);
    }

    public static void cacheRight(Job job, URI uri) {
        CachedReplicateJoinMapper.cacheRight(job, uri);
    }
}
