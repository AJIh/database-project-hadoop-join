package org.pku.database.project.costmodel;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;

/**
 * Created by aji on 2017/1/17.
 */
abstract public class FileCostModel implements CostModel {
    protected FileSystem fs;
    protected Long maxCacheLength = 10485760L;

    public FileCostModel(Configuration conf) throws IOException {
        this.fs = FileSystem.get(conf);
    }

    public FileCostModel(Configuration conf, Long maxCacheLength) throws IOException {
        this.fs = FileSystem.get(conf);
        this.maxCacheLength = maxCacheLength;
    }
}
