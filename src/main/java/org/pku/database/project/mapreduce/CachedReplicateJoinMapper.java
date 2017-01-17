package org.pku.database.project.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.pku.database.project.join.Dataset;

import javax.xml.crypto.Data;
import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by aji on 2017/1/17.
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
abstract public class CachedReplicateJoinMapper<T> extends ReplicateJoinMapper<T, Object, Text, NullWritable, Text> {

    private static final String CACHED_DATASET = "org.pku.database.project.cache.dataset.which";

    @Deprecated
    private static final String CACHED_DATASET_PATH = "org.pku.database.project.cache.dataset.path";

    @Deprecated
    private static void setCachedDatasetPath(Configuration conf, URI ...uris) {
        List<String> urisStr = new ArrayList<>();
        for (URI uri: uris) {
            urisStr.add(uri.toString());
        }
        conf.set(CACHED_DATASET_PATH, String.join(",", urisStr));
    }

    @Deprecated
    private static URI[] getCachedDatasetPath(Configuration conf) {
        String[] pathsStr = conf.get(CACHED_DATASET_PATH).split(",");
        URI[] uris = new URI[pathsStr.length];
        int i = 0;
        for (String pathURI: pathsStr) {
            uris[i] = URI.create(pathURI);
            i++;
        }
        return uris;
    }

    private static void testCacheIsEmpty(Job job) {
        Dataset dataset = job.getConfiguration().getEnum(CACHED_DATASET, Dataset.NULL);
        if (dataset != Dataset.NULL) {
            throw new RuntimeException("Cache is already set.");
        }
    }

    private static void addDataset(Job job, URI uri, Dataset which) {
        testCacheIsEmpty(job);
        setCachedDatasetPath(job.getConfiguration(), uri); // TODO 使用hdfs的话直接使用CacheFile即可
        // job.addCacheFile(uri);
        job.getConfiguration().setEnum(CACHED_DATASET, which);
    }

    public static void cacheLeft(Job job, URI uri) {
        addDataset(job, uri, Dataset.LEFT);
    }

    public static void cacheRight(Job job, URI uri) {
        addDataset(job, uri, Dataset.RIGHT);
    }

    private List<T> cacheList;
    private Dataset which;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        which = context.getConfiguration().getEnum(CACHED_DATASET, Dataset.NULL);

        URI[] files = getCachedDatasetPath(context.getConfiguration()); // TODO 使用hdfs的话直接使用CacheFile即可

        // URI[] files = context.getCacheFiles();

        if (which == Dataset.NULL || files == null || files.length == 0) {
            throw new RuntimeException("Not dataset in DistributedCache");
        }
        cacheList = new ArrayList<>();
        for (URI p: files) {
            try (BufferedReader rdr = new BufferedReader(new InputStreamReader(new FileInputStream(new File(p))))) {
                String line;
                while ((line = rdr.readLine()) != null) {
                    cacheList.add(transform(line));
                }
            }
        }
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        T transformed = transform(value.toString());
        for (T cached: cacheList) {
            T left, right;
            if (which == Dataset.LEFT) {
                left = cached;
                right = transformed;
            } else {
                left = transformed;
                right = cached;
            }
            if (condition.test(left, right)) {
                context.write(NullWritable.get(), new Text(combine(left, right)));
            }
        }
    }

}
