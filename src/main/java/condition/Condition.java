package condition;

import org.apache.hadoop.hbase.client.Result;

/**
 * Created by aji on 2017/1/16.
 */
public interface Condition {
    Boolean test(Result a, Result b);
}
