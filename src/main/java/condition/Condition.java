package condition;

import org.apache.hadoop.io.Text;

/**
 * Created by aji on 2017/1/16.
 */
public interface Condition {
    Boolean test(Text a, Text b);
}
