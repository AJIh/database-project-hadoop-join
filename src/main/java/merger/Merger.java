package merger;

import org.apache.hadoop.io.Text;

/**
 * Created by aji on 2017/1/16.
 */
public interface Merger {
    Text merge(Text a, Text b);
}
