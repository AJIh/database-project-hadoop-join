package join;

import condition.Condition;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;

/**
 * Created by aji on 2017/1/16.
 */
public abstract class ReplicateJoinTool implements Tool {
    private Configuration conf;

    private Class<? extends Condition> filterClass;

    public void setFilterClass(Class<? extends Condition> filterClass) {
        this.filterClass = filterClass;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = new Configuration(conf); // copy origin configuration
    }

    @Override
    public Configuration getConf() {
        return conf;
    }
}
