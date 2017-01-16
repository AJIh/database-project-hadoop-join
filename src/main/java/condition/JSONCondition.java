package condition;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.io.Text;

/**
 * Created by aji on 2017/1/16.
 */
public abstract class JSONCondition implements Condition {
    abstract Boolean testJSON(JSONObject a, JSONObject b);

    @Override
    final public Boolean test(Text a, Text b) {
        JSONObject jsonA = JSONObject.parseObject(a.toString());
        JSONObject jsonB = JSONObject.parseObject(b.toString());
        return testJSON(jsonA, jsonB);
    }
}
