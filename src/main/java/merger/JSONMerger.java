package merger;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.io.Text;

/**
 * Created by aji on 2017/1/16.
 */
public class JSONMerger implements Merger {
    @Override
    public Text merge(Text a, Text b) {
        JSONObject jsonA = JSONObject.parseObject(a.toString());
        JSONObject jsonB = JSONObject.parseObject(b.toString());

        JSONObject jsonNew = new JSONObject();
        jsonNew.putAll(jsonA);
        jsonNew.putAll(jsonB);
        return new Text(jsonNew.toString());
    }
}
