package mapreduce.impl;

import com.alibaba.fastjson.JSONObject;
import mapreduce.DiskReplicateJoinMapper;

/**
 * Created by aji on 2017/1/16.
 */
public class JSONDiskReplicateJoinMapper extends DiskReplicateJoinMapper<JSONObject> {
    @Override
    public JSONObject transform(String t) {
        return JSONObject.parseObject(t);
    }

    @Override
    public String combine(JSONObject a, JSONObject b) {
        JSONObject jsonNew = new JSONObject();
        jsonNew.putAll(a);
        jsonNew.putAll(b);
        return jsonNew.toString();
    }
}
