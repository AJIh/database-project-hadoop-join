package condition.impl;

import condition.Condition;
import org.apache.hadoop.hbase.client.Result;

/**
 * Created by aji on 2017/1/16.
 */
public class NotConditionImpl implements Condition {

    private Condition operand;

    public NotConditionImpl(Condition operand) {
        this.operand = operand;
    }

    @Override
    public Boolean test(Result a, Result b) {
        return !this.operand.test(a, b);
    }
}
