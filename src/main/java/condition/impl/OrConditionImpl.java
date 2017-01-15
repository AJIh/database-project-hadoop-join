package condition.impl;

import condition.Condition;
import org.apache.hadoop.hbase.client.Result;

/**
 * Created by aji on 2017/1/16.
 */
public class OrConditionImpl implements Condition {
    private Condition leftOperand;
    private Condition rightOperand;

    public OrConditionImpl(Condition leftOperand, Condition rightOperand) {
        this.leftOperand = leftOperand;
        this.rightOperand = rightOperand;
    }

    @Override
    public Boolean test(Result a, Result b) {
        return this.leftOperand.test(a, b) || this.rightOperand.test(a, b);
    }
}
