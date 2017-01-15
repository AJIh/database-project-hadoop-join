package condition.impl;

import condition.Condition;
import org.apache.hadoop.hbase.client.Result;

import java.util.function.BiFunction;

/**
 * Created by aji on 2017/1/16.
 */
public class BaseConditionImpl implements Condition {
    private BiFunction<Result, Result, Boolean> testFn;

    public BaseConditionImpl(BiFunction<Result, Result, Boolean> testFn) {
        this.testFn = testFn;
    }

    @Override
    public Boolean test(Result a, Result b) {
        return this.testFn.apply(a, b);
    }
}
