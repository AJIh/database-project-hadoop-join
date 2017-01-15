package condition;

import condition.impl.AndConditionImpl;
import condition.impl.BaseConditionImpl;
import condition.impl.NotConditionImpl;
import condition.impl.OrConditionImpl;
import org.apache.hadoop.hbase.client.Result;

import java.util.function.BiFunction;

/**
 * Created by aji on 2017/1/16.
 */
public class ConditionFactory {

    public static BaseConditionImpl condition(BiFunction<Result, Result, Boolean> testFn) {
        return new BaseConditionImpl(testFn);
    }

    public static OrConditionImpl or(Condition leftOperand, Condition rightOperand) {
        return new OrConditionImpl(leftOperand, rightOperand);
    }

    public static NotConditionImpl not(Condition operand) {
        return new NotConditionImpl(operand);
    }

    public static AndConditionImpl and(Condition leftOperand, Condition rightOperand) {
        return new AndConditionImpl(leftOperand, rightOperand);
    }
}
