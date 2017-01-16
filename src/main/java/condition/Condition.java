package condition;

/**
 * Created by aji on 2017/1/16.
 */
public interface Condition<T> {
    Boolean test(T a, T b);
}
