package com.taobao.tddl.executor.function.scalar;

import java.math.BigDecimal;
import java.math.RoundingMode;

import com.taobao.tddl.common.exception.NotSupportException;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * @since 5.1.0
 */
public class Division extends ScalarFunction {

    @Override
    public void compute(Object[] args) {
        this.result = computeInner(args);
    }

    private Comparable computeInner(Object[] args) {
        if (args[0] instanceof Long || args[0] instanceof Integer) {
            return ((Number) args[0]).longValue() * 1.0 / ((Number) args[1]).longValue() * 1.0;
        } else if (args[0] instanceof Double || args[0] instanceof Float) {
            return ((Number) args[0]).doubleValue() / ((Number) args[1]).doubleValue();
        } else if (args[0] instanceof String || args[1] instanceof String) {
            return Double.parseDouble(args[0].toString()) / Double.parseDouble(args[1].toString());
        } else if (args[0] instanceof BigDecimal) {
            BigDecimal o = new BigDecimal(args[0].toString());
            BigDecimal o2 = new BigDecimal(args[1].toString());
            return o.divide(o2, 4, RoundingMode.HALF_DOWN);
        } else {
            throw new NotSupportException("Division Type");
        }
    }

    @Override
    public DataType getReturnType() {
        return DataType.DoubleType;
    }

}
