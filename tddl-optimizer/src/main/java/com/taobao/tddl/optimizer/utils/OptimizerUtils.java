package com.taobao.tddl.optimizer.utils;

import java.text.ParseException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.time.DateFormatUtils;

import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.common.utils.TStringUtil;
import com.taobao.tddl.optimizer.config.table.ColumnMeta;
import com.taobao.tddl.optimizer.config.table.IndexMeta;
import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.datatype.DataType;
import com.taobao.tddl.optimizer.core.expression.IBooleanFilter;
import com.taobao.tddl.optimizer.core.expression.IColumn;
import com.taobao.tddl.optimizer.core.expression.IFilter;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.expression.ILogicalFilter;
import com.taobao.tddl.optimizer.core.expression.IOrderBy;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.core.expression.bean.BindVal;
import com.taobao.tddl.optimizer.core.expression.bean.NullValue;
import com.taobao.tddl.optimizer.parse.cobar.visitor.MySqlExprVisitor;

/**
 * @since 5.1.0
 */
public class OptimizerUtils {

    private static final String[] DATE_FORMATS = new String[] { "yyyy-MM-dd", "HH:mm:ss", "yyyy-MM-dd HH:mm:ss",
            "yyyy-MM-dd hh:mm:ss.S", "EEE MMM dd HH:mm:ss zzz yyyy", DateFormatUtils.ISO_DATETIME_FORMAT.getPattern(),
            DateFormatUtils.ISO_DATETIME_TIME_ZONE_FORMAT.getPattern(),
            DateFormatUtils.SMTP_DATETIME_FORMAT.getPattern(), };

    public static Object convertType(Object value, DataType type) {
        if (value == null) {
            return null;
        }

        if (type == null || value instanceof BindVal || value instanceof IFunction || value instanceof NullValue) {
            return value;
        }

        return type.convertFrom(value);

        // String strValue = value.toString();
        // try {
        // if (type.equals(DATA_TYPE.LONG_VAL)) {
        // value = Long.valueOf(strValue);
        // } else if (type.equals(DATA_TYPE.INT_VAL)) {
        // value = Integer.valueOf(strValue);
        // } else if (type.equals(DATA_TYPE.BOOLEAN_VAL)) {
        // value = Boolean.valueOf(strValue);
        // } else if (type.equals(DATA_TYPE.BYTES_VAL)) {
        // value = Byte.valueOf(strValue);
        // } else if (type.equals(DATA_TYPE.CHAR_VAL)) {
        // value = strValue;
        // } else if (type.equals(DATA_TYPE.DATE_VAL) ||
        // type.equals(DATA_TYPE.TIMESTAMP_VAL)) {
        // if (value != null && value instanceof Long) {
        // value = new Date((Long) value);
        // }
        //
        // if (!(value instanceof Date)) {
        // value = null;
        // try {
        // value = parseDate(strValue.trim(), DATE_FORMATS, Locale.ENGLISH);
        // } catch (Exception err) {
        // try {
        // value = parseDate(strValue.trim(), DATE_FORMATS,
        // Locale.getDefault());
        // } catch (Exception e) {
        // throw new TddlRuleException("unSupport date parse :" +
        // strValue.trim());
        // }
        // }
        // }
        // } else if (type.equals(DATA_TYPE.DOUBLE_VAL)) {
        // value = Double.valueOf(strValue);
        // } else if (type.equals(DATA_TYPE.FLOAT_VAL)) {
        // value = Float.valueOf(strValue);
        // } else if (type.equals(DATA_TYPE.SHORT_VAL)) {
        // value = Short.valueOf(strValue);
        // } else if (type.equals(DATA_TYPE.STRING_VAL)) {
        // value = strValue;
        // } else {
        // throw new UnsupportedOperationException("Not supported yet : " +
        // type);
        // }
        // } catch (Exception ex) {
        // // 若转换失败，则交给filter的function自己处理
        // return value;
        // }
        //
        // return value;
    }

    private static Date parseDate(String str, String[] parsePatterns, Locale locale) throws ParseException {
        if ((str == null) || (parsePatterns == null)) {
            throw new IllegalArgumentException("Date and Patterns must not be null");
        }

        SimpleDateFormat parser = null;
        ParsePosition pos = new ParsePosition(0);

        for (int i = 0; i < parsePatterns.length; i++) {
            if (i == 0) {
                parser = new SimpleDateFormat(parsePatterns[0], locale);
            } else {
                parser.applyPattern(parsePatterns[i]);
            }
            pos.setIndex(0);
            Date date = parser.parse(str, pos);
            if ((date != null) && (pos.getIndex() == str.length())) {
                return date;
            }
        }

        throw new ParseException("Unable to parse the date: " + str, -1);
    }

    public static IFilter copyFilter(IFilter f) {
        return (IFilter) (f == null ? null : f.copy());
    }

    public static Set<ISelectable> copySelectables(Set<ISelectable> cs) {
        if (cs == null) {
            return null;
        }
        Set<ISelectable> news = new HashSet(cs.size());
        for (ISelectable c : cs) {
            news.add(c.copy());
        }

        return news;
    }

    public static List<ISelectable> copySelectables(List<ISelectable> cs) {
        if (cs == null) {
            return null;
        }

        List<ISelectable> news = new ArrayList(cs.size());
        for (ISelectable c : cs) {
            news.add(c.copy());
        }

        return news;
    }

    public static List<IBooleanFilter> copyFilter(List<IBooleanFilter> filters) {
        if (filters == null) {
            return null;
        }

        List<IBooleanFilter> newFilters = new ArrayList<IBooleanFilter>(filters.size());
        for (IBooleanFilter f : filters) {
            newFilters.add(f.copy());
        }
        return newFilters;
    }

    public static List<IOrderBy> copyOrderBys(List<IOrderBy> orders) {
        if (orders == null) {
            return null;
        }

        List<IOrderBy> newOrders = new ArrayList<IOrderBy>(orders.size());

        for (IOrderBy o : orders) {
            newOrders.add(o.copy());
        }

        return newOrders;
    }

    public static List<ISelectable> copySelectables(List<ISelectable> selects, String tableName) {
        if (tableName == null) {
            return copySelectables(selects);
        }

        if (selects == null) {
            return null;
        }

        List<ISelectable> news = new ArrayList(selects.size());
        for (ISelectable s : selects) {
            ISelectable a = s.copy();
            if (a instanceof IColumn) {
                setColumn((IColumn) a, tableName);
            } else if (a instanceof IFilter) {
                setFilter((IFilter) a, tableName);
            } else if (a instanceof IFunction) {
                setFunction((IFunction) a, tableName);
            }

            news.add(a);
        }

        return news;
    }

    public static List<IOrderBy> copyOrderBys(List<IOrderBy> orderBys, String tableName) {
        if (tableName == null) {
            return copyOrderBys(orderBys);
        }

        if (orderBys == null) {
            return null;
        }

        List<IOrderBy> news = new ArrayList(orderBys.size());
        for (IOrderBy o : orderBys) {
            IOrderBy a = o.copy();
            if (a.getColumn() instanceof IColumn) {
                setColumn((IColumn) a.getColumn(), tableName);
            } else if (a.getColumn() instanceof IFilter) {
                setFilter((IFilter) a.getColumn(), tableName);
            } else if (a.getColumn() instanceof IFunction) {
                setFunction((IFunction) a.getColumn(), tableName);
            }

            news.add(a);
        }

        return news;
    }

    public static IFilter copyFilter(IFilter filter, String tableName) {
        if (filter == null) {
            return null;
        }

        IFilter newFilter = (IFilter) filter.copy();
        if (tableName != null) {
            setFilter(newFilter, tableName);
        }
        return newFilter;
    }

    private static void setFunction(IFunction f, String tableName) {
        for (Object arg : f.getArgs()) {
            if (arg instanceof ISelectable) {
                if (arg instanceof IColumn) {
                    setColumn((IColumn) arg, tableName);
                } else if (arg instanceof IFilter) {
                    setFilter((IFilter) arg, tableName);
                } else if (arg instanceof IFunction) {
                    setFunction((IFunction) arg, tableName);
                }
            }

        }
    }

    private static void setFilter(IFilter f, String tableName) {
        if (f instanceof IBooleanFilter) {
            Object column = ((IBooleanFilter) f).getColumn();
            if (column instanceof IColumn) {
                setColumn((IColumn) column, tableName);
            } else if (column instanceof IFilter) {
                setFilter((IFilter) column, tableName);
            } else if (column instanceof IFunction) {
                setFunction((IFunction) column, tableName);
            }

            Object value = ((IBooleanFilter) f).getValue();
            if (value instanceof IColumn) {
                setColumn((IColumn) value, tableName);
            } else if (value instanceof IFilter) {
                setFilter((IFilter) value, tableName);
            } else if (value instanceof IFunction) {
                setFunction((IFunction) value, tableName);
            }
        } else if (f instanceof ILogicalFilter) {
            for (IFilter sf : ((ILogicalFilter) f).getSubFilter()) {
                setFilter(sf, tableName);
            }
        }
    }

    private static void setColumn(IColumn c, String tableName) {
        if (tableName != null && c.getTableName() != null) {
            c.setTableName(tableName);
        }
    }

    /**
     * 根据索引信息，构建orderby条件
     */
    public static List<IOrderBy> getOrderBy(IndexMeta meta) {
        if (meta == null) {
            return new ArrayList<IOrderBy>(0);
        }

        List<IOrderBy> _orderBys = new ArrayList<IOrderBy>();
        for (ColumnMeta c : meta.getKeyColumns()) {
            IColumn column = ASTNodeFactory.getInstance().createColumn();
            column.setTableName(c.getTableName())
                .setColumnName(c.getName())
                .setDataType(c.getDataType())
                .setAlias(c.getAlias());
            IOrderBy orderBy = ASTNodeFactory.getInstance().createOrderBy().setColumn(column).setDirection(true);
            _orderBys.add(orderBy);
        }
        return _orderBys;
    }

    /**
     * 根据column string构造{@linkplain ISelectable}对象
     * 
     * @param columnStr
     * @return
     */
    public static ISelectable createColumnFromString(String columnStr) {
        if (columnStr == null) {
            return null;
        }

        // 别名只能单独处理
        if (TStringUtil.containsIgnoreCase(columnStr, " AS ")) {
            String tmp[] = TStringUtil.splitByWholeSeparator(columnStr, " AS ");
            if (tmp.length != 2) {
                throw new RuntimeException("createColumnFromString:" + columnStr);
            }

            ISelectable c = createColumnFromString(tmp[0].trim());
            c.setAlias(tmp[1].trim());
            return c;
        } else {
            MySqlExprVisitor visitor = MySqlExprVisitor.parser(columnStr);
            Comparable value = MySqlExprVisitor.parser(columnStr).getColumnOrValue();
            if (value instanceof ISelectable) {
                return (ISelectable) value;
            } else if (value instanceof IFilter) {
                return (IFilter) value;
            } else { // 可能是常量
                return visitor.buildConstanctFilter(value);
            }
        }
    }

    public static IColumn columnMetaToIColumn(ColumnMeta m, String tableName) {
        IColumn c = ASTNodeFactory.getInstance().createColumn();
        c.setDataType(m.getDataType());
        c.setColumnName(m.getName());
        c.setTableName(tableName);
        c.setAlias(m.getAlias());
        return c;
    }

    public static IColumn columnMetaToIColumn(ColumnMeta m) {
        IColumn c = ASTNodeFactory.getInstance().createColumn();
        c.setDataType(m.getDataType());
        c.setColumnName(m.getName());
        c.setTableName(m.getTableName());
        c.setAlias(m.getAlias());
        return c;
    }

    public static IColumn getColumn(Object column) {
        if (column instanceof IFunction) {
            return ASTNodeFactory.getInstance()
                .createColumn()
                .setTableName(((IFunction) column).getTableName())
                .setColumnName(((IFunction) column).getColumnName())
                .setAlias(((IFunction) column).getAlias())
                .setDataType(((IFunction) column).getDataType());
        } else if (!(column instanceof IColumn)) {
            throw new IllegalArgumentException("column :" + column + " is not a icolumn");
        }

        return (IColumn) column;
    }

    /**
     * 将columnMeta转化为column列
     */
    public static List<ISelectable> columnMetaListToIColumnList(Collection<ColumnMeta> ms, String tableName) {
        List<ISelectable> cs = new ArrayList(ms.size());
        for (ColumnMeta m : ms) {
            cs.add(columnMetaToIColumn(m, tableName));
        }

        return cs;
    }

    public static List<ISelectable> columnMetaListToIColumnList(Collection<ColumnMeta> ms) {
        List<ISelectable> cs = new ArrayList(ms.size());
        for (ColumnMeta m : ms) {
            cs.add(columnMetaToIColumn(m));
        }

        return cs;
    }

    // --------------------------- assignment --------------------------

    public static IFilter assignment(IFilter f, Map<Integer, ParameterContext> parameterSettings) {
        if (f == null) {
            return null;
        }

        return (IFilter) f.assignment(parameterSettings);
    }

    public static ISelectable assignment(ISelectable c, Map<Integer, ParameterContext> parameterSettings) {
        if (c == null) {
            return c;
        }

        return c.assignment(parameterSettings);
    }

    public static List<ISelectable> assignment(List<ISelectable> cs, Map<Integer, ParameterContext> parameterSettings) {
        if (cs == null) {
            return cs;
        }
        for (ISelectable s : cs) {
            assignment(s, parameterSettings);
        }

        return cs;
    }

}
