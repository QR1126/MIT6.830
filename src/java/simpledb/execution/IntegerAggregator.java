package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbField;
    private Type gbFieldType;
    private int aField;
    private Op what;
    private Map<Field, Object> groupIntegerAggVal;
    private TupleDesc integerAggDesc;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbField = gbfield;
        this.gbFieldType = gbfieldtype;
        this.aField = afield;
        this.what = what;
        this.groupIntegerAggVal = new ConcurrentHashMap<>();
        if (this.gbField == NO_GROUPING) {
            this.integerAggDesc = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"No_Grouping aggregate value"});
        } else {
            this.integerAggDesc = new TupleDesc(new Type[]{Type.INT_TYPE, gbfieldtype}, new String[]{"Grouping aggregate value"});
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) throws Exception {
        // some code goes here
        assert tup.getField(gbField).getType() == gbFieldType;
        Field field = this.gbField == NO_GROUPING ? null : tup.getField(gbField);
        Object oldValue = groupIntegerAggVal.get(field);
        if (oldValue == null) {
            oldValue = init();
        }
        Object aggVal = aggregate(oldValue, ((IntField) tup.getField(aField)).getValue());
        groupIntegerAggVal.put(field, aggVal);
    }

    private Object init() {
        switch (what) {
            case MAX: return Integer.MIN_VALUE;
            case MIN: return Integer.MAX_VALUE;
            case SUM: case COUNT: return 0;
            case AVG: return new pair(0 , 0);
        }
        assert false;
        return null;
    }

    private Object aggregate(Object oldVal, int newVal) {
        switch (what) {
            case MIN: return Integer.min((Integer) oldVal, newVal);
            case MAX: return Integer.max((Integer) oldVal, newVal);
            case COUNT: return (Integer) oldVal + 1;
            case SUM: return (Integer) oldVal + newVal;
            case AVG:
                pair p = (pair) oldVal;
                return new pair(p.first + newVal, p.second + 1);
        }
        assert false;
        return null;
    }

    private class pair {
        Integer first;
        Integer second;

        public pair(Integer first, Integer second) {
            this.first = first;
            this.second = second;
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        // throw new UnsupportedOperationException("please implement me for lab2");
        ArrayList<Tuple> IntegerAggregatorList = new ArrayList<>();
        for (Map.Entry<Field, Object> entry : groupIntegerAggVal.entrySet()) {
            Field groupVal = entry.getKey();
            int aggregateVal;
            if (what.equals(Op.AVG)) {
                pair aggPair = (pair) entry.getValue();
                aggregateVal = aggPair.first / aggPair.second;
            } else {
                aggregateVal = (int) entry.getValue();
            }
            Tuple tuple = new Tuple(integerAggDesc);
            if (gbField == NO_GROUPING) {
                tuple.setField(0, new IntField(aggregateVal));
            } else {
                tuple.setField(0, groupVal);
                tuple.setField(1, new IntField(aggregateVal));
            }
            IntegerAggregatorList.add(tuple);
        }
        return new TupleIterator(integerAggDesc, IntegerAggregatorList);
    }

}
