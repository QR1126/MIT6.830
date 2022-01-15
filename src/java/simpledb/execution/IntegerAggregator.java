package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbField;
    private Type gbFieldType;
    private int aField;
    private Op what;
    private Map<Field, Integer> groupMap;
    private Map<Field, Integer> countMap;
    private Map<Field, List<Integer>> avgMap;

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
        this.avgMap = new HashMap<>();
        this.countMap = new HashMap<>();
        this.groupMap = new HashMap<>();
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
        IntField afield = (IntField) tup.getField(aField);
        int aValue = afield.getValue();
        Field groupByField = gbField == NO_GROUPING ? null : tup.getField(gbField);
        if (groupByField != null && !groupByField.getType().equals(this.gbFieldType)) {
            throw new Exception("Type was wrong");
        }
        switch (what) {
            case MIN:
                if (!groupMap.containsKey(groupByField)) {
                    groupMap.put(groupByField, aValue);
                }
                else {
                    groupMap.put(groupByField, Math.min(groupMap.get(groupByField), aValue));
                }
                break;

            case MAX:
                if (!groupMap.containsKey(groupByField)) {
                    groupMap.put(groupByField, aValue);
                }
                else {
                    groupMap.put(groupByField, Math.max(groupMap.get(groupByField), aValue));
                }
                break;

            case SUM:
                groupMap.put(groupByField, groupMap.getOrDefault(groupByField, 0) + aValue);
                break;

            case COUNT:
                countMap.put(groupByField, countMap.getOrDefault(groupByField, 0) + 1);
                break;

            case AVG:
                if (!avgMap.containsKey(groupByField)) {
                    List<Integer> list = new ArrayList<>();
                    list.add(aValue);
                    avgMap.put(groupByField, list);
                }
                else {
                    List<Integer> list = avgMap.get(groupByField);
                    list.add(aValue);
                }
                break;

            default:
                break;
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
        return new IntegerAggregatorIterator();
    }

    private class IntegerAggregatorIterator implements OpIterator {



        @Override
        public void open() throws DbException, TransactionAbortedException {

        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            return false;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            return null;
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {

        }

        @Override
        public TupleDesc getTupleDesc() {
            return null;
        }

        @Override
        public void close() {

        }
    }
}
