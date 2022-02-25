package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbField;
    private Type gbFieldType;
    private int aField;
    private Op what;
    private Map<Field, Integer> groupStringAggVal;
    private TupleDesc StringAggDesc;
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbField = gbfield;
        this.gbFieldType = gbfieldtype;
        this.aField = afield;
        if (!what.equals(Op.COUNT)) throw new IllegalArgumentException("Exception: what != COUNT");
        this.what = what;
        groupStringAggVal = new ConcurrentHashMap<>();
        if (this.gbField == NO_GROUPING) {
            this.StringAggDesc = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"No_Grouping aggregate value"});
        } else {
            this.StringAggDesc = new TupleDesc(new Type[]{Type.INT_TYPE, gbfieldtype}, new String[]{"Grouping aggregate value", "Grouping fieldType"});
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        assert tup.getField(gbField).getType().equals(gbFieldType);
        Field field = gbField == NO_GROUPING ? null : tup.getField(gbField);
//        if (!groupStringAggVal.containsKey(field)) groupStringAggVal.put(field, 0);
//        groupStringAggVal.put(field, groupStringAggVal.get(field) + 1);
        groupStringAggVal.put(field, groupStringAggVal.getOrDefault(field, 0) + 1);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        //throw new UnsupportedOperationException("please implement me for lab2");
        List<Tuple> tupleList = new ArrayList<>();
        for (Map.Entry<Field, Integer> entry : groupStringAggVal.entrySet()) {
            Field groupVal = entry.getKey();
            Integer aggVal = entry.getValue();
            Tuple tuple = new Tuple(StringAggDesc);
            if (gbField == NO_GROUPING) {
                tuple.setField(0, new IntField(aggVal));
            } else {
                tuple.setField(0, groupVal);
                tuple.setField(1, new IntField(aggVal));
            }
            tupleList.add(tuple);
        }
        return new TupleIterator(StringAggDesc, tupleList);
    }

}
