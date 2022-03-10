package simpledb.optimizer;

import simpledb.common.Catalog;
import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.execution.Predicate;
import simpledb.execution.SeqScan;
import simpledb.storage.*;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentMap<String, TableStats> statsMap = new ConcurrentHashMap<>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(Map<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException | SecurityException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    private int tableId;
    private int ioCostPerPage;
    private Catalog.Table table;
    private String tableName;
    private HeapFile heapFile;

    private Map<Integer, IntHistogram> intHistogramMap;
    private Map<Integer, StringHistogram> stringHistogramMap;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
        this.tableId = tableid;
        this.ioCostPerPage = ioCostPerPage;
        this.table = Database.getCatalog().getTable(tableid);
        this.tableName = table.getName();
        this.heapFile = (HeapFile) table.getFile();
        TupleDesc heapFileTupleDesc = heapFile.getTupleDesc();
        int numFields = heapFileTupleDesc.numFields();

        int[] minVal = new int[numFields];
        int[] maxVal = new int[numFields];
        Arrays.fill(minVal, Integer.MAX_VALUE);
        Arrays.fill(maxVal, Integer.MIN_VALUE);

        this.intHistogramMap = new ConcurrentHashMap<>();
        this.stringHistogramMap = new ConcurrentHashMap<>();

        DbFileIterator dbFileIterator = heapFile.iterator(new TransactionId());

        try {
            dbFileIterator.open();
            //get the minVal and maxVal for each field
            while (dbFileIterator.hasNext()) {
                Tuple tuple = dbFileIterator.next();
                Iterator<Field> fieldIterator = tuple.fields();
                int index = 0;
                while (fieldIterator.hasNext()) {
                    Field field = fieldIterator.next();
                    if (field.getType().equals(Type.INT_TYPE)) {
                        IntField intField = (IntField) field;
                        minVal[index] = Math.min(minVal[index], intField.getValue());
                        maxVal[index] = Math.max(maxVal[index], intField.getValue());
                    } else {
                        StringField stringField = (StringField) field;
                        minVal[index] = Math.min(minVal[index], stringToInt(stringField.getValue()));
                        maxVal[index] = Math.max(maxVal[index], stringToInt(stringField.getValue()));
                    }
                    index++;
                }
            }
            dbFileIterator.rewind();
            // add val to histogram
            while (dbFileIterator.hasNext()) {
                Tuple tuple = dbFileIterator.next();
                Iterator<Field> iterator = tuple.fields();
                int index = 0;
                while (iterator.hasNext()) {
                    Field field = iterator.next();
                    if (field.getType().equals(Type.INT_TYPE)) {
                        IntField intField = (IntField) field;
                        IntHistogram intHistogram = intHistogramMap.get(index);
                        if (intHistogram == null) {
                            intHistogram = new IntHistogram(NUM_HIST_BINS, minVal[index], maxVal[index]);
                        }
                        intHistogram.addValue(intField.getValue());
                        intHistogramMap.put(index, intHistogram);
                    } else {
                        StringField stringField = (StringField) field;
                        StringHistogram stringHistogram = stringHistogramMap.get(index);
                        if (stringHistogram == null) {
                            stringHistogram = new StringHistogram(NUM_HIST_BINS);
                        }
                        stringHistogram.addValue(stringField.getValue());
                        stringHistogramMap.put(index, stringHistogram);
                    }
                    index++;
                }
            }
            dbFileIterator.close();

        } catch (DbException e) {
            e.printStackTrace();
        } catch (TransactionAbortedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Convert a string to an integer, with the property that if the return
     * value(s1) < return value(s2), then s1 < s2
     */
    private int stringToInt(String s) {
        int i;
        int v = 0;
        for (i = 3; i >= 0; i--) {
            if (s.length() > 3 - i) {
                int ci = s.charAt(3 - i);
                v += (ci) << (i * 8);
            }
        }

        // XXX: hack to avoid getting wrong results for
        // strings which don't output in the range min to max
        if (!(s.equals("") || s.equals("zzzz"))) {
            if (v < minVal()) {
                v = minVal();
            }

            if (v > maxVal()) {
                v = maxVal();
            }
        }

        return v;
    }

    /** @return the maximum value indexed by the histogram */
    int maxVal() {
        return stringToInt("zzzz");
    }

    /** @return the minimum value indexed by the histogram */
    int minVal() {
        return stringToInt("");
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        return ioCostPerPage * heapFile.numPages();
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
        return ((int) (totalTuples() * selectivityFactor));
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
        if (constant.getType().equals(Type.INT_TYPE)) {
            IntHistogram intHistogram = intHistogramMap.get(field);
            IntField intConstant = (IntField) constant;
            return intHistogram.estimateSelectivity(op, intConstant.getValue());
        } else {
            StringHistogram stringHistogram = stringHistogramMap.get(field);
            StringField stringField = (StringField) constant;
            return stringHistogram.estimateSelectivity(op, stringField.getValue());
        }
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here
        HeapFile heapFile = (HeapFile) table.getFile();
        int numOfTuples = 0;
        DbFileIterator iterator = heapFile.iterator(new TransactionId());
        try {
            while (iterator.hasNext()) {
                iterator = (DbFileIterator) iterator.next();
                numOfTuples++;
            }
        } catch (TransactionAbortedException e) {
            e.printStackTrace();
        } catch (DbException e) {
            e.printStackTrace();
        }
        return numOfTuples;
    }

}
