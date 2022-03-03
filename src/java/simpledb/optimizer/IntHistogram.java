package simpledb.optimizer;

import simpledb.execution.Predicate;

import java.math.BigDecimal;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int[] buckets;
    private int numOfBuckets;
    private int min;
    private int max;
    private int wb;
    private int numOfTuples;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this.buckets = new int[buckets];
        this.numOfBuckets = buckets;
        this.min = min;
        this.max = max;
        this.wb = (max - min + 1) / numOfBuckets;
        this.numOfTuples = 0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        int index = v / wb;
        buckets[index]++;
        numOfTuples++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {

    	// some code goes here
        switch (op) {
            case LESS_THAN: return estimateLessThan(v);
            case EQUALS: return estimateEquals(v);
            case GREATER_THAN: return estimateGreaterThan(v);
            case NOT_EQUALS: return estimateNotEquals(v);
            case LESS_THAN_OR_EQ: return estimateLessThanOrEq(v);
            case GREATER_THAN_OR_EQ: return estimateGreaterThanOrEq(v);
        }
        return 0.0;
    }

    private double estimateGreaterThanOrEq(int v) {
        return 0.0;
    }

    private double estimateLessThanOrEq(int v) {
        return 0.0;
    }

    private double estimateNotEquals(int v) {
        return 0.0;
    }

    private double estimateGreaterThan(int v) {
        return 0.0;
    }

    private double estimateEquals(int v) {
        int index = v / wb;
        int height = buckets[index];
        double selectivity = ((double) height / (double) wb) / (double) numOfTuples;
        return selectivity;
    }

    private double estimateLessThan(int v) {
        return 0.0;
    }

    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return buckets.toString();
    }
}
