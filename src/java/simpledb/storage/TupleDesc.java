package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return tupleDescList.iterator();
    }

    private static final long serialVersionUID = 1L;

    private List<TDItem> tupleDescList;
    private Map<String, Integer> name2Index;
    private int size = 0;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        tupleDescList = new ArrayList<>();
        name2Index = new HashMap<>();
        for (int i = 0; i < typeAr.length; i++) {
            Type type = typeAr[i];
            String name = fieldAr[i];
            TDItem item = new TDItem(type, name);
            tupleDescList.add(item);
            size += type.getLen();
            if (name != null) name2Index.put(name, i);
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        tupleDescList = new ArrayList<>();
        name2Index = new HashMap<>();
        for (int i = 0; i < typeAr.length; i++) {
            Type type = typeAr[i];
            TDItem item = new TDItem(type, null);
            tupleDescList.add(item);
            size += type.getLen();
        }
    }

    public TupleDesc(List<TDItem> list) {
        tupleDescList = new ArrayList<>();
        name2Index = new HashMap<>();
        for (int i = 0; i < list.size(); i++) {
            Type type = list.get(i).fieldType;
            String name = list.get(i).fieldName;
            tupleDescList.add(new TDItem(type, name));
            if (name != null) name2Index.put(name, i);
            size += type.getLen();
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return tupleDescList.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if (i < 0 || i >= tupleDescList.size()) {
            throw new NoSuchElementException("getFieldName");
        } else {
            return tupleDescList.get(i).fieldName;
        }
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        if (i < 0 || i >= tupleDescList.size()) {
            throw new NoSuchElementException("getFieldType");
        } else {
            return tupleDescList.get(i).fieldType;
        }
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        if (name2Index.isEmpty() || !name2Index.containsKey(name)) {
            throw new NoSuchElementException("fieldNameToIndex");
        }
        return name2Index.get(name);
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        List<TDItem> list1 = td1.tupleDescList;
        List<TDItem> list2 = td2.tupleDescList;
        List<TDItem> list = new ArrayList<>();
        list.addAll(list1);
        list.addAll(list2);
        return new TupleDesc(list);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // some code goes here
        if (o == this) return true;
        if (o != null && o instanceof TupleDesc) {
            TupleDesc other = (TupleDesc) o;
            if (other.numFields() == this.numFields() && other.getSize() == this.getSize()) {
                List<TDItem> list = other.tupleDescList;
                for (int i = 0; i < numFields(); i++) {
                    if (list.get(i).fieldName == null && tupleDescList.get(i).fieldName == null) continue;
                    if (list.get(i).fieldType != tupleDescList.get(i).fieldType
                    || !list.get(i).fieldName.equals(tupleDescList.get(i).fieldName)) {
                        return false;
                    }
                }
                return true;
            }
        }
        return false;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        return tupleDescList.hashCode();
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < size; i++) {
            TDItem tdItem = tupleDescList.get(i);
            if (i != size - 1) {
                sb.append(tdItem.fieldType + "[" + i + "]" + "(" + tdItem.fieldName + "[" + i + "]),");
            } else {
                sb.append(tdItem.fieldType + "[" + i + "]" + "(" + tdItem.fieldName + "[" + i + "])");
            }
        }
        return sb.toString();
    }

}
