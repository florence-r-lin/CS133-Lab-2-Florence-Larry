package simpledb;

import java.util.*;


/**
 * Computes some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private final HashMap<Field, Stat> groups;



    private class Stat {
        private int MIN, MAX, SUM, COUNT;

        public Stat() {
            MIN = Integer.MAX_VALUE;
            MAX = Integer.MIN_VALUE;
            SUM = 0;
            COUNT = 0;
        }

        public void merge(int value) {
            if (value < MIN) MIN = value;
            if (value > MAX) MAX = value;
            SUM += value;
            COUNT++;
        }
    }
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
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.groups = new HashMap<>();
        if (gbfield == NO_GROUPING) {
            groups.put(null, new Stat());
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor. See Aggregator.java for more.
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        if (gbfield == NO_GROUPING) {
            groups.get(null).merge(((IntField) tup.getField(afield)).getValue());
            return;
        }

        Field key = tup.getField(gbfield);
        if(!groups.containsKey(key)){
            groups.put(key, new Stat());
            groups.get(key).merge(((IntField) tup.getField(afield)).getValue());
        } else{
            groups.get(key).merge(((IntField) tup.getField(afield)).getValue());
        }
    }

    private int getStat(Stat stat) {
        switch (what) {
            case MIN:
                return stat.MIN;
            case MAX:
                return stat.MAX;
            case SUM:
                return stat.SUM;
            case COUNT:
                return stat.COUNT;
            case AVG:
                return stat.SUM / stat.COUNT;
            default:
                throw new IllegalStateException("Invalid operation");
        }
    }
    /**
     * Returns a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        List<Tuple> tuples = new ArrayList<>();

        TupleDesc td;
        if (gbfield == NO_GROUPING) {
            td = new TupleDesc(new Type[] {Type.INT_TYPE});
        } else{
            td = new TupleDesc(new Type[] {gbfieldtype, Type.INT_TYPE});
        }
        
        for (Map.Entry<Field, Stat> entry : groups.entrySet()) {
            Tuple tuple = new Tuple(td);
            if (gbfield == NO_GROUPING) {
                tuple.setField(0, new IntField(getStat(entry.getValue())));
            } else{
                tuple.setField(0, entry.getKey());
                tuple.setField(1, new IntField(getStat(entry.getValue())));
            }
            tuples.add(tuple);
        }

        return new TupleIterator(td, tuples);
    }

}
