package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    private final Predicate p;

    private DbIterator child;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, DbIterator child) {
        this.p = p;
        this.child = child;
    }

    public Predicate getPredicate() {
        return this.p;
    }

    public TupleDesc getTupleDesc() {
        return this.child.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
    TransactionAbortedException {
        super.open();
        this.child.open();
    }

    public void close() {
        super.close();
        this.child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        this.child.rewind();
    }

    /**
     * The Filter operator iterates through the tuples from its child, 
     * applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * This method returns the next tuple.
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
    TransactionAbortedException, DbException {
        while (child.hasNext()){
            Tuple t = child.next();
            if( p.filter(t)) {
                return t;
            }
        }
        return null;
    }

    /**
     * See Operator.java for additional notes 
     */
    @Override
    public DbIterator[] getChildren() {
        DbIterator[] children = {child};
        return children;
    }

    /**
     * See Operator.java for additional notes 
     */
    @Override
    public void setChildren(DbIterator[] children) {
        this.child = children[0];
    }

}
