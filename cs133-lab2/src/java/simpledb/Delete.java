package simpledb;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private DbIterator child;
    private TransactionId t;
   

    private boolean hasFetchNextBeenCalled = false;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     *
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        this.t = t;
        this.child = child;
    }

    public TupleDesc getTupleDesc() {
        return new TupleDesc(new Type[]{Type.INT_TYPE});
    }

    public void open() throws DbException, TransactionAbortedException {
        super.open();
        child.open();
    }

    public void close() {
        child.close();
        super.close();
    }

    /**
     * You can just close and then open the child
     */
    public void rewind() throws DbException, TransactionAbortedException {
        child.close();
        child.open();
        hasFetchNextBeenCalled = false;
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method. You can pass along the TransactionId from the constructor.
     * This operator should keep track of whether its fetchNext() method has been called already.
     *
     * @return A 1-field tuple containing the number of deleted records (even if there are 0)
     *          or null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (hasFetchNextBeenCalled) {
            return null;
        }
        hasFetchNextBeenCalled = true;

        int deletedCnt= 0;
        while (child.hasNext()) {
            try {
                Tuple tup = child.next();
                Database.getBufferPool().deleteTuple(t, tup);
                deletedCnt++;
            } catch (java.io.IOException e) {
                throw new DbException("deletion operation failed because " + e.getMessage());
            }
        }

        Tuple retTuple = new Tuple(getTupleDesc());
        retTuple.setField(0, new IntField(deletedCnt));
        return retTuple;
    }

    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[]{child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        this.child = children[0];
    }

}
