package simpledb;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private DbIterator child;
    private TransactionId t;
    private int tableId;

    private boolean hasFetchNextBeenCalled = false;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableid
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, DbIterator child, int tableid)
            throws DbException {
        this.t = t;
        this.child = child;
        this.tableId = tableid;

        TupleDesc tableTd = Database.getCatalog().getTupleDesc(tableid);
        if (!child.getTupleDesc().equals(tableTd)) {
            throw new DbException("TupleDesc of child differs from table");
        }
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
     * Inserts tuples read from child into the relation with the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records (even if there are 0!).
     * Insertions should be passed through BufferPool.insertTuple() with the
     * TransactionId from the constructor. An instance of BufferPool is available via
     * Database.getBufferPool(). Note that insert DOES NOT need to check to see if
     * a particular tuple is a duplicate before inserting it.
     *
     * This operator should keep track if its fetchNext() has already been called,
     * returning null if called multiple times.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (hasFetchNextBeenCalled) {
            return null;
        }
        hasFetchNextBeenCalled = true;

        int insertedCnt = 0;
        while (child.hasNext()) {
            try {
                Tuple tup = child.next();
                Database.getBufferPool().insertTuple(t, tableId, tup);
                insertedCnt++;
            } catch (java.io.IOException e) {
                throw new DbException("insertion failed because:" + e.getMessage());
            }
        }

        Tuple retTuple = new Tuple(getTupleDesc());
        retTuple.setField(0, new IntField(insertedCnt));
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
