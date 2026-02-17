package simpledb;

/** Unique identifier for HeapPage objects. */
public class HeapPageId implements PageId {

    private final int pgNo;
    private final int tableId;
    
    /**
     * Constructor. Create a page id structure for a specific page of a
     * specific table.
     *
     * @param tableId The table that is being referenced
     * @param pgNo The page number in that table.
     */
    public HeapPageId(int tableId, int pgNo) {
        this.tableId = tableId;
        this.pgNo = pgNo;
    }

    /** @return the table associated with this PageId */
    public int getTableId() {
        return this.tableId;
    }

    /**
     * @return the page number in the table getTableId() associated with
     *   this PageId
     */
    public int getPageNumber() {
        return this.pgNo;
    }

    /**
     * @return a hash code for this page, represented by the concatenation of
     *   the table number and the page number (needed if a PageId is used as a
     *   key in a hash table in the BufferPool, for example.). You may want to 
     *   parse the concatenation as a long (Long.parseLong()) before casting to int.
     *   Note that it's possible for both the table id and the page number to be negative 
     *   (for testing), so you may need to modify those values before concatenating.
     * @see BufferPool
     */
    public int hashCode() {
        int absTableId = Math.abs(tableId);
        int abspgNo = Math.abs(pgNo);
        String hash = Integer.toString(absTableId) + Integer.toString(abspgNo);
        long hashLong = Long.parseLong(hash);
        
        return (int) hashLong;
    }

    /**
     * Compares one PageId to another.
     *
     * @param o The object to compare against (must be a PageId)
     * @return true if the objects are equal (e.g., page numbers and table
     *   ids are the same)
     */
    public boolean equals(Object o) {
        if (!(o instanceof PageId)){
            return false;
        }
        PageId o_pID = (PageId) o;
        return (this.pgNo == o_pID.getPageNumber()) && (this.tableId == o_pID.getTableId());
    }

    /**
     *  Return a representation of this object as an array of
     *  integers, for writing to disk.  Size of returned array must contain
     *  number of integers that corresponds to number of args to one of the
     *  constructors.
     */
    public int[] serialize() {
        int data[] = new int[2];

        data[0] = getTableId();
        data[1] = getPageNumber();

        return data;
    }

}
