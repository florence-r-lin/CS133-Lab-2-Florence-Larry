package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private final File heapFile;
    private TupleDesc td;
    private int numPages;
    private final int pageSize = BufferPool.getPageSize();
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.heapFile = f;
        this.td = td;
        this.numPages = (int) f.length() / pageSize;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return this.heapFile;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return this.heapFile.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) throws IllegalArgumentException {
        long offset = pid.getPageNumber() * pageSize;
        byte[] data = new byte[pageSize];
        try {
            RandomAccessFile raf = new RandomAccessFile(heapFile, "r");
            if(offset + pageSize > raf.length()){
                raf.close();
                throw new IllegalArgumentException("Page not found inside heapfile");
            }
            raf.seek(offset);
            raf.readFully(data);
            raf.close();
            return new HeapPage((HeapPageId) pid, data);
        } catch(IOException e){
            e.printStackTrace();
            throw new IllegalArgumentException("Heapfile is invalid");
        }
    }

    

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // TODO: some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
       return this.numPages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // TODO: some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
    TransactionAbortedException {
        // TODO: some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs

    private class HeapFileIterator implements DbFileIterator{
        private int currPage = -1;
        private Iterator<Tuple> currIter = null;
        private final TransactionId tid;
        private Boolean open = false;

        HeapFileIterator(TransactionId tid){
            this.tid = tid;
        }

        private Iterator<Tuple> getTuples(int currPageNo) throws TransactionAbortedException, DbException{
            if (currPageNo < 0 || currPageNo >= numPages()) {
                return null;
            }
            BufferPool my_bp = Database.getBufferPool();
            HeapPageId hpid = new HeapPageId(getId(), currPageNo);

            Page curr_page = my_bp.getPage(tid, hpid, Permissions.READ_ONLY);
            HeapPage currPage = (HeapPage) curr_page;
            return currPage.iterator();
        }

        private void nextPage() throws TransactionAbortedException, DbException{
            currIter = null;
            while (currPage+1 < numPages()){
                currPage += 1;
                currIter = getTuples(currPage);
                if(currIter != null && currIter.hasNext()){
                    return;
                }
            }
        }

        public void open() throws DbException, TransactionAbortedException {
            currIter = null;
            currPage = -1;
            open = true;
            nextPage();
        }

        public boolean hasNext() throws DbException, TransactionAbortedException{ 
            if(!open){
                throw new IllegalStateException("Error: couldn't access the iterator without opening it");
            }
            while (currIter != null && !currIter.hasNext()) {
                nextPage();
            }
            return currIter != null && currIter.hasNext();
        }

        public Tuple next()
                    throws DbException, TransactionAbortedException, NoSuchElementException {
            if(!hasNext()){
                throw new NoSuchElementException("Error, no more tuples in heapfile");
            }
            return currIter.next();
        }

        public void rewind() throws DbException, TransactionAbortedException{
            close();
            open();
        }

        public void close(){
            currPage = -1;
            currIter = null;
            open = false;
        }

    }
    
    public DbFileIterator iterator(final TransactionId tid) {
        DbFileIterator myIter = new HeapFileIterator(tid);
        return myIter;
    }

}

