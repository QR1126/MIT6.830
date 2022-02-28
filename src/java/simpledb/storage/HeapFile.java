package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
    final private File file;
    final private TupleDesc tupleDesc;
    final private int fileId;
    final private Lock deleteLock = new ReentrantLock(false);
    final private Lock insertLock = new ReentrantLock(false);
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        file = f;
        tupleDesc = td;
        fileId = file.getAbsolutePath().hashCode();
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
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
        // some code goes here
        return this.fileId;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        long offset = pid.getPageNumber() * BufferPool.getPageSize();
        byte[] data = new byte[BufferPool.getPageSize()];
        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
            randomAccessFile.seek(offset);
            for (int i = 0; i < BufferPool.getPageSize(); i++) {
                data[i] = (byte) randomAccessFile.read();
            }
            HeapPage heapPage = new HeapPage((HeapPageId) pid, data);
            randomAccessFile.close();
            return heapPage;
        } catch (FileNotFoundException exception) {
            exception.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        int offset = page.getId().getPageNumber();
        byte[] pageData = page.getPageData();
        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
        randomAccessFile.seek(offset);
        randomAccessFile.write(pageData, 0, BufferPool.getPageSize());
        randomAccessFile.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) Math.floor(file.length() * 1.0 / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        for (int i = 0; i < this.numPages(); i++) {
            HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), i), Permissions.READ_WRITE);
            if (heapPage.getNumEmptySlots() == 0) continue;
            heapPage.insertTuple(t);
            return new ArrayList<Page>(){{add(heapPage);}};
        }
        HeapPage heapPage = new HeapPage(new HeapPageId(getId(), numPages()), HeapPage.createEmptyPageData());
        heapPage.insertTuple(t);
        this.writePage(heapPage);
        return new ArrayList<>();
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        HeapPageId heapPageId = (HeapPageId) t.getRecordId().getPageId();
        if (heapPageId.getTableId() != getId()) throw new DbException("the tuple cannot be deleted or is not a member of the file");
        HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, heapPageId, Permissions.READ_WRITE);
        return new ArrayList<Page>(){{add(heapPage);}};
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new heapFileIterator(tid, this);
    }

    private class heapFileIterator implements DbFileIterator {

        private TransactionId tid;
        private Iterator<Tuple> it;
        private HeapFile heapFile;
        private int numberOfPage;

        public heapFileIterator(TransactionId tid, HeapFile heapFile) {
            this.tid = tid;
            this.heapFile = heapFile;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            numberOfPage = 0;
            it = getIt(numberOfPage);
        }

        private Iterator<Tuple> getIt(int numberOfPage) throws TransactionAbortedException, DbException {
            if (numberOfPage >= 0 && numberOfPage < heapFile.numPages()) {
                HeapPageId heapPageId = new HeapPageId(heapFile.getId(), numberOfPage);
                HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, heapPageId, Permissions.READ_ONLY);
                return heapPage.iterator();
            }
            else {
                throw new NoSuchElementException();
            }
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (it == null) {
                return false;
            }
            if (!it.hasNext()) {
                if (numberOfPage < this.numberOfPage) {
                    it = getIt(++numberOfPage);
                    return it.hasNext();
                }
                else {
                    return false;
                }
            }
            else {
                return true;
            }
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (it == null || !it.hasNext()) {
                throw new NoSuchElementException();
            }
            return it.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public void close() {
            it = null;
        }
    }

}

