package simpledb.storage;

import org.omg.CORBA.PUBLIC_MEMBER;
import simpledb.common.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;

import java.nio.file.Path;
import java.sql.PreparedStatement;
import java.time.Period;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private final HashMap<PageId, Page> pageBuffer;
    private final lruManager bufferPoolManager;
    private final LockManager lockManager;
    private final int numPages;

    private final class lruNode {
        Page page;
        lruNode prev;
        lruNode next;

        public lruNode(Page page) {
            this.page = page;
        }
    }

    private final class lruManager {
        lruNode dummyHead;
        lruNode dummyTail;
        int capacity;
        int size;
        Map<PageId, lruNode> pageIdlruNodeMap;

        public lruManager(int capacity) {
            this.capacity = capacity;
            this.size = 0;
            pageIdlruNodeMap = new ConcurrentHashMap<>();
            dummyHead = new lruNode(null);
            dummyTail = new lruNode(null);
            dummyHead.next = dummyTail;
            dummyTail.prev = dummyHead;
        }

        public void put(lruNode node) {
            PageId pageId = node.page.getId();
            pageIdlruNodeMap.put(pageId, node);
            pageBuffer.put(pageId, node.page);
            addToHead(node);
            size++;
        }

        public void delete(lruNode node) {
            removeNode(node);
            pageBuffer.remove(node.page.getId());
            pageIdlruNodeMap.remove(node.page.getId());
            size--;
        }

        public lruNode getTail() {
            return dummyTail.prev;
        }

        public void addToHead(lruNode node) {
            node.next = dummyHead.next;
            node.prev = dummyHead;
            dummyHead.next.prev = node;
            dummyHead.next = node;
        }

        public int getSize() {
            return size;
        }

        public void moveToHead(lruNode node) {
            removeNode(node);
            addToHead(node);
        }

        public void removeNode(lruNode node) {
            node.prev.next = node.next;
            node.next.prev = node.prev;
        }
    }


    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.numPages = numPages;
        pageBuffer = new HashMap<>();
        bufferPoolManager = new lruManager(numPages);
        lockManager = new LockManager();
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
        lockManager.acquireLock(tid, pid, perm);
        if (pageBuffer.containsKey(pid)) {
            lruNode lruNode = bufferPoolManager.pageIdlruNodeMap.get(pid);
            bufferPoolManager.addToHead(lruNode);
            return pageBuffer.get(pid);
        } else {
            DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
            Page page = dbFile.readPage(pid);
            if (bufferPoolManager.getSize() == numPages) {
                try {
                    evictPage();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            bufferPoolManager.put(new lruNode(page));
            return page;
        }
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        lockManager.releaseLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid)  {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lockManager.isHoldLock(tid, p, Permissions.READ_ONLY);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
        List<LockManager.pair<PageId, lockType>> lockPages = lockManager.getLockPages(tid);
        if (commit) {
            for (LockManager.pair<PageId, lockType> pair : lockPages) {
                try {
                    flushPage(pair.getFirst());

                    // use current page contents as the before-image
                    // for the next transaction that modifies this page.
                    pageBuffer.get(pair.getFirst()).setBeforeImage();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        } else {
            for (LockManager.pair<PageId, lockType> pair : lockPages) {
                discardPage(pair.getFirst());
            }
        }
        lockManager.releaseAllLock(tid);
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> dirtyPages = dbFile.insertTuple(tid, t);
        PageId pid = t.getRecordId().getPageId();
        for (Page dirtyPage : dirtyPages) {
            if (pageBuffer.containsKey(dirtyPage.getId())) {
                dirtyPage.markDirty(true, tid);
                lruNode dirtyNode = bufferPoolManager.pageIdlruNodeMap.get(dirtyPage.getId());
                bufferPoolManager.moveToHead(dirtyNode);
            } else {
                dirtyPage.markDirty(true, tid);
                if (bufferPoolManager.getSize() == numPages) { evictPage(); }
                bufferPoolManager.put(new lruNode(dirtyPage));
            }
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile dbFile = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        if (dbFile == null) return;
        List<Page> dirtyPages = dbFile.deleteTuple(tid, t);
        for (Page dirtyPage : dirtyPages) {
            if (pageBuffer.containsKey(dirtyPage.getId())) {
                dirtyPage.markDirty(true, tid);
                lruNode dirtyNode = bufferPoolManager.pageIdlruNodeMap.get(dirtyPage.getId());
                bufferPoolManager.moveToHead(dirtyNode);
            } else {
                dirtyPage.markDirty(true, tid);
                if (bufferPoolManager.getSize() == numPages) { evictPage(); }
                bufferPoolManager.put(new lruNode(dirtyPage));
            }
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for (Map.Entry<PageId, Page> entry : pageBuffer.entrySet()) {
            Page page = entry.getValue();
            flushPage(page.getId());
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        if (pageBuffer.containsKey(pid)) {
            lruNode deleteNode = bufferPoolManager.pageIdlruNodeMap.get(pid);
            bufferPoolManager.delete(deleteNode);
        }
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        Page page = pageBuffer.get(pid);
        if (page == null) return;
        if (page.isDirty() == null) return;
        DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());

        // append an update record to the log, with
        // a before-image and after-image.
        TransactionId dirtier = page.isDirty();
        if (dirtier != null){
            Database.getLogFile().logWrite(dirtier, page.getBeforeImage(), page);
            Database.getLogFile().force();
        }

        dbFile.writePage(page);
        page.markDirty(false, null);
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException, IOException {
        // some code goes here
        // not necessary for lab1

        // The no steal policy means that not evicts dirty page
        Page evictPage = getNotDirtyPage();
        flushPage(evictPage.getId());
        discardPage(evictPage.getId());
    }

    private Page getNotDirtyPage() throws DbException {
        lruNode curNode = bufferPoolManager.getTail();
        while (curNode.page.isDirty() != null) {
            curNode = curNode.prev;
            if (curNode.equals(bufferPoolManager.dummyHead)) throw new DbException("all pages are dirty");
        }
        return curNode.page;
    }

}
