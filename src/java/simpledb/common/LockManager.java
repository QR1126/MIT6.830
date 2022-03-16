package simpledb.common;

import simpledb.storage.PageId;
import simpledb.transaction.TransactionId;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class LockManager {

    public static final class lock {
        private lockType type;
        private final List<TransactionId> holders;

        public lock() {
            this.type = lockType.shared;
            this.holders = new ArrayList<>();
        }
    }

    private final Map<TransactionId, List<PageId>> tidToLockedPages;
    private final Map<PageId, lock> pidToLock;

    public LockManager() {
        this.tidToLockedPages = new ConcurrentHashMap<>();
        this.pidToLock = new ConcurrentHashMap<>();
    }

    public void acquireLock(TransactionId tid, PageId pid, Permissions perm) {
        if (isHoldLock(tid, pid, perm)) return;
        if (perm.equals(Permissions.READ_ONLY)) acquireSLock(tid, pid, perm);
        else if (perm.equals(Permissions.READ_WRITE)) acquireXLock(tid, pid, perm);
    }

    public synchronized lock getLock(PageId pid) {
        if (pidToLock.containsKey(pid)) return pidToLock.get(pid);
        else {
            lock lock = new lock();
            pidToLock.put(pid, lock);
            return lock;
        }
    }

    private void addLock(TransactionId tid, PageId pid) {
        tidToLockedPages.putIfAbsent(tid, new ArrayList<>());
        List<PageId> pageIds = tidToLockedPages.get(tid);
        if (!pageIds.contains(pid)) pageIds.add(pid);
    }

    public void acquireSLock(TransactionId tid, PageId pid, Permissions perm) {
        lock lock = getLock(pid);
        while (true) {
            synchronized (lock) {
                if (lock.holders.isEmpty()) {
                    lock.type = lockType.shared;
                    addLock(tid, pid);
                    lock.holders.add(tid);
                    break;
                } else if (lock.type == lockType.shared || lock.type == lockType.exclusive) {
                    lock.holders.add(tid);
                    addLock(tid, pid);
                    break;
                }
                else {

                }
            }
        }
    }

    public void acquireXLock(TransactionId tid, PageId pid, Permissions perm) {
        lock lock = getLock(pid);
        while (true) {
            synchronized (lock) {
                if (lock.holders.isEmpty()) {
                    lock.type = lockType.exclusive;
                    addLock(tid, pid);
                    lock.holders.add(tid);
                    break;
                } else if (lock.holders.size() == 1 && lock.holders.get(0) == tid) {
                    lock.type = lockType.exclusive;
                    break;
                } else {

                }
            }
        }
    }

    public void releaseLock(TransactionId tid, PageId pid) {
        if (isHoldLock(tid, pid, Permissions.READ_ONLY) || isHoldLock(tid, pid, Permissions.READ_WRITE)) {
            lock lock = getLock(pid);
            synchronized (lock) {
                List<PageId> pageIds = tidToLockedPages.get(tid);
                pageIds.remove(pid);
                lock.holders.remove(tid);
            }
        }
    }

    public synchronized void releaseAllLock(TransactionId tid) {
        List<pair<PageId, lockType>> lockPages = getLockPages(tid);
        for (pair<PageId, lockType> pair : lockPages) {
            PageId pageId = pair.getFirst();
            releaseLock(tid, pageId);
        }
    }

    public List<pair<PageId, lockType>> getLockPages(TransactionId tid) {
        List<PageId> pageIds = tidToLockedPages.get(tid);
        List<pair<PageId, lockType>> list = new ArrayList<>();
        if (pageIds == null) return list;
        for (PageId pageId : pageIds) {
            lock lock = getLock(pageId);
            list.add(new pair<PageId, lockType>(pageId, lock.type));
        }
        return list;
    }

    public final static class pair<T1,T2> {
        private T1 first;
        private T2 second;

        public pair(T1 first, T2 second) {
            this.first = first;
            this.second = second;
        }

        public T1 getFirst() {
            return first;
        }

        public T2 getSecond() {
            return second;
        }
    }

    public synchronized boolean isHoldLock(TransactionId tid, PageId pid, Permissions perm) {
        if (!pidToLock.containsKey(pid)) return false;
        else {
            lock lock = pidToLock.get(pid);
            lockType lockType = lock.type;
            if (!lock.holders.contains(tid)) return false;
            if ((perm.equals(Permissions.READ_ONLY) && lockType.equals(simpledb.common.lockType.exclusive))
                || (perm.equals(Permissions.READ_WRITE) && lockType.equals(simpledb.common.lockType.shared))) {
                return false;
            }
            return true;
        }
    }
}
