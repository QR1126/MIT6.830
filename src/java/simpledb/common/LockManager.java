package simpledb.common;

import simpledb.storage.PageId;
import simpledb.transaction.TransactionId;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class LockManager {

    private static final class lock {
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

    public synchronized void acquireLock(TransactionId tid, PageId pid, Permissions perm) {
        if (isHoldLock(tid, pid, perm)) return;
        if (perm.equals(Permissions.READ_ONLY)) acquireSLock(tid, pid, perm);
        else if (perm.equals(Permissions.READ_WRITE)) acquireXLock(tid, pid, perm);
    }

    public synchronized void acquireSLock(TransactionId tid, PageId pid, Permissions perm) {

    }

    public synchronized void acquireXLock(TransactionId tid, PageId pid, Permissions perm) {

    }

    public synchronized void releaseLock(TransactionId tid, PageId pid) {

    }

    public synchronized void releaseAllLock(TransactionId tid) {

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
