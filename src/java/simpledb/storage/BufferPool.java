package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

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
    /**
     * Bytes per page, including header.
     */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    /**
     * Default number of pages passed to the constructor. This is used by
     * other classes. BufferPool should use the numPages argument to the
     * constructor instead.
     */
    public static final int DEFAULT_PAGES = 50;

    private final Random random = new Random();
    final int numPages;   // number of pages -- currently, not enforced
    final ConcurrentMap<PageId, Page> pages; // hash table storing current pages in memory
    final LockManager lm;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        this.numPages = numPages;
        this.pages = new ConcurrentHashMap<>();
        this.lm = new LockManager();
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
     * @param tid  the ID of the transaction requesting the page
     * @param pid  the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        // block until we get a lock on this page
        try {
            //System.out.println("bp acquiring for " + tid + " on " + pid);
            lm.acquireLock(tid, pid, perm);
            // Note (rytaft): I commented the below line out and replaced it with the line
            // above because it doesn't make any sense.
            // If someone put this here for a reason please let me know...
            //lm.acquireLock(tid, new HeapPageId(pid.getTableId(), pid.getPageNumber()), perm);
        } catch (DeadlockException de) {
            //System.out.println("bp found deadlock for " + tid + " on " + pid);
            // XXX SRM -- why is this necessary???
            lm.releaseAllLocks(tid, false);
            throw new TransactionAbortedException();
        }
        // XXX Yuan points out that HashMap is not synchronized, so this is buggy.
        // XXX TODO(ghuo): do we really know enough to implement NO STEAL here?
        //     won't we still evict pages?
        Page p;
        synchronized (this) {
            p = pages.get(pid);
            if (p == null) {
                if (pages.size() >= numPages) {
                    evictPage();
                }

                p = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
                pages.put(pid, p);
            }
        }

        return p;
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
    public void unsafeReleasePage(TransactionId tid, PageId pid) {
        lm.releaseLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // XXX flush pages
        transactionComplete(tid, true);
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(TransactionId tid, PageId p) {
        return lm.holdsLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        lm.releaseAllLocks(tid, commit);

        Set<PageId> pageIds = lm.pagesLockedByTid(tid);
        if (pageIds == null) return;
        for (PageId p : pageIds) {
            if (pages.containsKey(p))
                pages.get(p).setBeforeImage();
        }
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid     the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t       the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);

        // let the specific implementation of the file decide which page to add it
        // to.

        List<Page> dirtypages = file.insertTuple(tid, t);

        synchronized (this) {
            for (Page p : dirtypages) {
                p.markDirty(true, tid);

                //System.out.println("ADDING TUPLE TO PAGE " + p.getId().pageno() + " WITH HASH CODE " + p.getId().hashCode());

                // if page in pool already, done.
                if (pages.get(p.getId()) != null) {
                    //replace old page with new one in case addTuple returns a new copy of the page
                    pages.put(p.getId(), p);
                } else {

                    // put page in pool
                    if (pages.size() >= numPages)
                        evictPage();
                    pages.put(p.getId(), p);
                }
            }
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t   the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        DbFile file = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        List<Page> dirtypages = file.deleteTuple(tid, t);

        synchronized (this) {
            for (Page p : dirtypages) {
                p.markDirty(true, tid);

                // if page in pool already, done.
                if (pages.get(p.getId()) != null) {
                    //replace old page with new one in case deleteTuple returns a new copy of the page
                    pages.put(p.getId(), p);
                } else {

                    // put page in pool
                    if (pages.size() >= numPages)
                        evictPage();
                    pages.put(p.getId(), p);
                }
            }
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        /* calls flushPage() for each page in the BufferPool */
        for (PageId pageId : pages.keySet()) flushPage(pageId);

    }

    /**
     * Remove the specific page id from the buffer pool.
     * Needed by the recovery manager to ensure that the
     * buffer pool doesn't keep a rolled back page in its
     * cache.
     * <p>
     * Also used by B+ tree files to ensure that deleted pages
     * are removed from the cache so they can be reused safely
     */
    public synchronized void removePage(PageId pid) {
        Page p = pages.get(pid);
        if (p != null) {
            pages.remove(pid);
        }
    }

    /**
     * Flushes a certain page to disk
     *
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {

        Page p = pages.get(pid);
        if (p == null)
            return; //not in buffer pool -- doesn't need to be flushed

        DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
        file.writePage(p);
        p.markDirty(false, null);
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        Set<PageId> pageId = lm.pagesLockedByTid(tid);
        if (pageId == null) return;

        for (PageId p : pageId) {
            //System.out.println("FLUSHING PAGE " + p.pageno());
            flushPage(p);
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // pick a random page and flush it.
        // XXX this will work for lab1, but not for lab4.
        // XXX this can cause pages to be evicted that have uncommitted updates on them
        Object[] pids = pages.keySet().toArray();
        PageId pid = (PageId) pids[random.nextInt(pids.length)];
        try {
            Page p = pages.get(pid);
            if (p.isDirty() != null) { //if this is dirty, remove first non-dirty
                boolean gotNew = false;
                for (PageId pg : pages.keySet()) {
                    if (pages.get(pg).isDirty() == null) {
                        pid = pg;
                        gotNew = true;
                        break;
                    }
                }
                if (!gotNew) {
                    throw new DbException("All buffer pool slots contain dirty pages;  COMMIT or ROLLBACK to continue.");
                }
            }
            //XXX: The above code makes sure page is not dirty. 
            //Assuming we have FORCE, Why do we flush it to disk?
            //Answer: yes we don't need this if we have FORCE, but we do need it if we don't.
            //it doesn't hurt to keep it here.            
            flushPage(pid);
        } catch (IOException e) {
            throw new DbException("could not evict page");
        }
        pages.remove(pid);
    }

    /**
     * Manages locks.
     * <p>
     * All the field read/write operations are protected by this
     *
     * @Threadsafe
     */
    class LockManager {

        final int LOCK_WAIT = 10;       // ms
        final Random random = new Random();

        // maps TransactionId to a Set of Pages
        final Map<TransactionId, Set<PageId>> _tid2pages;

        // set of pages that are locked
        final Map<PageId, Set<TransactionId>> _page2tids;

        // permission under which page got locked
        final Map<PageId, Permissions> _page2perm;

        final Map<TransactionId, List<TransactionId>> _waitsFor;

        private LockManager() {
            _tid2pages = new HashMap<>();
            _page2tids = new HashMap<>();
            _page2perm = new HashMap<>();
            _waitsFor = new HashMap<>();

        }

        //methods to check waits for graph
        public synchronized boolean checkWaitsFor(TransactionId origin, List<TransactionId> goal) {

            List<TransactionId> start = _waitsFor.get(origin);
            if (start == null) return false;
            for (TransactionId t : start) {
                List<TransactionId> newGoal = new ArrayList<>(goal);
                newGoal.add(t);
                if (goal.contains(t) || checkWaitsFor(t, newGoal)) {
                    return true;
                }
            }
            return false;
        }

        public synchronized boolean checkWaitsForDeadlock(TransactionId tid) {
            return checkWaitsFor(tid, Collections.singletonList(tid));
        }

        /**
         * Tried to acquire a lock on page pid for transaction tid, with
         * permissions perm.
         *
         * @throws DeadlockException after on cycle-based deadlock
         */
        public void acquireLock(TransactionId tid, PageId pid, Permissions perm)
                throws DeadlockException {

            while (!lock(tid, pid, perm)) {

                synchronized (this) {
                    List<TransactionId> v = _waitsFor.computeIfAbsent(tid, k -> new ArrayList<>());
                    Set<TransactionId> holder = _page2tids.get(pid);
                    if (holder != null) {
                        holder = new HashSet<>(holder);
                        holder.remove(tid);
                        v.addAll(holder);
                        _waitsFor.put(tid, v);
                        if (checkWaitsForDeadlock(tid)) {
                            //System.out.println("Cycle based deadlock in " + _waitsFor);
                            _waitsFor.remove(tid);
                            throw new DeadlockException();
                        }
                    }
                }

                try {
                    Thread.sleep(LOCK_WAIT);
                } catch (InterruptedException ignored) {
                }

            }

            synchronized (this) {
                _waitsFor.remove(tid);
            }

        }

        /**
         * Release all locks corresponding to TransactionId tid.
         */
        public synchronized void releaseAllLocks(TransactionId tid, boolean commit) {
            Set<PageId> s = _tid2pages.get(tid);
            if (s == null)
                return;

            Set<PageId> sx = new HashSet<>(s);
            for (PageId pid : sx) {

                //System.out.println("RELEASING LOCK ON PAGE " + pid.pageno());
                if (commit) {
                    // if we commit, then we flush the page to disk under the FORCE policy
                    try {
                        // System.out.println("bp flushing page " + pid.pageno());
                        Page p = pages.get(pid);
                        if (p != null) {
                            //not necessary to write it if its not dirty
                            //p.markDirty(true, tid);
                            //really, pages aught to be have been flushed already before we do this!
                            // No, we flush pages here in early labs with FORCE buffer pool policy.
                            // We get rid of these flushes when we switch to NO FORCE in the recovery lab
                            flushPage(pid);
                            p.setBeforeImage(); // next abort should only roll back to here
                        }
                    } catch (IOException e) {
                        throw new RuntimeException("failed during commit: " + e);
                    }
                } else if ((_page2perm.get(pid)).equals(Permissions.READ_WRITE)) {
                    // if we abort, then we restore the page from disk iff we had a
                    //   write lock.
                    // System.out.println("bp reloading page " + pid.pageno());
                    // better have done rollback first if we are allowing dirty pages to go to disk
                    Page p = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
                    pages.put(pid, p);
                }

                releaseLock(tid, pid);
            }
        }

        public synchronized Set<PageId> pagesLockedByTid(TransactionId tid) {
            return _tid2pages.get(tid);
        }

        /**
         * Return true if the specified transaction has a lock on the specified page
         */
        public synchronized boolean holdsLock(TransactionId tid, PageId p) {
            Set<TransactionId> tset = _page2tids.get(p);
            if (tset == null) return false;
            return tset.contains(tid);
        }

        /**
         * Returns whether or not this tid/pid/perm is already locked by another object.
         * if nobody is holding a lock, then this tid/pid/perm can acquire the lock.
         * <p>
         * if perm == READ
         * if tid is holding any sort of lock on pid, then the tid can acquire the lock (return false).
         * <p>
         * if another tid is holding a READ lock on pid, then the tid can acquire the lock (return false).
         * if another tid is holding a WRITE lock on pid, then tid can not currently
         * acquire the lock (return true).
         * <p>
         * else
         * if tid is THE ONLY ONE holding a READ lock on pid, then tid can acquire the lock (return false).
         * if tid is holding a WRITE lock on pid, then the tid already has the lock (return false).
         * <p>
         * if another tid is holding any sort of lock on pid, then the tid can not currenty acquire the lock (return true).
         */
        private boolean locked(TransactionId tid, PageId pid, Permissions perm) {
            Set<PageId> pset = null;
            Set<TransactionId> tset = null;
            Permissions p = null;

            synchronized (this) {
                pset = _tid2pages.get(tid);
                tset = _page2tids.get(pid);
                p = _page2perm.get(pid);
            }

            // System.out.println("locked(): pid = " + pid);

            // if nobody is holding a lock on this page, duh, grant lock
            if (p == null)
                return false;

            // if perm == READ
            if (perm == Permissions.READ_ONLY) {
                // if tid is holding any sort of lock on pid, duh, grant lock
                if (pset != null && pset.contains(pid))
                    return false;

                // if another tid is holding a READ lock on pid, grant lock
                if (p == Permissions.READ_ONLY)
                    return false;

                //otherwise, someone else has the lock, so we can't have it
                if (p == Permissions.READ_WRITE)
                    return true;

                System.out.println("locked weirdness, 1");

                // if perm == READ_WRITE
            } else {
                // if tid is THE ONLY ONE holding a READ lock on pid, this is allowed but we will need to
                // change lock to WRITE later
                if (p == Permissions.READ_ONLY && tset.contains(tid) && tset.size() == 1)
                    return false;

                // if tid is holding a WRITE lock on pid, duh, grant lock
                if (p == Permissions.READ_WRITE && tset.contains(tid))
                    return false;

                // if another tid is holding any sort of lock on pid, sleep, goto start
                if (tset.size() != 0)
                    return true;

                System.out.println("locked weirdness, 2");
            }
            System.exit(-1);

            System.out.println("locked weirdness, 3");
            System.exit(-1);
            return true;
        }

        public synchronized void releaseLock(TransactionId tid, PageId pid) {
            //System.out.println("released " + _page2perm.get(pid) + " on " + pid.pageno() + " for " + tid);

            Set<PageId> pset = _tid2pages.get(tid);
            if (pset != null) {
                pset.remove(pid);
                if (pset.isEmpty())
                    _tid2pages.remove(tid);
            }

            Set<TransactionId> tset = _page2tids.get(pid);
            if (tset != null) {
                tset.remove(tid);
                if (tset.isEmpty()) {
                    _page2tids.remove(pid);
                    _page2perm.remove(pid);
                }
            }
        }

        private synchronized boolean lock(TransactionId tid, PageId pid, Permissions perm) {

            if (locked(tid, pid, perm))
                return false;

            Set<PageId> pset = _tid2pages.computeIfAbsent(tid, k -> new HashSet<>());

            pset.add(pid);
            /*
              System.out.print(tid + " locks: ");
              while (it.hasNext()) {
              PageId p = it.next();

              System.out.print(p.pageno() + ",");
              }
              System.out.println("");
            */

            Set<TransactionId> tset = _page2tids.computeIfAbsent(pid, k -> new HashSet<>());
            // System.out.println("putting " + tid + " -> " + pid.pageno() + " in _page2tids");
            tset.add(tid);

            //System.out.println("putting " + pid + " -> " + perm + " in _page2perm");
            Permissions old = _page2perm.get(pid);
            if (old == null || (old == Permissions.READ_ONLY && perm == Permissions.READ_WRITE)) {
                _page2perm.put(pid, perm);
            }
            return true;
        }
    }
}
