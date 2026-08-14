/* SPDX-License-Identifier: GPL-2.0 */
#ifndef __XFS_WICACHE_H__
#define __XFS_WICACHE_H__

#define USE_WICACHE

#include <linux/atomic.h>
#include <linux/gfp_types.h>
#include <linux/list.h>
#include <linux/mutex.h>
#include <linux/rcupdate.h>
#include <linux/refcount.h>
#include <linux/rhashtable.h>
#include <linux/rbtree.h>
#include <linux/rwsem.h>
#include <linux/spinlock.h>
#include <linux/types.h>
#include <linux/wait.h>
#include <linux/workqueue.h>
#include <linux/xarray.h>

struct file;
struct folio;
struct iov_iter;
struct kiocb;
struct xfs_inode;

#define XFS_WICACHE_NR_SHARDS		64
#define XFS_WICACHE_SHARD_MASK		(XFS_WICACHE_NR_SHARDS - 1)
#define XFS_WICACHE_NR_ADMISSION_LOCKS	64
#define XFS_WICACHE_ADMISSION_MASK	(XFS_WICACHE_NR_ADMISSION_LOCKS - 1)
#define XFS_WICACHE_SEG_SHIFT		9
#define XFS_WICACHE_SEG_SIZE		(1U << XFS_WICACHE_SEG_SHIFT)
#define XFS_WICACHE_NR_SEGS		(PAGE_SIZE / XFS_WICACHE_SEG_SIZE)
#define XFS_WICACHE_FULL_MASK		((1UL << XFS_WICACHE_NR_SEGS) - 1)
#define XFS_WICACHE_ACCOUNT_GFP(gfp)	((gfp) | __GFP_ACCOUNT)

enum xfs_wicache_entry_state {
	XFS_WICACHE_ENTRY_DIRTY = 0,
	XFS_WICACHE_ENTRY_FLUSHING,
	XFS_WICACHE_ENTRY_INVALID,
};

enum xfs_wicache_inode_state {
	XFS_WICACHE_INODE_ACTIVE = 0,
	XFS_WICACHE_INODE_DYING,
};

struct xfs_wicache_mount;
struct xfs_wicache_inode;
struct xfs_wicache_batch;

struct xfs_wicache_entry {
	struct xfs_wicache_inode	*wi;
	struct file			*io_file;
	pgoff_t			page_index;
	u8				*active[XFS_WICACHE_NR_SEGS];
	u8				*flushing[XFS_WICACHE_NR_SEGS];
	unsigned long			active_mask;
	unsigned long			flushing_mask;
	struct folio			*transient_base;
	struct folio			*prepared_folio;
	bool				prepared_charged;
	struct xfs_wicache_batch	*batch;
	int				prepare_error;
	u64				seq;
	enum xfs_wicache_entry_state	state;
	bool				queued;
	bool				on_dirty_tree;
	u64				prepare_queued_ns;

	refcount_t			refcount;
	struct mutex			lock;
	struct rb_node			dirty_node;
	struct work_struct		prepare_work;
	struct rcu_head			rcu;
};

struct xfs_wicache_shard {
	struct xarray		entries;
};

struct xfs_wicache_inode {
	struct xfs_wicache_mount	*wm;
	struct xfs_inode		*ip;
	struct rhash_head		hash_node;
	struct list_head		mount_node;

	struct xfs_wicache_shard	shards[XFS_WICACHE_NR_SHARDS];
	atomic64_t			dirty_bytes;
	atomic64_t			nr_entries;
	atomic64_t			seq;
	atomic_t			batch_active;
	enum xfs_wicache_inode_state	state;

	struct rw_semaphore		visibility_sem;
	spinlock_t			dirty_lock;
	struct rb_root_cached		dirty_tree;
	struct delayed_work		flush_work;
	refcount_t			refcount;
	struct rcu_head			rcu;
};

struct xfs_wicache_mount {
	bool				enabled;
	struct rhashtable		inode_table;
	struct mutex			inode_lock;
	struct mutex			admission_locks[XFS_WICACHE_NR_ADMISSION_LOCKS];
	struct list_head		inodes;
	struct workqueue_struct		*control_wq;
	struct workqueue_struct		*io_wq;
	atomic64_t			total_dirty_bytes;
	wait_queue_head_t		dirty_wait;
};

struct xfs_wicache_mount *xfs_wicache_mount_alloc(gfp_t gfp);
void xfs_wicache_mount_free(struct xfs_wicache_mount *wm);

struct xfs_wicache_inode *xfs_wicache_inode_lookup(
		struct xfs_wicache_mount *wm, struct xfs_inode *ip);
struct xfs_wicache_inode *xfs_wicache_inode_get_or_create(
		struct xfs_wicache_mount *wm, struct xfs_inode *ip, gfp_t gfp);
void xfs_wicache_inode_put(struct xfs_wicache_inode *wi);
struct mutex *xfs_wicache_admission_lock(struct xfs_wicache_mount *wm,
		struct xfs_inode *ip);

bool xfs_wicache_can_stage(struct kiocb *iocb, struct iov_iter *from);
ssize_t xfs_wicache_stage_iter(struct xfs_wicache_inode *wi,
		struct file *file, struct iov_iter *from, loff_t pos,
		size_t count);
int xfs_wicache_overlay_iter(struct xfs_wicache_inode *wi,
		struct iov_iter *to, loff_t pos, size_t count);
bool xfs_wicache_inode_has_dirty(struct xfs_wicache_inode *wi);
int xfs_wicache_inode_drain(struct xfs_wicache_inode *wi);
bool xfs_wicache_range_has_entry(struct xfs_wicache_inode *wi,
		loff_t pos, size_t count);
void xfs_wicache_read_lock(struct xfs_wicache_inode *wi);
void xfs_wicache_read_unlock(struct xfs_wicache_inode *wi);
void xfs_wicache_record_front_iolock(u64 ns);
void xfs_wicache_record_mapping_check(u64 ns);

ssize_t xfs_wicache_dio_read_folio(struct file *file, loff_t pos,
		struct folio *folio);
ssize_t xfs_wicache_dio_write_folio(struct file *file, loff_t pos,
		struct folio *folio);
ssize_t xfs_wicache_dio_read_folios(struct file *file, loff_t pos,
		struct folio **folios, unsigned int nr);
ssize_t xfs_wicache_dio_write_folios(struct file *file, loff_t pos,
		struct folio **folios, unsigned int nr);

#endif /* __XFS_WICACHE_H__ */
