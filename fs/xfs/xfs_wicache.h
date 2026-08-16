/* SPDX-License-Identifier: GPL-2.0 */
#ifndef __XFS_WICACHE_H__
#define __XFS_WICACHE_H__

#define USE_WICACHE

#include <linux/atomic.h>
#include <linux/bitmap.h>
#include <linux/gfp_types.h>
#include <linux/list.h>
#include <linux/mutex.h>
#include <linux/rcupdate.h>
#include <linux/refcount.h>
#include <linux/rwsem.h>
#include <linux/spinlock.h>
#include <linux/types.h>
#include <linux/wait.h>
#include <linux/workqueue.h>
#include <linux/xarray.h>

struct file;
struct bio_vec;
struct folio;
struct iov_iter;
struct kiocb;
struct xfs_inode;

#define XFS_WICACHE_NR_ADMISSION_LOCKS	64
#define XFS_WICACHE_ADMISSION_MASK	(XFS_WICACHE_NR_ADMISSION_LOCKS - 1)
#define XFS_WICACHE_SEG_SHIFT		9
#define XFS_WICACHE_SEG_SIZE		(1U << XFS_WICACHE_SEG_SHIFT)
#define XFS_WICACHE_NR_SEGS		(PAGE_SIZE / XFS_WICACHE_SEG_SIZE)
#define XFS_WICACHE_VALID_LONGS		BITS_TO_LONGS(XFS_WICACHE_SEG_SIZE)
#define XFS_WICACHE_VALID_SIZE		(XFS_WICACHE_VALID_LONGS * sizeof(unsigned long))
#define XFS_WICACHE_FULL_MASK		((1UL << XFS_WICACHE_NR_SEGS) - 1)
#define XFS_WICACHE_ACCOUNT_GFP(gfp)	((gfp) | __GFP_ACCOUNT)
#define XFS_WICACHE_DIO_CHUNK		(4UL << 20)
#define XFS_WICACHE_DIO_SLOT_SIZE	(4UL << 20)
#define XFS_WICACHE_DIO_SLOTS		2
#define XFS_WICACHE_DIO_SLOT_PAGES	(XFS_WICACHE_DIO_SLOT_SIZE / PAGE_SIZE)
#define XFS_WICACHE_XA_DIRTY		XA_MARK_0
#define XFS_WICACHE_XA_FULL		XA_MARK_1
#define XFS_WICACHE_XA_FLUSHING		XA_MARK_2
#define XFS_WICACHE_SMALL_WRITE_MAX	(1UL << 20)

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
	unsigned long			*active_valid[XFS_WICACHE_NR_SEGS];
	unsigned long			*flushing_valid[XFS_WICACHE_NR_SEGS];
	unsigned long			active_mask;
	unsigned long			flushing_mask;
	struct folio			*active_full;
	struct folio			*prepared_folio;
	bool				prepared_charged;
	bool				flushing_full;
	struct xfs_wicache_batch	*batch;
	int				prepare_error;
	u64				seq;
	enum xfs_wicache_entry_state	state;
	bool				queued;
	u64				prepare_queued_ns;

	refcount_t			refcount;
	struct mutex			lock;
	struct work_struct		prepare_work;
	struct rcu_head			rcu;
};

struct xfs_wicache_inode {
	struct xfs_wicache_mount	*wm;
	struct xfs_inode		*ip;
	struct file			*io_file;
	spinlock_t			file_lock;
	struct list_head		mount_node;

	struct xarray			entries;
	atomic64_t			dirty_bytes;
	atomic64_t			nr_entries;
	atomic64_t			seq;
	atomic_t			batch_active;
	enum xfs_wicache_inode_state	state;

	struct mutex			drain_mutex;
	spinlock_t			drain_lock;
	bool				drain_range_active;
	pgoff_t			drain_first;
	pgoff_t			drain_last;
	struct rw_semaphore		visibility_sem;
	struct delayed_work		flush_work;
	refcount_t			refcount;
	struct rcu_head			rcu;
};

struct xfs_wicache_dio_slot {
	struct list_head		list;
	void				*data;
	struct bio_vec			*bvec;
};

struct xfs_wicache_mount {
	bool				enabled;
	struct mutex			inode_lock;
	struct mutex			admission_locks[XFS_WICACHE_NR_ADMISSION_LOCKS];
	struct list_head		inodes;
	struct workqueue_struct		*control_wq;
	struct workqueue_struct		*io_wq;
	atomic64_t			total_dirty_bytes;
	wait_queue_head_t		dirty_wait;
	spinlock_t			dio_slot_lock;
	struct list_head		dio_free_slots;
	wait_queue_head_t		dio_slot_wait;
	unsigned int			dio_slots_available;
	struct xfs_wicache_dio_slot	*dio_slots;
};

struct xfs_wicache_mount *xfs_wicache_mount_alloc(gfp_t gfp);
void xfs_wicache_mount_free(struct xfs_wicache_mount *wm);

struct xfs_wicache_inode *xfs_wicache_inode_lookup(
		struct xfs_wicache_mount *wm, struct xfs_inode *ip);
struct xfs_wicache_inode *xfs_wicache_inode_get_or_create(
		struct xfs_wicache_mount *wm, struct xfs_inode *ip, gfp_t gfp);
void xfs_wicache_inode_put(struct xfs_wicache_inode *wi);
void xfs_wicache_inode_detach(struct xfs_inode *ip);
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
int xfs_wicache_range_drain(struct xfs_wicache_inode *wi,
		loff_t pos, size_t count);
bool xfs_wicache_range_has_entry(struct xfs_wicache_inode *wi,
		loff_t pos, size_t count);
void xfs_wicache_read_lock(struct xfs_wicache_inode *wi);
void xfs_wicache_read_unlock(struct xfs_wicache_inode *wi);
void xfs_wicache_record_front_iolock(u64 ns);
void xfs_wicache_record_mapping_check(u64 ns);
unsigned long xfs_wicache_io_unit_bytes(void);
bool xfs_wicache_user_dio_enabled(void);
void xfs_wicache_record_middle_dio(size_t bytes, u64 prepare_ns,
		u64 bvec_ns, u64 dio_ns, u64 release_ns);
void xfs_wicache_record_middle_copy(size_t bytes, u64 copy_ns);
void xfs_wicache_record_middle_direct(size_t bytes, u64 dio_ns);
void xfs_wicache_record_middle_staged(size_t bytes, u64 dio_ns);
void xfs_wicache_record_middle_async(u64 submit_ns, u64 wait_ns);
void xfs_wicache_record_fragment(size_t bytes, u64 ns);
void xfs_wicache_record_small_write(size_t bytes, u64 ns);
struct xfs_wicache_dio_slot *xfs_wicache_dio_slot_get(
		struct xfs_wicache_mount *wm);
void xfs_wicache_dio_slot_put(struct xfs_wicache_mount *wm,
		struct xfs_wicache_dio_slot *slot);

ssize_t xfs_wicache_dio_read_folio(struct file *file, loff_t pos,
		struct folio *folio);
ssize_t xfs_wicache_dio_write_folio(struct file *file, loff_t pos,
		struct folio *folio);
ssize_t xfs_wicache_dio_read_folios(struct file *file, loff_t pos,
		struct folio **folios, unsigned int nr);
ssize_t xfs_wicache_dio_write_folios(struct file *file, loff_t pos,
		struct folio **folios, unsigned int nr);
ssize_t xfs_wicache_dio_write_folios_timed(struct file *file, loff_t pos,
		struct folio **folios, unsigned int nr, u64 *bvec_ns,
		u64 *dio_ns);
ssize_t xfs_wicache_dio_write_bvecs_timed(struct file *file, loff_t pos,
		struct bio_vec *bvec, unsigned int nr, size_t count,
		u64 *dio_ns);

#endif /* __XFS_WICACHE_H__ */
