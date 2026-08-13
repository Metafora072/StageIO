// SPDX-License-Identifier: GPL-2.0

#include "xfs.h"
#include "xfs_fs.h"
#include "xfs_shared.h"
#include "xfs_format.h"
#include "xfs_log_format.h"
#include "xfs_trans_resv.h"
#include "xfs_mount.h"
#include "xfs_inode.h"
#include "xfs_wicache.h"

#include <linux/err.h>
#include <linux/file.h>
#include <linux/fs.h>
#include <linux/highmem.h>
#include <linux/jiffies.h>
#include <linux/mm.h>
#include <linux/moduleparam.h>
#include <linux/pagemap.h>
#include <linux/slab.h>
#include <linux/uio.h>

static bool xfs_wicache_enable;
static unsigned int xfs_wicache_batch = 32;
static unsigned int xfs_wicache_qd = 16;
static unsigned int xfs_wicache_delay_ms = 1;
static unsigned long xfs_wicache_high_bytes = 256UL << 20;

static atomic64_t xfs_wicache_global_mem_bytes;
static atomic64_t xfs_wicache_global_peak_bytes;
static atomic64_t xfs_wicache_global_accepted_bytes;
static atomic64_t xfs_wicache_global_drained_bytes;
static atomic64_t xfs_wicache_global_base_reads;
static atomic64_t xfs_wicache_global_cache_bases;
static atomic64_t xfs_wicache_global_full_cancels;
static atomic64_t xfs_wicache_global_device_read_bytes;
static atomic64_t xfs_wicache_global_device_write_bytes;
static atomic64_t xfs_wicache_global_flush_errors;

module_param_named(wicache_enable, xfs_wicache_enable, bool, 0644);
MODULE_PARM_DESC(wicache_enable, "Enable experimental sparse write overlay");
module_param_named(wicache_batch, xfs_wicache_batch, uint, 0644);
MODULE_PARM_DESC(wicache_batch, "Maximum pages admitted by one flush scan");
module_param_named(wicache_qd, xfs_wicache_qd, uint, 0644);
MODULE_PARM_DESC(wicache_qd, "Maximum concurrent RMW page workers");
module_param_named(wicache_delay_ms, xfs_wicache_delay_ms, uint, 0644);
MODULE_PARM_DESC(wicache_delay_ms, "Dirty accumulation delay in milliseconds");
module_param_named(wicache_high_bytes, xfs_wicache_high_bytes, ulong, 0644);
MODULE_PARM_DESC(wicache_high_bytes, "Sparse overlay payload high watermark");

static int
xfs_wicache_atomic64_get(
	char			*buffer,
	const struct kernel_param *kp)
{
	return sysfs_emit(buffer, "%lld\n", atomic64_read(kp->arg));
}

static const struct kernel_param_ops xfs_wicache_atomic64_ops = {
	.get = xfs_wicache_atomic64_get,
};

module_param_cb(wicache_dirty_bytes, &xfs_wicache_atomic64_ops,
		&xfs_wicache_global_mem_bytes, 0444);
module_param_cb(wicache_peak_bytes, &xfs_wicache_atomic64_ops,
		&xfs_wicache_global_peak_bytes, 0444);
module_param_cb(wicache_accepted_bytes, &xfs_wicache_atomic64_ops,
		&xfs_wicache_global_accepted_bytes, 0444);
module_param_cb(wicache_drained_bytes, &xfs_wicache_atomic64_ops,
		&xfs_wicache_global_drained_bytes, 0444);
module_param_cb(wicache_base_reads, &xfs_wicache_atomic64_ops,
		&xfs_wicache_global_base_reads, 0444);
module_param_cb(wicache_cache_bases, &xfs_wicache_atomic64_ops,
		&xfs_wicache_global_cache_bases, 0444);
module_param_cb(wicache_full_cancels, &xfs_wicache_atomic64_ops,
		&xfs_wicache_global_full_cancels, 0444);
module_param_cb(wicache_device_read_bytes, &xfs_wicache_atomic64_ops,
		&xfs_wicache_global_device_read_bytes, 0444);
module_param_cb(wicache_device_write_bytes, &xfs_wicache_atomic64_ops,
		&xfs_wicache_global_device_write_bytes, 0444);
module_param_cb(wicache_flush_errors, &xfs_wicache_atomic64_ops,
		&xfs_wicache_global_flush_errors, 0444);

static const struct rhashtable_params xfs_wicache_inode_hash_params = {
	.key_len		= sizeof(struct xfs_inode *),
	.key_offset		= offsetof(struct xfs_wicache_inode, ip),
	.head_offset		= offsetof(struct xfs_wicache_inode, hash_node),
	.automatic_shrinking	= true,
};

struct xfs_wicache_batch {
	struct work_struct		dispatch_work;
	atomic_t			prepare_pending;
	unsigned int			nr;
	struct xfs_wicache_entry	*entries[];
};

static void xfs_wicache_entry_prepare_work(struct work_struct *work);
static void xfs_wicache_finish_entry(struct xfs_wicache_entry *entry);
static void xfs_wicache_batch_dispatch_work(struct work_struct *work);
static void xfs_wicache_inode_flush_work(struct work_struct *work);

static inline unsigned int
xfs_wicache_shard_id(
	pgoff_t		page_index)
{
	return page_index & XFS_WICACHE_SHARD_MASK;
}

static inline struct xfs_wicache_shard *
xfs_wicache_shard(
	struct xfs_wicache_inode *wi,
	pgoff_t		page_index)
{
	return &wi->shards[xfs_wicache_shard_id(page_index)];
}

static void
xfs_wicache_update_peak(
	s64			value)
{
	s64			old;

	old = atomic64_read(&xfs_wicache_global_peak_bytes);
	while (value > old) {
		s64 seen = atomic64_cmpxchg(&xfs_wicache_global_peak_bytes,
				old, value);

		if (seen == old)
			break;
		old = seen;
	}
}

static int
xfs_wicache_charge(
	struct xfs_wicache_mount *wm,
	s64			bytes,
	bool			throttle)
{
	s64			old;

	for (;;) {
		old = atomic64_read(&wm->total_dirty_bytes);
		if (!throttle || old + bytes <= xfs_wicache_high_bytes) {
			if (atomic64_cmpxchg(&wm->total_dirty_bytes, old,
					old + bytes) == old)
				break;
			continue;
		}

		if (wait_event_killable(wm->dirty_wait,
				atomic64_read(&wm->total_dirty_bytes) + bytes <=
				xfs_wicache_high_bytes))
			return -ERESTARTSYS;
	}

	old = atomic64_add_return(bytes, &xfs_wicache_global_mem_bytes);
	xfs_wicache_update_peak(old);
	return 0;
}

static void
xfs_wicache_uncharge(
	struct xfs_wicache_mount *wm,
	s64			bytes)
{
	atomic64_sub(bytes, &wm->total_dirty_bytes);
	atomic64_sub(bytes, &xfs_wicache_global_mem_bytes);
	wake_up_all(&wm->dirty_wait);
}

static bool
xfs_wicache_entry_get(
	struct xfs_wicache_entry *entry)
{
	return refcount_inc_not_zero(&entry->refcount);
}

static void
xfs_wicache_entry_free_rcu(
	struct rcu_head		*rcu)
{
	struct xfs_wicache_entry *entry = container_of(rcu,
			struct xfs_wicache_entry, rcu);
	unsigned int		i;

	for (i = 0; i < XFS_WICACHE_NR_SEGS; i++) {
		kfree(entry->active[i]);
		kfree(entry->flushing[i]);
	}
	if (entry->transient_base)
		folio_put(entry->transient_base);
	if (entry->prepared_folio)
		folio_put(entry->prepared_folio);
	WARN_ON_ONCE(entry->io_file);
	kfree(entry);
}

static void
xfs_wicache_entry_put(
	struct xfs_wicache_entry *entry)
{
	if (entry && refcount_dec_and_test(&entry->refcount))
		call_rcu(&entry->rcu, xfs_wicache_entry_free_rcu);
}

static struct xfs_wicache_entry *
xfs_wicache_entry_alloc(
	struct xfs_wicache_inode *wi,
	struct file		*file,
	pgoff_t		page_index,
	gfp_t			gfp)
{
	struct xfs_wicache_entry *entry;

	entry = kzalloc(sizeof(*entry), XFS_WICACHE_ACCOUNT_GFP(gfp));
	if (!entry)
		return NULL;

	entry->wi = wi;
	entry->io_file = get_file(file);
	entry->page_index = page_index;
	entry->state = XFS_WICACHE_ENTRY_DIRTY;
	refcount_set(&entry->refcount, 1);
	mutex_init(&entry->lock);
	INIT_LIST_HEAD(&entry->dirty_node);
	INIT_WORK(&entry->prepare_work, xfs_wicache_entry_prepare_work);
	return entry;
}

static void
xfs_wicache_init_shard(
	struct xfs_wicache_shard *shard)
{
	xa_init(&shard->entries);
	spin_lock_init(&shard->dirty_lock);
	INIT_LIST_HEAD(&shard->dirty_list);
}

static void
xfs_wicache_entry_drop_buffers(
	struct xfs_wicache_entry *entry)
{
	struct xfs_wicache_mount *wm = entry->wi->wm;
	struct file		*file;
	unsigned int		i;

	mutex_lock(&entry->lock);
	for (i = 0; i < XFS_WICACHE_NR_SEGS; i++) {
		if (entry->active[i]) {
			kfree(entry->active[i]);
			entry->active[i] = NULL;
			xfs_wicache_uncharge(wm, XFS_WICACHE_SEG_SIZE);
		}
		if (entry->flushing[i]) {
			kfree(entry->flushing[i]);
			entry->flushing[i] = NULL;
			xfs_wicache_uncharge(wm, XFS_WICACHE_SEG_SIZE);
		}
	}
	if (entry->transient_base) {
		folio_put(entry->transient_base);
		entry->transient_base = NULL;
		xfs_wicache_uncharge(wm, PAGE_SIZE);
	}
	if (entry->prepared_folio) {
		folio_put(entry->prepared_folio);
		entry->prepared_folio = NULL;
		xfs_wicache_uncharge(wm, PAGE_SIZE);
	}
	entry->active_mask = 0;
	entry->flushing_mask = 0;
	file = entry->io_file;
	entry->io_file = NULL;
	entry->state = XFS_WICACHE_ENTRY_INVALID;
	mutex_unlock(&entry->lock);
	if (file)
		fput(file);
}

static void
xfs_wicache_destroy_shard(
	struct xfs_wicache_inode *wi,
	struct xfs_wicache_shard *shard)
{
	struct xfs_wicache_entry *entry;
	unsigned long		index;

	xa_for_each(&shard->entries, index, entry) {
		xa_erase(&shard->entries, index);
		if (!list_empty(&entry->dirty_node))
			list_del_init(&entry->dirty_node);
		xfs_wicache_entry_drop_buffers(entry);
		atomic64_dec(&wi->nr_entries);
		xfs_wicache_entry_put(entry);
	}
	xa_destroy(&shard->entries);
}

static struct xfs_wicache_inode *
xfs_wicache_inode_alloc(
	struct xfs_wicache_mount *wm,
	struct xfs_inode	*ip,
	gfp_t			gfp)
{
	struct xfs_wicache_inode *wi;
	unsigned int		i;

	wi = kzalloc(sizeof(*wi), XFS_WICACHE_ACCOUNT_GFP(gfp));
	if (!wi)
		return NULL;

	wi->wm = wm;
	wi->ip = ip;
	for (i = 0; i < XFS_WICACHE_NR_SHARDS; i++)
		xfs_wicache_init_shard(&wi->shards[i]);
	atomic64_set(&wi->dirty_bytes, 0);
	atomic64_set(&wi->nr_entries, 0);
	atomic64_set(&wi->seq, 0);
	atomic_set(&wi->batch_active, 0);
	wi->state = XFS_WICACHE_INODE_ACTIVE;
	init_rwsem(&wi->visibility_sem);
	mutex_init(&wi->scan_lock);
	wi->flush_cursor = 0;
	INIT_DELAYED_WORK(&wi->flush_work, xfs_wicache_inode_flush_work);
	INIT_LIST_HEAD(&wi->mount_node);
	refcount_set(&wi->refcount, 1);
	return wi;
}

static void
xfs_wicache_inode_destroy(
	struct xfs_wicache_inode *wi)
{
	unsigned int		i;

	for (i = 0; i < XFS_WICACHE_NR_SHARDS; i++)
		xfs_wicache_destroy_shard(wi, &wi->shards[i]);
}

static void
xfs_wicache_inode_free_rcu(
	struct rcu_head		*rcu)
{
	struct xfs_wicache_inode *wi = container_of(rcu,
			struct xfs_wicache_inode, rcu);

	xfs_wicache_inode_destroy(wi);
	kfree(wi);
}

void
xfs_wicache_inode_put(
	struct xfs_wicache_inode *wi)
{
	if (wi && refcount_dec_and_test(&wi->refcount))
		call_rcu(&wi->rcu, xfs_wicache_inode_free_rcu);
}

static void
xfs_wicache_free_inode_record(
	void			*ptr,
	void			*arg)
{
	xfs_wicache_inode_put(ptr);
}

static void
xfs_wicache_reset_stats(void)
{
	atomic64_set(&xfs_wicache_global_mem_bytes, 0);
	atomic64_set(&xfs_wicache_global_peak_bytes, 0);
	atomic64_set(&xfs_wicache_global_accepted_bytes, 0);
	atomic64_set(&xfs_wicache_global_drained_bytes, 0);
	atomic64_set(&xfs_wicache_global_base_reads, 0);
	atomic64_set(&xfs_wicache_global_cache_bases, 0);
	atomic64_set(&xfs_wicache_global_full_cancels, 0);
	atomic64_set(&xfs_wicache_global_device_read_bytes, 0);
	atomic64_set(&xfs_wicache_global_device_write_bytes, 0);
	atomic64_set(&xfs_wicache_global_flush_errors, 0);
}

struct xfs_wicache_mount *
xfs_wicache_mount_alloc(
	gfp_t			gfp)
{
	struct xfs_wicache_mount *wm;
	int			error;

	wm = kzalloc(sizeof(*wm), gfp);
	if (!wm)
		return ERR_PTR(-ENOMEM);

	mutex_init(&wm->inode_lock);
	INIT_LIST_HEAD(&wm->inodes);
	init_waitqueue_head(&wm->dirty_wait);
	atomic64_set(&wm->total_dirty_bytes, 0);
	error = rhashtable_init(&wm->inode_table,
			&xfs_wicache_inode_hash_params);
	if (error)
		goto out_free;

	wm->enabled = xfs_wicache_enable;
	if (!wm->enabled)
		return wm;

	xfs_wicache_batch = clamp_t(unsigned int, xfs_wicache_batch, 1, 128);
	xfs_wicache_qd = clamp_t(unsigned int, xfs_wicache_qd, 1, 128);
	if (xfs_wicache_high_bytes < PAGE_SIZE * xfs_wicache_qd)
		xfs_wicache_high_bytes = PAGE_SIZE * xfs_wicache_qd;

	wm->control_wq = alloc_ordered_workqueue("xfs-mbuffer-ctl",
			WQ_MEM_RECLAIM);
	if (!wm->control_wq) {
		error = -ENOMEM;
		goto out_hash;
	}
	wm->io_wq = alloc_workqueue("xfs-mbuffer-io",
			WQ_UNBOUND | WQ_MEM_RECLAIM, xfs_wicache_qd);
	if (!wm->io_wq) {
		error = -ENOMEM;
		goto out_control;
	}

	xfs_wicache_reset_stats();
	return wm;

out_control:
	destroy_workqueue(wm->control_wq);
out_hash:
	rhashtable_destroy(&wm->inode_table);
out_free:
	kfree(wm);
	return ERR_PTR(error);
}

static void
xfs_wicache_kick_inode(
	struct xfs_wicache_inode *wi,
	unsigned long		delay)
{
	if (wi->wm->control_wq)
		mod_delayed_work(wi->wm->control_wq, &wi->flush_work, delay);
}

void
xfs_wicache_mount_free(
	struct xfs_wicache_mount *wm)
{
	struct xfs_wicache_inode *wi;
	unsigned int		loops = 0;

	if (!wm)
		return;

	if (wm->enabled) {
		while (atomic64_read(&wm->total_dirty_bytes) && loops++ < 4096) {
			mutex_lock(&wm->inode_lock);
			list_for_each_entry(wi, &wm->inodes, mount_node)
				xfs_wicache_kick_inode(wi, 0);
			mutex_unlock(&wm->inode_lock);
			flush_workqueue(wm->control_wq);
			flush_workqueue(wm->io_wq);
		}
		if (atomic64_read(&wm->total_dirty_bytes))
			pr_err("XFS MBuffer: unmount with %lld buffered bytes\n",
				atomic64_read(&wm->total_dirty_bytes));

		wm->enabled = false;
		destroy_workqueue(wm->control_wq);
		destroy_workqueue(wm->io_wq);
	}

	mutex_lock(&wm->inode_lock);
	while (!list_empty(&wm->inodes)) {
		wi = list_first_entry(&wm->inodes,
				struct xfs_wicache_inode, mount_node);
		list_del_init(&wi->mount_node);
	}
	mutex_unlock(&wm->inode_lock);
	rhashtable_free_and_destroy(&wm->inode_table,
			xfs_wicache_free_inode_record, NULL);
	rcu_barrier();
	kfree(wm);
}

struct xfs_wicache_inode *
xfs_wicache_inode_lookup(
	struct xfs_wicache_mount *wm,
	struct xfs_inode	*ip)
{
	struct xfs_wicache_inode *wi;

	if (!wm || !ip || !wm->enabled)
		return NULL;

	rcu_read_lock();
	wi = rhashtable_lookup_fast(&wm->inode_table, &ip,
			xfs_wicache_inode_hash_params);
	if (wi && !refcount_inc_not_zero(&wi->refcount)) {
		wi = NULL;
	} else if (wi && READ_ONCE(wi->state) !=
			XFS_WICACHE_INODE_ACTIVE) {
		xfs_wicache_inode_put(wi);
		wi = NULL;
	}
	rcu_read_unlock();
	return wi;
}

struct xfs_wicache_inode *
xfs_wicache_inode_get_or_create(
	struct xfs_wicache_mount *wm,
	struct xfs_inode	*ip,
	gfp_t			gfp)
{
	struct xfs_wicache_inode *wi, *old;

	if (!wm || !ip || !wm->enabled)
		return ERR_PTR(-EOPNOTSUPP);

	wi = xfs_wicache_inode_lookup(wm, ip);
	if (wi)
		return wi;

	wi = xfs_wicache_inode_alloc(wm, ip, gfp);
	if (!wi)
		return ERR_PTR(-ENOMEM);

	refcount_inc(&wi->refcount);
	old = rhashtable_lookup_get_insert_fast(&wm->inode_table,
			&wi->hash_node, xfs_wicache_inode_hash_params);
	if (IS_ERR(old)) {
		xfs_wicache_inode_put(wi);
		xfs_wicache_inode_put(wi);
		return ERR_CAST(old);
	}
	if (old) {
		if (!refcount_inc_not_zero(&old->refcount)) {
			old = ERR_PTR(-ENOENT);
		} else if (READ_ONCE(old->state) !=
				XFS_WICACHE_INODE_ACTIVE) {
			xfs_wicache_inode_put(old);
			old = ERR_PTR(-ENOENT);
		}
		xfs_wicache_inode_put(wi);
		xfs_wicache_inode_put(wi);
		return old;
	}

	mutex_lock(&wm->inode_lock);
	list_add_tail(&wi->mount_node, &wm->inodes);
	mutex_unlock(&wm->inode_lock);
	return wi;
}

static struct xfs_wicache_entry *
xfs_wicache_lookup_entry(
	struct xfs_wicache_inode *wi,
	pgoff_t		page_index)
{
	struct xfs_wicache_shard *shard = xfs_wicache_shard(wi,
			page_index);
	struct xfs_wicache_entry *entry;

	rcu_read_lock();
	entry = xa_load(&shard->entries, page_index);
	if (entry && (!xfs_wicache_entry_get(entry) ||
		    READ_ONCE(entry->state) == XFS_WICACHE_ENTRY_INVALID)) {
		if (refcount_read(&entry->refcount))
			xfs_wicache_entry_put(entry);
		entry = NULL;
	}
	rcu_read_unlock();
	return entry;
}

static struct xfs_wicache_entry *
xfs_wicache_get_or_create_entry(
	struct xfs_wicache_inode *wi,
	struct file		*file,
	pgoff_t		page_index)
{
	struct xfs_wicache_shard *shard = xfs_wicache_shard(wi,
			page_index);
	struct xfs_wicache_entry *entry, *old;

retry:
	entry = xfs_wicache_lookup_entry(wi, page_index);
	if (entry)
		return entry;

	entry = xfs_wicache_entry_alloc(wi, file, page_index, GFP_NOFS);
	if (!entry)
		return ERR_PTR(-ENOMEM);

	old = xa_cmpxchg(&shard->entries, page_index, NULL, entry,
			XFS_WICACHE_ACCOUNT_GFP(GFP_NOFS));
	if (xa_is_err(old)) {
		struct file *held = entry->io_file;

		entry->io_file = NULL;
		fput(held);
		xfs_wicache_entry_put(entry);
		return ERR_PTR(xa_err(old));
	}
	if (old) {
		struct file *held = entry->io_file;

		entry->io_file = NULL;
		fput(held);
		xfs_wicache_entry_put(entry);
		cond_resched();
		goto retry;
	}

	atomic64_inc(&wi->nr_entries);
	xfs_wicache_entry_get(entry);
	return entry;
}

static void
xfs_wicache_mark_dirty(
	struct xfs_wicache_entry *entry)
{
	struct xfs_wicache_shard *shard = xfs_wicache_shard(entry->wi,
			entry->page_index);
	unsigned long		delay;

	spin_lock(&shard->dirty_lock);
	if (!entry->queued && list_empty(&entry->dirty_node))
		list_add_tail(&entry->dirty_node, &shard->dirty_list);
	spin_unlock(&shard->dirty_lock);
	delay = msecs_to_jiffies(xfs_wicache_delay_ms);
	if (atomic64_read(&entry->wi->wm->total_dirty_bytes) >=
	    xfs_wicache_high_bytes * 3 / 4)
		delay = 0;
	xfs_wicache_kick_inode(entry->wi, delay);
}

bool
xfs_wicache_can_stage(
	struct kiocb		*iocb,
	struct iov_iter		*from)
{
	size_t			count;

	if (!iocb || !from)
		return false;
	count = iov_iter_count(from);
	if (!count || !IS_ALIGNED(iocb->ki_pos, XFS_WICACHE_SEG_SIZE) ||
	    !IS_ALIGNED(count, XFS_WICACHE_SEG_SIZE))
		return false;
	if (iocb->ki_flags & (IOCB_DIRECT | IOCB_DSYNC | IOCB_SYNC |
			IOCB_APPEND | IOCB_NOWAIT))
		return false;
	return true;
}

static int
xfs_wicache_stage_segment(
	struct xfs_wicache_entry *entry,
	unsigned int		seg,
	struct iov_iter		*from)
{
	struct xfs_wicache_mount *wm = entry->wi->wm;
	u8			*new_data = NULL;
	size_t			copied;
	int			error;

	mutex_lock(&entry->lock);
	if (entry->state == XFS_WICACHE_ENTRY_INVALID) {
		mutex_unlock(&entry->lock);
		return -EAGAIN;
	}
	if (entry->active[seg]) {
		copied = copy_from_iter(entry->active[seg],
				XFS_WICACHE_SEG_SIZE, from);
		if (copied == XFS_WICACHE_SEG_SIZE) {
			entry->active_mask |= BIT(seg);
			entry->seq = atomic64_inc_return(&entry->wi->seq);
		}
		mutex_unlock(&entry->lock);
		return copied == XFS_WICACHE_SEG_SIZE ? 0 : -EFAULT;
	}
	mutex_unlock(&entry->lock);

	error = xfs_wicache_charge(wm, XFS_WICACHE_SEG_SIZE, true);
	if (error)
		return error;
	new_data = kmalloc(XFS_WICACHE_SEG_SIZE,
			XFS_WICACHE_ACCOUNT_GFP(GFP_NOFS));
	if (!new_data) {
		xfs_wicache_uncharge(wm, XFS_WICACHE_SEG_SIZE);
		return -ENOMEM;
	}
	copied = copy_from_iter(new_data, XFS_WICACHE_SEG_SIZE, from);
	if (copied != XFS_WICACHE_SEG_SIZE) {
		kfree(new_data);
		xfs_wicache_uncharge(wm, XFS_WICACHE_SEG_SIZE);
		return -EFAULT;
	}

	mutex_lock(&entry->lock);
	if (entry->state == XFS_WICACHE_ENTRY_INVALID) {
		mutex_unlock(&entry->lock);
		kfree(new_data);
		xfs_wicache_uncharge(wm, XFS_WICACHE_SEG_SIZE);
		return -EAGAIN;
	}
	if (entry->active[seg]) {
		memcpy(entry->active[seg], new_data, XFS_WICACHE_SEG_SIZE);
		kfree(new_data);
		xfs_wicache_uncharge(wm, XFS_WICACHE_SEG_SIZE);
	} else {
		entry->active[seg] = new_data;
	}
	entry->active_mask |= BIT(seg);
	entry->seq = atomic64_inc_return(&entry->wi->seq);
	mutex_unlock(&entry->lock);
	return 0;
}

ssize_t
xfs_wicache_stage_iter(
	struct xfs_wicache_inode *wi,
	struct file		*file,
	struct iov_iter		*from,
	loff_t			pos,
	size_t			count)
{
	size_t			written = 0;
	int			error = 0;

	while (written < count) {
		pgoff_t index = (pos + written) >> PAGE_SHIFT;
		unsigned int seg = offset_in_page(pos + written) >>
				XFS_WICACHE_SEG_SHIFT;
		struct xfs_wicache_entry *entry;

retry_entry:
		entry = xfs_wicache_get_or_create_entry(wi, file, index);
		if (IS_ERR(entry)) {
			error = PTR_ERR(entry);
			break;
		}
		error = xfs_wicache_stage_segment(entry, seg, from);
		if (error == -EAGAIN) {
			xfs_wicache_entry_put(entry);
			cond_resched();
			goto retry_entry;
		}
		if (!error)
			xfs_wicache_mark_dirty(entry);
		xfs_wicache_entry_put(entry);
		if (error)
			break;
		written += XFS_WICACHE_SEG_SIZE;
	}

	atomic64_add(written, &xfs_wicache_global_accepted_bytes);
	atomic64_add(written, &wi->dirty_bytes);
	return written ? written : error;
}

static int
xfs_wicache_overlay_data(
	struct xfs_wicache_entry *entry,
	u8			**segments,
	unsigned long		mask,
	struct iov_iter		*to,
	loff_t			pos,
	size_t			count)
{
	loff_t			end = pos + count;
	unsigned int		i;

	for_each_set_bit(i, &mask, XFS_WICACHE_NR_SEGS) {
		loff_t seg_start = ((loff_t)entry->page_index << PAGE_SHIFT) +
				((loff_t)i << XFS_WICACHE_SEG_SHIFT);
		loff_t copy_start = max_t(loff_t, pos, seg_start);
		loff_t copy_end = min_t(loff_t, end,
				seg_start + XFS_WICACHE_SEG_SIZE);
		struct iov_iter dst;
		size_t bytes;

		if (copy_start >= copy_end || !segments[i])
			continue;
		dst = *to;
		iov_iter_advance(&dst, copy_start - pos);
		bytes = copy_end - copy_start;
		if (copy_to_iter(segments[i] + copy_start - seg_start,
				bytes, &dst) != bytes)
			return -EFAULT;
	}
	return 0;
}

int
xfs_wicache_overlay_iter(
	struct xfs_wicache_inode *wi,
	struct iov_iter		*to,
	loff_t			pos,
	size_t			count)
{
	pgoff_t		first = pos >> PAGE_SHIFT;
	pgoff_t		last = (pos + count - 1) >> PAGE_SHIFT;
	pgoff_t		index;
	int			error = 0;

	for (index = first; index <= last; index++) {
		struct xfs_wicache_entry *entry;

		entry = xfs_wicache_lookup_entry(wi, index);
		if (!entry)
			continue;
		mutex_lock(&entry->lock);
		error = xfs_wicache_overlay_data(entry, entry->flushing,
				entry->flushing_mask, to, pos, count);
		if (!error)
			error = xfs_wicache_overlay_data(entry, entry->active,
					entry->active_mask, to, pos, count);
		mutex_unlock(&entry->lock);
		xfs_wicache_entry_put(entry);
		if (error)
			break;
	}
	return error;
}

bool
xfs_wicache_inode_has_dirty(
	struct xfs_wicache_inode *wi)
{
	return atomic64_read(&wi->nr_entries) != 0;
}

void
xfs_wicache_read_lock(
	struct xfs_wicache_inode *wi)
{
	down_write(&wi->visibility_sem);
}

void
xfs_wicache_read_unlock(
	struct xfs_wicache_inode *wi)
{
	up_write(&wi->visibility_sem);
}

static bool
xfs_wicache_copy_cache_base(
	struct file		*file,
	pgoff_t		index,
	struct folio		*dst)
{
	struct address_space	*mapping = file->f_mapping;
	struct folio		*src;
	void			*src_addr, *dst_addr;
	size_t			offset;
	bool			copied = false;

	src = filemap_get_folio(mapping, index);
	if (IS_ERR(src))
		return false;
	folio_lock(src);
	if (folio_mapping(src) == mapping && folio_test_uptodate(src) &&
	    !folio_test_dirty(src) && !folio_test_writeback(src)) {
		offset = (index - src->index) << PAGE_SHIFT;
		src_addr = kmap_local_folio(src, offset);
		dst_addr = kmap_local_folio(dst, 0);
		memcpy(dst_addr, src_addr, PAGE_SIZE);
		kunmap_local(dst_addr);
		kunmap_local(src_addr);
		copied = true;
	}
	folio_unlock(src);
	folio_put(src);
	return copied;
}

static void
xfs_wicache_copy_folio(
	struct folio		*dst,
	struct folio		*src)
{
	void			*src_addr = kmap_local_folio(src, 0);
	void			*dst_addr = kmap_local_folio(dst, 0);

	memcpy(dst_addr, src_addr, PAGE_SIZE);
	kunmap_local(dst_addr);
	kunmap_local(src_addr);
}

static void
xfs_wicache_apply_flushing(
	struct xfs_wicache_entry *entry,
	struct folio		*folio)
{
	void			*addr = kmap_local_folio(folio, 0);
	unsigned int		i;

	mutex_lock(&entry->lock);
	for_each_set_bit(i, &entry->flushing_mask, XFS_WICACHE_NR_SEGS)
		memcpy(addr + (i << XFS_WICACHE_SEG_SHIFT),
				entry->flushing[i], XFS_WICACHE_SEG_SIZE);
	mutex_unlock(&entry->lock);
	kunmap_local(addr);
}

static void
xfs_wicache_requeue_entry(
	struct xfs_wicache_entry *entry,
	bool			dirty)
{
	struct xfs_wicache_shard *shard = xfs_wicache_shard(entry->wi,
			entry->page_index);

	spin_lock(&shard->dirty_lock);
	entry->queued = false;
	if (dirty && list_empty(&entry->dirty_node))
		list_add_tail(&entry->dirty_node, &shard->dirty_list);
	spin_unlock(&shard->dirty_lock);
}

static void
xfs_wicache_remove_entry(
	struct xfs_wicache_entry *entry)
{
	struct xfs_wicache_inode *wi = entry->wi;
	struct xfs_wicache_shard *shard = xfs_wicache_shard(wi,
			entry->page_index);
	struct xfs_wicache_entry *old;

	old = xa_cmpxchg(&shard->entries, entry->page_index, entry, NULL,
			GFP_NOFS);
	if (old == entry) {
		atomic64_dec(&wi->nr_entries);
		xfs_wicache_entry_put(entry);
	}
}

static int
xfs_wicache_prepare_entry(
	struct xfs_wicache_entry *entry)
{
	struct xfs_wicache_mount *wm = entry->wi->wm;
	struct folio		*folio = NULL, *base = NULL;
	struct file		*file = NULL;
	unsigned long		flush_mask;
	ssize_t			ret = 0;
	bool			charged = false;
	unsigned int		i;

	mutex_lock(&entry->lock);
	if (entry->state == XFS_WICACHE_ENTRY_INVALID ||
	    !entry->active_mask || entry->flushing_mask ||
	    entry->prepared_folio) {
		ret = -EAGAIN;
		goto out_unlock;
	}
	for (i = 0; i < XFS_WICACHE_NR_SEGS; i++) {
		entry->flushing[i] = entry->active[i];
		entry->active[i] = NULL;
	}
	entry->flushing_mask = entry->active_mask;
	entry->active_mask = 0;
	entry->state = XFS_WICACHE_ENTRY_FLUSHING;
	flush_mask = entry->flushing_mask;
	file = get_file(entry->io_file);
	if (entry->transient_base) {
		base = entry->transient_base;
		folio_get(base);
	}
	mutex_unlock(&entry->lock);

	ret = xfs_wicache_charge(wm, PAGE_SIZE, false);
	if (ret)
		goto out;
	charged = true;
	folio = folio_alloc(XFS_WICACHE_ACCOUNT_GFP(GFP_NOFS), 0);
	if (!folio) {
		xfs_wicache_uncharge(wm, PAGE_SIZE);
		charged = false;
		ret = -ENOMEM;
		goto out;
	}

	if (flush_mask == XFS_WICACHE_FULL_MASK) {
		folio_zero_range(folio, 0, PAGE_SIZE);
		atomic64_inc(&xfs_wicache_global_full_cancels);
	} else if (base) {
		xfs_wicache_copy_folio(folio, base);
		atomic64_inc(&xfs_wicache_global_cache_bases);
	} else if (xfs_wicache_copy_cache_base(file, entry->page_index,
			folio)) {
		atomic64_inc(&xfs_wicache_global_cache_bases);
	} else {
		ret = -ENODATA;
	}

	if (!ret)
		xfs_wicache_apply_flushing(entry, folio);
out:
	mutex_lock(&entry->lock);
	entry->prepared_folio = folio;
	entry->prepared_charged = charged;
	entry->prepare_error = ret;
	mutex_unlock(&entry->lock);
	if (base)
		folio_put(base);
	if (file)
		fput(file);
	return ret;

out_unlock:
	entry->prepare_error = ret;
	mutex_unlock(&entry->lock);
	return ret;
}

static void
xfs_wicache_entry_prepare_work(
	struct work_struct	*work)
{
	struct xfs_wicache_entry *entry = container_of(work,
			struct xfs_wicache_entry, prepare_work);
	struct xfs_wicache_batch *batch = entry->batch;

	xfs_wicache_prepare_entry(entry);
	if (atomic_dec_and_test(&batch->prepare_pending))
		queue_work(entry->wi->wm->io_wq, &batch->dispatch_work);
}

static void
xfs_wicache_batch_dispatch_work(
	struct work_struct	*work)
{
	struct xfs_wicache_batch *batch = container_of(work,
			struct xfs_wicache_batch, dispatch_work);
	struct xfs_wicache_inode *wi = batch->entries[0]->wi;
	struct folio		**folios;
	struct file		*file;
	unsigned int		i, first, nr;
	ssize_t			ret;

	folios = kcalloc(batch->nr, sizeof(*folios), GFP_NOFS);
	if (!folios) {
		for (i = 0; i < batch->nr; i++) {
			mutex_lock(&batch->entries[i]->lock);
			batch->entries[i]->prepare_error = -ENOMEM;
			mutex_unlock(&batch->entries[i]->lock);
			xfs_wicache_finish_entry(batch->entries[i]);
		}
		atomic_dec(&wi->batch_active);
		if (atomic64_read(&wi->nr_entries))
			xfs_wicache_kick_inode(wi, 0);
		kfree(batch);
		return;
	}

	for (i = 1; i < batch->nr; i++) {
		struct xfs_wicache_entry *entry = batch->entries[i];
		unsigned int j = i;

		while (j && batch->entries[j - 1]->page_index >
				entry->page_index) {
			batch->entries[j] = batch->entries[j - 1];
			j--;
		}
		batch->entries[j] = entry;
	}

	for (first = 0; first < batch->nr; first += nr) {
		struct xfs_wicache_entry *entry = batch->entries[first];

		mutex_lock(&entry->lock);
		if (entry->prepare_error != -ENODATA) {
			mutex_unlock(&entry->lock);
			nr = 1;
			continue;
		}
		file = get_file(entry->io_file);
		folios[0] = entry->prepared_folio;
		mutex_unlock(&entry->lock);
		for (nr = 1; first + nr < batch->nr; nr++) {
			entry = batch->entries[first + nr];
			mutex_lock(&entry->lock);
			if (entry->prepare_error != -ENODATA ||
			    entry->page_index !=
				batch->entries[first]->page_index + nr ||
			    entry->io_file != file) {
				mutex_unlock(&entry->lock);
				break;
			}
			folios[nr] = entry->prepared_folio;
			mutex_unlock(&entry->lock);
		}
		ret = xfs_wicache_dio_read_folios(file,
				(loff_t)batch->entries[first]->page_index <<
				PAGE_SHIFT, folios, nr);
		fput(file);
		if (ret != (ssize_t)nr << PAGE_SHIFT)
			ret = ret < 0 ? ret : -EIO;
		for (i = 0; i < nr; i++) {
			entry = batch->entries[first + i];
			mutex_lock(&entry->lock);
			entry->prepare_error = ret < 0 ? ret : 0;
			mutex_unlock(&entry->lock);
			if (ret >= 0) {
				xfs_wicache_apply_flushing(entry, folios[i]);
				atomic64_inc(&xfs_wicache_global_base_reads);
				atomic64_add(PAGE_SIZE,
					&xfs_wicache_global_device_read_bytes);
			}
		}
	}

	down_read(&wi->visibility_sem);
	for (first = 0; first < batch->nr; first += nr) {
		struct xfs_wicache_entry *entry = batch->entries[first];

		mutex_lock(&entry->lock);
		if (entry->prepare_error) {
			mutex_unlock(&entry->lock);
			nr = 1;
			continue;
		}
		file = get_file(entry->io_file);
		folios[0] = entry->prepared_folio;
		mutex_unlock(&entry->lock);
		for (nr = 1; first + nr < batch->nr; nr++) {
			entry = batch->entries[first + nr];
			mutex_lock(&entry->lock);
			if (entry->prepare_error ||
			    entry->page_index !=
				batch->entries[first]->page_index + nr ||
			    entry->io_file != file) {
				mutex_unlock(&entry->lock);
				break;
			}
			folios[nr] = entry->prepared_folio;
			mutex_unlock(&entry->lock);
		}
		ret = xfs_wicache_dio_write_folios(file,
				(loff_t)batch->entries[first]->page_index <<
				PAGE_SHIFT, folios, nr);
		fput(file);
		if (ret != (ssize_t)nr << PAGE_SHIFT)
			ret = ret < 0 ? ret : -EIO;
		for (i = 0; i < nr; i++) {
			entry = batch->entries[first + i];
			mutex_lock(&entry->lock);
			entry->prepare_error = ret < 0 ? ret : PAGE_SIZE;
			mutex_unlock(&entry->lock);
			if (ret >= 0)
				atomic64_add(PAGE_SIZE,
					&xfs_wicache_global_device_write_bytes);
		}
	}
	up_read(&wi->visibility_sem);

	for (i = 0; i < batch->nr; i++)
		xfs_wicache_finish_entry(batch->entries[i]);
	kfree(folios);
	atomic_dec(&wi->batch_active);
	if (atomic64_read(&wi->nr_entries))
		xfs_wicache_kick_inode(wi, 0);
	kfree(batch);
}

static void
xfs_wicache_finish_entry(
	struct xfs_wicache_entry *entry)
{
	struct xfs_wicache_inode *wi = entry->wi;
	struct xfs_wicache_mount *wm = wi->wm;
	struct folio		*folio;
	struct file		*owned_file = NULL;
	unsigned long		flush_mask;
	ssize_t			ret;
	bool			folio_charged;
	bool			remove = false, requeue = false;
	unsigned int		i;

	mutex_lock(&entry->lock);
	folio = entry->prepared_folio;
	entry->prepared_folio = NULL;
	folio_charged = entry->prepared_charged;
	entry->prepared_charged = false;
	ret = entry->prepare_error;
	entry->prepare_error = 0;
	flush_mask = entry->flushing_mask;
	mutex_unlock(&entry->lock);

	if (!ret)
		ret = -EIO;

	mutex_lock(&entry->lock);
	if (ret == PAGE_SIZE) {
		for (i = 0; i < XFS_WICACHE_NR_SEGS; i++) {
			if (!entry->flushing[i])
				continue;
			kfree(entry->flushing[i]);
			entry->flushing[i] = NULL;
			xfs_wicache_uncharge(wm, XFS_WICACHE_SEG_SIZE);
		}
		entry->flushing_mask = 0;
		atomic64_add(hweight_long(flush_mask) *
				XFS_WICACHE_SEG_SIZE,
				&xfs_wicache_global_drained_bytes);

		if (entry->transient_base) {
			folio_put(entry->transient_base);
			entry->transient_base = NULL;
			xfs_wicache_uncharge(wm, PAGE_SIZE);
		}
		if (entry->active_mask) {
			if (folio_charged) {
				entry->transient_base = folio;
				folio = NULL;
				folio_charged = false;
			}
			entry->state = XFS_WICACHE_ENTRY_DIRTY;
			requeue = true;
		} else {
			entry->state = XFS_WICACHE_ENTRY_INVALID;
			owned_file = entry->io_file;
			entry->io_file = NULL;
			remove = true;
		}
	} else {
		for (i = 0; i < XFS_WICACHE_NR_SEGS; i++) {
			if (!entry->flushing[i])
				continue;
			if (entry->active[i]) {
				kfree(entry->flushing[i]);
				xfs_wicache_uncharge(wm,
						XFS_WICACHE_SEG_SIZE);
			} else {
				entry->active[i] = entry->flushing[i];
				entry->active_mask |= BIT(i);
			}
			entry->flushing[i] = NULL;
		}
		entry->flushing_mask = 0;
		entry->state = XFS_WICACHE_ENTRY_DIRTY;
		atomic64_inc(&xfs_wicache_global_flush_errors);
		requeue = true;
	}
	entry->batch = NULL;
	mutex_unlock(&entry->lock);

	xfs_wicache_requeue_entry(entry, requeue);
	if (remove)
		xfs_wicache_remove_entry(entry);

	if (owned_file)
		fput(owned_file);
	if (folio) {
		folio_put(folio);
		if (folio_charged)
			xfs_wicache_uncharge(wm, PAGE_SIZE);
	}
	xfs_wicache_entry_put(entry);
}

static void
xfs_wicache_inode_flush_work(
	struct work_struct	*work)
{
	struct xfs_wicache_inode *wi = container_of(to_delayed_work(work),
			struct xfs_wicache_inode, flush_work);
	struct xfs_wicache_batch *batch;
	unsigned int		queued = 0, i;
	unsigned long		next_index;

	if (atomic_inc_return(&wi->batch_active) > xfs_wicache_qd) {
		atomic_dec(&wi->batch_active);
		return;
	}
	batch = kzalloc(struct_size(batch, entries, xfs_wicache_batch),
			XFS_WICACHE_ACCOUNT_GFP(GFP_NOFS));
	if (!batch) {
		atomic_dec(&wi->batch_active);
		return;
	}

	mutex_lock(&wi->scan_lock);
	next_index = wi->flush_cursor;
	while (queued < xfs_wicache_batch) {
		struct xfs_wicache_entry *entry = NULL;
		unsigned long		best = ULONG_MAX;
		unsigned int		shard_id, best_shard = 0;

		rcu_read_lock();
		for (shard_id = 0; shard_id < XFS_WICACHE_NR_SHARDS;
		     shard_id++) {
			struct xfs_wicache_entry *candidate;
			unsigned long index = next_index;

			candidate = xa_find(&wi->shards[shard_id].entries,
					&index, ULONG_MAX, XA_PRESENT);
			if (candidate && index < best) {
				entry = candidate;
				best = index;
				best_shard = shard_id;
			}
		}
		if (entry && !xfs_wicache_entry_get(entry))
			entry = NULL;
		rcu_read_unlock();
		if (!entry)
			break;

		next_index = best + 1;
		spin_lock(&wi->shards[best_shard].dirty_lock);
		if (entry->queued || list_empty(&entry->dirty_node)) {
			spin_unlock(&wi->shards[best_shard].dirty_lock);
			xfs_wicache_entry_put(entry);
			continue;
		}
		list_del_init(&entry->dirty_node);
		entry->queued = true;
		spin_unlock(&wi->shards[best_shard].dirty_lock);
		batch->entries[queued++] = entry;
	}
	wi->flush_cursor = next_index;
	if (!queued)
		wi->flush_cursor = 0;
	mutex_unlock(&wi->scan_lock);

	if (!queued) {
		kfree(batch);
		atomic_dec(&wi->batch_active);
		return;
	}
	batch->nr = queued;
	atomic_set(&batch->prepare_pending, queued);
	INIT_WORK(&batch->dispatch_work, xfs_wicache_batch_dispatch_work);
	for (i = 0; i < queued; i++) {
		mutex_lock(&batch->entries[i]->lock);
		batch->entries[i]->batch = batch;
		mutex_unlock(&batch->entries[i]->lock);
		WARN_ON_ONCE(!queue_work(wi->wm->io_wq,
				&batch->entries[i]->prepare_work));
	}
	if (atomic64_read(&wi->nr_entries) > queued)
		xfs_wicache_kick_inode(wi, 0);
}
