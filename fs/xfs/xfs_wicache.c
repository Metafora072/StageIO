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
#include <linux/fs.h>
#include <linux/mm.h>
#include <linux/pagemap.h>
#include <linux/slab.h>
#include <linux/uio.h>

/*
 * WICache 挂载器的哈希表 rhashtable 参数。
 */
static const struct rhashtable_params xfs_wicache_inode_hash_params = {
	.key_len				= sizeof(struct xfs_inode *),
	.key_offset				= offsetof(struct xfs_wicache_inode, ip),
	.head_offset			= offsetof(struct xfs_wicache_inode, hash_node),
	.automatic_shrinking	= true,
};

/*
 * 根据文件逻辑页号计算 shard ID。
 */
static inline unsigned int
xfs_wicache_shard_id(
	pgoff_t			page_index)
{
	return page_index & XFS_WICACHE_SHARD_MASK;
}

/*
 * 根据文件逻辑页号返回对应的 shard。
 */
static inline struct xfs_wicache_shard *
xfs_wicache_shard(
	struct xfs_wicache_inode	*wi,
	pgoff_t						page_index)
{
	return &wi->shards[xfs_wicache_shard_id(page_index)];
}

/*
 * 获取 WICache 的 entry 引用。
 *
 * 用于 RCU lookup 后的生命周期保护。若 refcount 已经归零，
 * 说明 entry 正在释放，调用者不能再使用该 entry。
 */
static bool
xfs_wicache_entry_get(
	struct xfs_wicache_entry	*entry)
{
	return refcount_inc_not_zero(&entry->refcount);
}

/*
 * RCU 延迟释放 WICache entry。
 */
static void
xfs_wicache_entry_free_rcu(
	struct rcu_head				*rcu)
{
	struct xfs_wicache_entry	*entry;

	// 根据 rcu_head 获取 entry 指针
	entry = container_of(rcu, struct xfs_wicache_entry, rcu);
	if (entry->folio)
		folio_put(entry->folio);
	kfree(entry);
}

/*
 * 释放 WICache entry 引用, 当引用计数归零时会通过 RCU 机制异步释放 entry。
 */
void
xfs_wicache_entry_put(
	struct xfs_wicache_entry	*entry)
{
	if (entry && refcount_dec_and_test(&entry->refcount))
		call_rcu(&entry->rcu, xfs_wicache_entry_free_rcu);
}

/*
 * 创建 WICache entry。
 */
static struct xfs_wicache_entry *
xfs_wicache_entry_alloc(
	pgoff_t						page_index,
	struct folio				*folio,
	u64							seq,
	gfp_t						gfp)
{
	struct xfs_wicache_entry	*entry;

	// 申请 entry 内存时使用带 __GFP_ACCOUNT 的 GFP 标志，以便 memcg 内存计费
	gfp = XFS_WICACHE_ACCOUNT_GFP(gfp);
	entry = kzalloc(sizeof(*entry), gfp);
	if (!entry)
		return NULL;

	entry->page_index = page_index;
	entry->folio = folio;
	entry->seq = seq;
	// 创建 entry 时默认状态为 DIRTY，因为它是由前台写入触发创建
	entry->state = XFS_WICACHE_ENTRY_DIRTY;
	// 初始化引用计数为 1，表示当前有一个持有者（创建者）在使用该 entry
	refcount_set(&entry->refcount, 1);
	spin_lock_init(&entry->state_lock);
	// 该 entry 刚创建时还没有被加入任何 shard 的脏页链表，所以初始化 dirty_node 为独立状态
	INIT_LIST_HEAD(&entry->dirty_node);

	return entry;
}

/*
 * 初始化 shard。
 */
static void
xfs_wicache_init_shard(
	struct xfs_wicache_shard	*shard)
{
	// 初始化 shard 的 xarray
	xa_init(&shard->entries);
	spin_lock_init(&shard->dirty_lock);
	INIT_LIST_HEAD(&shard->dirty_list);
	atomic64_set(&shard->dirty_bytes, 0);
}

/*
 * 销毁 shard 并释放其中所有 entry。
 */
static void
xfs_wicache_destroy_shard(
	struct xfs_wicache_inode	*wi,
	struct xfs_wicache_shard	*shard)
{
	struct xfs_wicache_entry	*entry;
	unsigned long				index;

	// 遍历 shard 中的所有 entry，释放它们的引用并从脏页链表中移除
	xa_for_each(&shard->entries, index, entry) {
		xa_erase(&shard->entries, index);
		if (!list_empty(&entry->dirty_node))
			list_del_init(&entry->dirty_node);
		atomic64_dec(&wi->nr_entries);
		atomic64_sub(PAGE_SIZE, &wi->dirty_bytes);
		atomic64_sub(PAGE_SIZE, &shard->dirty_bytes);
		atomic64_sub(PAGE_SIZE, &wi->wm->total_dirty_bytes);
		xfs_wicache_entry_put(entry);
	}
	xa_destroy(&shard->entries);
}

/*
 * 创建 inode WICache。
 */
static struct xfs_wicache_inode *
xfs_wicache_inode_alloc(
	struct xfs_wicache_mount	*wm,
	struct xfs_inode			*ip,
	gfp_t						gfp)
{
	struct xfs_wicache_inode	*wi;
	int							i;

	// 申请 inode WICache 内存时使用带 __GFP_ACCOUNT 的 GFP 标志，以便 memcg 内存计费
	gfp = XFS_WICACHE_ACCOUNT_GFP(gfp);
	wi = kzalloc(sizeof(*wi), gfp);
	if (!wi)
		return NULL;

	wi->wm = wm;
	wi->ip = ip;
	// 初始化 WICache 的每个 shard
	for (i = 0; i < XFS_WICACHE_NR_SHARDS; i++)
		xfs_wicache_init_shard(&wi->shards[i]);

	atomic64_set(&wi->dirty_bytes, 0);
	atomic64_set(&wi->nr_entries, 0);
	atomic64_set(&wi->seq, 0);
	// 创建 inode WICache 时默认状态为 ACTIVE，表示它可以被正常使用
	WRITE_ONCE(wi->state, XFS_WICACHE_INODE_ACTIVE);
	// 创建 inode WICache 时默认引用计数为 1，表示当前有一个持有者（创建者）在使用该 WICache
	refcount_set(&wi->refcount, 1);

	return wi;
}

/*
 * 销毁 inode WICache。
 */
static void
xfs_wicache_inode_destroy(
	struct xfs_wicache_inode	*wi)
{
	int							i;

	// 销毁 WICache 中的每个 shard
	for (i = 0; i < XFS_WICACHE_NR_SHARDS; i++)
		xfs_wicache_destroy_shard(wi, &wi->shards[i]);
}

/*
 * RCU 延迟释放 inode WICache。
 */
static void
xfs_wicache_inode_free_rcu(
	struct rcu_head				*rcu)
{
	struct xfs_wicache_inode	*wi;

	wi = container_of(rcu, struct xfs_wicache_inode, rcu);
	xfs_wicache_inode_destroy(wi);
	kfree(wi);
}

/*
 * 释放 inode WICache 引用, 当引用计数归零时会通过 RCU 机制异步释放 inode WICache 结构。
 */
void
xfs_wicache_inode_put(
	struct xfs_wicache_inode	*wi)
{
	if (wi && refcount_dec_and_test(&wi->refcount))
		call_rcu(&wi->rcu, xfs_wicache_inode_free_rcu);
}

/*
 * rhashtable 销毁回调。
 *
 * rhashtable_free_and_destroy() 会对表中每个 inode WICache 调用该
 * 回调。这里释放的是 hash 表持有的长期引用。
 */
static void
xfs_wicache_free_inode_record(
	void						*ptr,
	void						*arg)
{
	struct xfs_wicache_inode	*wi = ptr;

	xfs_wicache_inode_put(wi);
}

/*
 * 创建 WICache 挂载器。
 */
struct xfs_wicache_mount *
xfs_wicache_mount_alloc(
	gfp_t						gfp)
{
	struct xfs_wicache_mount	*wm;
	int							error;

	wm = kzalloc(sizeof(*wm), gfp);
	if (!wm)
		return ERR_PTR(-ENOMEM);

	wm->enabled = true;
	atomic64_set(&wm->total_dirty_bytes, 0);

	error = rhashtable_init(&wm->inode_table, &xfs_wicache_inode_hash_params);
	if (error) {
		kfree(wm);
		return ERR_PTR(error);
	}

	return wm;
}

/*
 * 销毁 WICache 挂载器。
 */
void
xfs_wicache_mount_free(
	struct xfs_wicache_mount	*wm)
{
	if (!wm)
		return;

	if (wm->flush_wq)
		destroy_workqueue(wm->flush_wq);
	rhashtable_free_and_destroy(&wm->inode_table, xfs_wicache_free_inode_record, NULL);
	kfree(wm);
}

/*
 * 从挂载器上查找 inode 对应的 WICache。
 *
 * 查找时会持有引用，调用者必须显式释放该引用
 */
struct xfs_wicache_inode *
xfs_wicache_inode_lookup(
	struct xfs_wicache_mount	*wm,
	struct xfs_inode			*ip)
{
	struct xfs_wicache_inode	*wi;

	if (!wm || !ip)
		return NULL;

	// 注意访问 rhashtable 时需要 RCU 保护
	rcu_read_lock();
	// 查挂载器 WICache 哈希表
	wi = rhashtable_lookup_fast(&wm->inode_table, &ip, xfs_wicache_inode_hash_params);
	if (wi) {
	 	// 成功查到后获取引用，保护 WICache 生命周期。
	    // 如果 refcount 已经归零，说明 WICache 正在释放，调用者不能再使用。
		if (!refcount_inc_not_zero(&wi->refcount)) {
			wi = NULL;
		} else if (READ_ONCE(wi->state) != XFS_WICACHE_INODE_ACTIVE) {
			// refcount 只保证对象内存仍然有效，不保证对象仍然处于业务有效状态。
			// 因此拿到引用后还需要检查 inode WICache 是否仍为 ACTIVE
			// 如果不是 ACTIVE，说明它正在被移除，调用者也不能再使用。
			xfs_wicache_inode_put(wi);
			wi = NULL;
		}
	}
	rcu_read_unlock();

	return wi;
}

/*
 * 获取 inode WICache，不存在则创建并插入挂载器哈希表。
 */
struct xfs_wicache_inode *
xfs_wicache_inode_get_or_create(
	struct xfs_wicache_mount	*wm,
	struct xfs_inode			*ip,
	gfp_t						gfp)
{
	struct xfs_wicache_inode	*wi;
	struct xfs_wicache_inode	*old;

	if (!wm || !ip)
		return ERR_PTR(-EINVAL);

	wi = xfs_wicache_inode_lookup(wm, ip);
	if (wi)
		return wi;

	wi = xfs_wicache_inode_alloc(wm, ip, gfp);
	if (!wi)
		return ERR_PTR(-ENOMEM);

	// 新对象需要两个引用：一个给调用者，一个给挂载级 hash 表。
	// alloc 函数已经为调用者的引用初始化为 1，这里再增加一个引用给 hash 表。
	refcount_inc(&wi->refcount);
	// rhashtable_lookup_get_insert_fast() 会尝试把新对象插入 hash 表
	// 如果发现已有对象满足条件，则返回该对象并不插入新对象。
	old = rhashtable_lookup_get_insert_fast(&wm->inode_table, &wi->hash_node, xfs_wicache_inode_hash_params);
	if (IS_ERR(old)) {
		xfs_wicache_inode_put(wi);
		xfs_wicache_inode_put(wi);
		return ERR_CAST(old);
	}
	if (old) { // 存在旧对象时，丢弃新对象，使用旧对象
		if (!refcount_inc_not_zero(&old->refcount)) {
			old = ERR_PTR(-ENOENT);
		} else if (READ_ONCE(old->state) != XFS_WICACHE_INODE_ACTIVE) {
			xfs_wicache_inode_put(old);
			old = ERR_PTR(-ENOENT);
		}
		xfs_wicache_inode_put(wi);
		xfs_wicache_inode_put(wi);
		return old;
	}

	return wi;
}

/*
 * 从挂载器哈希表移除 inode WICache。
 */
int
xfs_wicache_inode_remove(
	struct xfs_wicache_mount	*wm,
	struct xfs_inode			*ip)
{
	struct xfs_wicache_inode	*wi;
	int							error;

	wi = xfs_wicache_inode_lookup(wm, ip);
	if (!wi)
		return -ENOENT;

	// 先标记 DYING，使后续 lookup 即使短暂读到该对象，也会在拿到引用后因为状态检查失败而放弃使用。
	WRITE_ONCE(wi->state, XFS_WICACHE_INODE_DYING);

	error = rhashtable_remove_fast(&wm->inode_table, &wi->hash_node, xfs_wicache_inode_hash_params);
	if (!error){
		// 释放 hash 表持有的引用
		xfs_wicache_inode_put(wi);
	} else {
		// 移除失败时恢复 ACTIVE 状态
		WRITE_ONCE(wi->state, XFS_WICACHE_INODE_ACTIVE);
	}
	// 释放调用 lookup 时获取的引用
	xfs_wicache_inode_put(wi);		

	return error;
}

/*
 * 判断 write 是否可以进入 WICache 逻辑。
 * 
 * 当前只接受普通 buffered write，并要求 offset 和长度均按 4k 对齐,
 * direct、sync、append、nowait 等路径暂时回退到原生 XFS。
 */
bool
xfs_wicache_can_stage(
	struct kiocb				*iocb,
	struct iov_iter				*from)
{
	size_t						count;

	if (!iocb || !from)
		return false;

	count = iov_iter_count(from);
	if (!count)
		return false;

	if (iocb->ki_flags & (IOCB_DIRECT | IOCB_DSYNC | IOCB_SYNC |
			      IOCB_APPEND | IOCB_NOWAIT))
		return false;

	return IS_ALIGNED(iocb->ki_pos, PAGE_SIZE) &&
	       IS_ALIGNED(count, PAGE_SIZE);
}

/*
 * 从用户写入 iov_iter 中分配一个 4k folio，并把数据拷贝到该 folio 中。
 */
struct folio *
xfs_wicache_alloc_folio_from_iter(
	struct iov_iter				*from,
	gfp_t						gfp)
{
	struct folio				*folio;
	size_t						copied;

	gfp = XFS_WICACHE_ACCOUNT_GFP(gfp);
	folio = folio_alloc(gfp, 0);
	if (!folio)
		return ERR_PTR(-ENOMEM);

	copied = copy_page_from_iter(&folio->page, 0, PAGE_SIZE, from);
	if (copied != PAGE_SIZE) {
		iov_iter_revert(from, copied);
		folio_put(folio);
		return ERR_PTR(-EFAULT);
	}

	return folio;
}

/*
 * 将指定 folio 存入 inode WICache。
 */
int
xfs_wicache_store_folio(
	struct xfs_wicache_inode	*wi,
	pgoff_t						page_index,
	struct folio				*folio,
	gfp_t						gfp)
{
	struct xfs_wicache_shard	*shard;
	struct xfs_wicache_entry	*entry;
	struct xfs_wicache_entry	*old;
	u64							seq;

	if (!wi || !folio)
		return -EINVAL;

	gfp = XFS_WICACHE_ACCOUNT_GFP(gfp);
	// folio 被封装到 entry 中, WICache 递增版本号以标识数据更新
	// WICache 的版本号也会被携带进该 entry，标识该 entry 的版本
	seq = atomic64_inc_return(&wi->seq);
	entry = xfs_wicache_entry_alloc(page_index, folio, seq, gfp);
	if (!entry)
		return -ENOMEM;

	// 将 entry 存入对应 shard 的 xarray 中，key 为 page_index
	shard = xfs_wicache_shard(wi, page_index);
	// xa_store 返回旧 entry 的指针，或者 ERR_PTR 错误码
	// 如果 old 不为 NULL 且不是 ERR_PTR，说明发生了覆盖j
	old = xa_store(&shard->entries, page_index, entry, gfp);
	if (xa_is_err(old)) {
		entry->folio = NULL;
		xfs_wicache_entry_put(entry);
		return xa_err(old);
	}

	// old entry 存在，说明发生覆盖写，需要从 dirty_list 中摘除旧 entry
	// 新 entry 无论如何都要加入 dirty_list
	spin_lock(&shard->dirty_lock);
	if (old && !list_empty(&old->dirty_node))
		list_del_init(&old->dirty_node);
	list_add_tail(&entry->dirty_node, &shard->dirty_list);
	spin_unlock(&shard->dirty_lock);

	// 如果是覆盖写，脏数据量不变
	// 如果是新增写，需要递增 inode WICache、shard、挂载器的脏数据统计
	if (!old) {
		atomic64_inc(&wi->nr_entries);
		atomic64_add(PAGE_SIZE, &wi->dirty_bytes);
		atomic64_add(PAGE_SIZE, &shard->dirty_bytes);
		atomic64_add(PAGE_SIZE, &wi->wm->total_dirty_bytes);
	}

	// 释放 old entry 的引用
	if (old)
		xfs_wicache_entry_put(old);

	return 0;
}

/*
 * 查找 page_index 对应的 WICache entry。
 */
struct xfs_wicache_entry *
xfs_wicache_lookup(
	struct xfs_wicache_inode	*wi,
	pgoff_t						page_index)
{
	struct xfs_wicache_shard	*shard;
	struct xfs_wicache_entry	*entry;

	if (!wi)
		return NULL;

	shard = xfs_wicache_shard(wi, page_index);

	rcu_read_lock();
	entry = xa_load(&shard->entries, page_index);
	if (entry && READ_ONCE(entry->state) == XFS_WICACHE_ENTRY_INVALID)
		entry = NULL;
	if (entry) {
		if (!xfs_wicache_entry_get(entry)) {
			entry = NULL;
		} else if (READ_ONCE(entry->state) == XFS_WICACHE_ENTRY_INVALID) {
			// entry 的 state 可能在获取引用前后发生变化，因此需要再次检查 state 是否有效。
			xfs_wicache_entry_put(entry);
			entry = NULL;
		}
	}
	rcu_read_unlock();

	return entry;
}

/*
 * 将 entry 中的数据拷贝到 iov_iter, 返回实际拷贝的字节数。
 */
size_t
xfs_wicache_copy_to_iter(
	struct xfs_wicache_entry	*entry,
	size_t						offset,
	size_t						bytes,
	struct iov_iter				*to)
{
	if (!entry || !entry->folio || !to || offset >= PAGE_SIZE)
		return 0;

	bytes = min_t(size_t, bytes, PAGE_SIZE - offset);
	return copy_folio_to_iter(entry->folio, offset, bytes, to);
}

/*
 * 失效并移除指定 page 的 WICache entry。
 */
void
xfs_wicache_invalidate_page(
	struct xfs_wicache_inode	*wi,
	pgoff_t						page_index)
{
	struct xfs_wicache_shard	*shard;
	struct xfs_wicache_entry	*entry;

	if (!wi)
		return;

	shard = xfs_wicache_shard(wi, page_index);
	// 将 entry 从 xarray 中移除，返回被移除的 entry 指针
	entry = xa_erase(&shard->entries, page_index);
	if (!entry)
		return;

	// 标记 entry 无效
	WRITE_ONCE(entry->state, XFS_WICACHE_ENTRY_INVALID);

	// 从 dirty_list 中移除 entry
	spin_lock(&shard->dirty_lock);
	if (!list_empty(&entry->dirty_node))
		list_del_init(&entry->dirty_node);
	spin_unlock(&shard->dirty_lock);

	// 更新脏数据统计并释放 entry 引用
	atomic64_dec(&wi->nr_entries);
	atomic64_sub(PAGE_SIZE, &wi->dirty_bytes);
	atomic64_sub(PAGE_SIZE, &shard->dirty_bytes);
	atomic64_sub(PAGE_SIZE, &wi->wm->total_dirty_bytes);
	xfs_wicache_entry_put(entry);
}
