/* SPDX-License-Identifier: GPL-2.0 */

/*
 * WICache - XFS 写隔离缓存。
 */
#ifndef __XFS_WICACHE_H__
#define __XFS_WICACHE_H__

/*
 * WICache 开关宏
 */
#define USE_WICACHE

#include <linux/atomic.h>
#include <linux/gfp_types.h>
#include <linux/list.h>
#include <linux/rcupdate.h>
#include <linux/refcount.h>
#include <linux/rhashtable.h>
#include <linux/spinlock.h>
#include <linux/types.h>
#include <linux/workqueue.h>
#include <linux/xarray.h>

struct folio;
struct iov_iter;
struct kiocb;
struct xfs_inode;
struct xfs_wicache_mount;

/*
 * 每个 inode 内部按 page index 分片。
 *  - XFS_WICACHE_NR_SHARDS: 分片数量，必须是 2 的幂以简化计算。
 *  - XFS_WICACHE_SHARD_MASK: 分片掩码，用于计算 page index 对应的分片 ID。
 * shard_id = page_index & XFS_WICACHE_SHARD_MASK = page_index % XFS_WICACHE_NR_SHARDS
 */
#define XFS_WICACHE_NR_SHARDS		64
#define XFS_WICACHE_SHARD_MASK		(XFS_WICACHE_NR_SHARDS - 1)

/*
 * WICache 数据页和元数据 memcg 计费追加 GFP 标志。
 */
#define XFS_WICACHE_ACCOUNT_GFP(gfp)	((gfp) | __GFP_ACCOUNT)

/*
 * wicache entry 状态机，一个 entry 代表一个 page，大小为 4KB。
 *  - DIRTY    : 该 entry 包含最新的脏数据。
 *  - FLUSHING : 后台线程正在下刷该 entry。
 *  - CLEAN    : entry 已下刷完成，后续可迁移到 page cache 或直接回收。
 *  - INVALID  : entry 已失效。
 */
enum xfs_wicache_entry_state {
	XFS_WICACHE_ENTRY_DIRTY = 0,
	XFS_WICACHE_ENTRY_FLUSHING,
	XFS_WICACHE_ENTRY_CLEAN,
	XFS_WICACHE_ENTRY_INVALID,
};

/*
 * inode WICache 状态机。
 *  - ACTIVE : 该 WICache 仍在 hash 表中有效，可以被读写路径使用。
 *  - DYING  : 该 WICache 正在从 hash 表移除或回收，新的读写路径不能再使用。
 */
enum xfs_wicache_inode_state {
	XFS_WICACHE_INODE_ACTIVE = 0,
	XFS_WICACHE_INODE_DYING,
};

/*
 * WICache entry 结构体，代表一个 page 大小的写隔离缓存数据。
 */
struct xfs_wicache_entry {
	pgoff_t							page_index; // 文件逻辑页号（4KB）
	struct folio					*folio;     // 该 entry 实际存储数据的 folio 结构，大小为 4KB
	u64								seq;        // entry 版本号
	enum xfs_wicache_entry_state	state;      // entry 当前状态

	refcount_t						refcount;   // entry 生命周期引用计数
	spinlock_t						state_lock; // entry 状态锁
	struct list_head				dirty_node; // entry 所属 shard 的脏页链表节点, flush 线程根据该链表进行下刷扫描
	struct rcu_head					rcu;        // 用于 RCU 延迟释放
};

/*
 * inode 内的一个 WICache 分片结构 shard。
 */
struct xfs_wicache_shard {
	struct xarray			entries;     // 存储该 shard 内的所有 entry。key 为 page index, value 为 struct xfs_wicache_entry*
	spinlock_t				dirty_lock;  // 保护 dirty_list 的自旋锁
	struct list_head		dirty_list;  // 记录该 shard 内所有脏 entry 的链表，链表节点为 struct xfs_wicache_entry.dirty_node
	atomic64_t				dirty_bytes; // 该 shard 内所有 entry 的总脏数据量，单位为字节
};

/*
 * 单个 XFS inode 文件的 WICache 结构。
 */
struct xfs_wicache_inode {
	struct xfs_wicache_mount	*wm;							// 挂载器指针
	struct xfs_inode			*ip;							// 对应的 XFS inode 指针
	struct rhash_head			hash_node;						// 把这个 WICache 结构挂入挂载器 rhashtable 的节点

	struct xfs_wicache_shard	shards[XFS_WICACHE_NR_SHARDS];  // 该文件内部的 XFS_WICACHE_NR_SHARDS 个分片 shard
	atomic64_t					dirty_bytes;				    // WICache 中所有 entry 的总脏数据量，单位为字节
	atomic64_t					nr_entries;						// WICache 中当前存在的 entry 数量
	atomic64_t					seq;							// WICache 版本号，每次新增或更新 entry 时递增
	enum xfs_wicache_inode_state	state;							// WICache 当前逻辑状态

	refcount_t					refcount;						// WICache 生命周期引用计数
	struct rcu_head				rcu;							// 用于 RCU 延迟释放
};

/*
 * WICache 挂载器。
 * 在 xfs_mount 挂载时创建，维护整个文件系统范围内的 WICache。
 */
struct xfs_wicache_mount {
	bool						enabled;			// WICache 是否启用
	struct rhashtable			inode_table;		// 维护所有 inode 的 WICache 的哈希表，根据 inode 指针定位对应的 WICache 结构
	struct workqueue_struct		*flush_wq;			// WICache 后台 flush 工作队列
	struct delayed_work			flush_work;			// flush 延迟任务
	atomic64_t					total_dirty_bytes;	// 整个文件系统范围内 WICache 中所有 entry 的总脏数据量，单位为字节
};


// xfs_wicache_mount_alloc 创建并初始化 WICache 挂载器
struct xfs_wicache_mount *xfs_wicache_mount_alloc(gfp_t gfp);
// xfs_wicache_mount_free 销毁 WICache 挂载器，释放相关资源
void xfs_wicache_mount_free(struct xfs_wicache_mount *wm);


// xfs_wicache_inode_lookup 根据 inode 指针在挂载器的哈希表中查找对应的 WICache
struct xfs_wicache_inode *xfs_wicache_inode_lookup(struct xfs_wicache_mount *wm, struct xfs_inode *ip);
// xfs_wicache_inode_get_or_create 根据 inode 指针在挂载器的哈希表中查找对应的 WICache, 不存在则会创建一个新的 WICache 并插入哈希表。内部会给 WICache 元数据分配追加 __GFP_ACCOUNT
struct xfs_wicache_inode *xfs_wicache_inode_get_or_create(struct xfs_wicache_mount *wm, struct xfs_inode *ip, gfp_t gfp);
// xfs_wicache_inode_remove 从挂载器的哈希表中移除对应 inode 的 WICache
int xfs_wicache_inode_remove(struct xfs_wicache_mount *wm, struct xfs_inode *ip);
// xfs_wicache_inode_put 释放 inode WICache 引用, 当引用计数归零时会通过 RCU 机制异步释放 inode WICache 结构
void xfs_wicache_inode_put(struct xfs_wicache_inode *wi);


// xfs_wicache_can_stage 判断一个 write 是否满足进入 WICache, 只允许对齐的普通 buffered write 进入 WICache
bool xfs_wicache_can_stage(struct kiocb *iocb, struct iov_iter *from);
// xfs_wicache_alloc_folio_from_iter 从用户空间 iov_iter 中分配一个 memcg accounted folio 并拷贝用户写入数据, 成功时返回该 folio, 失败时返回 ERR_PTR
struct folio *xfs_wicache_alloc_folio_from_iter(struct iov_iter *from, gfp_t gfp);
// xfs_wicache_store_folio 将一个 folio 存到 inode WICache 中。entry 和 xarray 节点会进入 memcg 计费
int xfs_wicache_store_folio(struct xfs_wicache_inode *wi, pgoff_t page_index, struct folio *folio, gfp_t gfp);


// xfs_wicache_lookup 根据 page index 在 inode WICache 中查找对应的 entry
struct xfs_wicache_entry *xfs_wicache_lookup(struct xfs_wicache_inode *wi, pgoff_t page_index);
// xfs_wicache_copy_to_iter 从一个 WICache entry 中拷贝数据到用户空间 iov_iter
size_t xfs_wicache_copy_to_iter(struct xfs_wicache_entry *entry, size_t offset, size_t bytes, struct iov_iter *to);
// xfs_wicache_invalidate_page 使 inode WICache 中对应 page index 的 entry 失效, 并从 xarray 和脏页链表中移除
void xfs_wicache_invalidate_page(struct xfs_wicache_inode *wi, pgoff_t page_index);
// xfs_wicache_entry_put 释放 entry 引用, 当引用计数归零时会通过 RCU 机制异步释放 entry 结构
void xfs_wicache_entry_put(struct xfs_wicache_entry *entry);

#endif /* __XFS_WICACHE_H__ */
