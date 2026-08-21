// SPDX-License-Identifier: GPL-2.0

#include "xfs.h"
#include "xfs_fs.h"
#include "xfs_shared.h"
#include "xfs_format.h"
#include "xfs_log_format.h"
#include "xfs_trans_resv.h"
#include "xfs_mount.h"
#include "xfs_inode.h"
#include "xfs_stageio.h"

#include <linux/err.h>
#include <linux/bvec.h>
#include <linux/file.h>
#include <linux/fs.h>
#include <linux/highmem.h>
#include <linux/jiffies.h>
#include <linux/ktime.h>
#include <linux/log2.h>
#include <linux/mm.h>
#include <linux/moduleparam.h>
#include <linux/pagemap.h>
#include <linux/slab.h>
#include <linux/uio.h>
#include <linux/vmalloc.h>

static bool xfs_stageio_enable;
static unsigned int xfs_stageio_batch = 32;
static unsigned int xfs_stageio_qd = 16;
static unsigned int xfs_stageio_delay_ms = 1;
static unsigned long xfs_stageio_high_bytes = 256UL << 20;
static unsigned long xfs_stageio_io_unit = PAGE_SIZE;
static bool xfs_stageio_user_dio = true;
static bool xfs_stageio_clean_handoff;
static bool xfs_stageio_clean_handoff_async;
static bool xfs_stageio_clean_handoff_writebehind;
static bool xfs_stageio_clean_recent;
static bool xfs_stageio_clean_access_aware = true;
static bool xfs_stageio_clean_reuse = true;
static unsigned long xfs_stageio_clean_recent_inode_bytes = 32UL << 20;
static unsigned long xfs_stageio_clean_reuse_bytes = 256UL << 20;
static unsigned int xfs_stageio_raw_order = 4;

static atomic64_t xfs_stageio_global_mem_bytes;
static atomic64_t xfs_stageio_global_peak_bytes;
static atomic64_t xfs_stageio_global_accepted_bytes;
static atomic64_t xfs_stageio_global_drained_bytes;
static atomic64_t xfs_stageio_global_base_reads;
static atomic64_t xfs_stageio_global_cache_bases;
static atomic64_t xfs_stageio_global_full_cancels;
static atomic64_t xfs_stageio_global_device_read_bytes;
static atomic64_t xfs_stageio_global_device_write_bytes;
static atomic64_t xfs_stageio_global_flush_errors;
static atomic64_t xfs_stageio_global_batches;
static atomic64_t xfs_stageio_global_batch_pages;
static atomic64_t xfs_stageio_global_dio_read_calls;
static atomic64_t xfs_stageio_global_dio_read_pages;
static atomic64_t xfs_stageio_global_dio_write_calls;
static atomic64_t xfs_stageio_global_dio_write_pages;
static atomic64_t xfs_stageio_global_dio_current;
static atomic64_t xfs_stageio_global_dio_peak;
static atomic64_t xfs_stageio_global_dio_weighted_ns;
static atomic64_t xfs_stageio_global_dio_busy_ns;
static DEFINE_SPINLOCK(xfs_stageio_dio_stats_lock);
static u64 xfs_stageio_dio_stats_last_ns;
static atomic64_t xfs_stageio_global_prepare_queue_ns;
static atomic64_t xfs_stageio_global_prepare_ns;
static atomic64_t xfs_stageio_global_dispatch_queue_ns;
static atomic64_t xfs_stageio_global_dio_read_ns;
static atomic64_t xfs_stageio_global_visibility_wait_ns;
static atomic64_t xfs_stageio_global_dio_write_ns;
static atomic64_t xfs_stageio_global_batch_active_peak;
static atomic64_t xfs_stageio_global_scan_ns;
static atomic64_t xfs_stageio_global_dispatch_ns;
static atomic64_t xfs_stageio_global_finish_ns;
static atomic64_t xfs_stageio_global_entry_bytes;
static atomic64_t xfs_stageio_global_entry_peak_bytes;
static atomic64_t xfs_stageio_global_region_bytes;
static atomic64_t xfs_stageio_global_region_peak_bytes;
static atomic64_t xfs_stageio_global_front_iolock_ns;
static atomic64_t xfs_stageio_global_front_iolock_calls;
static atomic64_t xfs_stageio_global_front_iolock_max_ns;
static atomic64_t xfs_stageio_global_mapping_check_ns;
static atomic64_t xfs_stageio_global_owner_file_refs;
static atomic64_t xfs_stageio_global_temp_file_refs;
static atomic64_t xfs_stageio_global_middle_calls;
static atomic64_t xfs_stageio_global_middle_bytes;
static atomic64_t xfs_stageio_global_middle_prepare_ns;
static atomic64_t xfs_stageio_global_middle_bvec_ns;
static atomic64_t xfs_stageio_global_middle_dio_ns;
static atomic64_t xfs_stageio_global_middle_release_ns;
static atomic64_t xfs_stageio_global_middle_copy_calls;
static atomic64_t xfs_stageio_global_middle_copy_bytes;
static atomic64_t xfs_stageio_global_middle_copy_ns;
static atomic64_t xfs_stageio_global_middle_direct_calls;
static atomic64_t xfs_stageio_global_middle_direct_bytes;
static atomic64_t xfs_stageio_global_middle_direct_dio_ns;
static atomic64_t xfs_stageio_global_middle_staged_calls;
static atomic64_t xfs_stageio_global_middle_staged_bytes;
static atomic64_t xfs_stageio_global_middle_staged_dio_ns;
static atomic64_t xfs_stageio_global_middle_async_submit_ns;
static atomic64_t xfs_stageio_global_middle_async_wait_ns;
static atomic64_t xfs_stageio_global_clean_handoff_bytes;
static atomic64_t xfs_stageio_global_clean_handoff_pages;
static atomic64_t xfs_stageio_global_clean_handoff_conflicts;
static atomic64_t xfs_stageio_global_clean_handoff_ns;
static atomic64_t xfs_stageio_global_async_handoff_queued_bytes;
static atomic64_t xfs_stageio_global_async_handoff_completed_bytes;
static atomic64_t xfs_stageio_global_async_handoff_pending_bytes;
static atomic64_t xfs_stageio_global_async_handoff_pending_peak_bytes;
static atomic64_t xfs_stageio_global_clean_recent_retained_bytes;
static atomic64_t xfs_stageio_global_clean_recent_evicted_bytes;
static atomic64_t xfs_stageio_global_clean_recent_current_bytes;
static atomic64_t xfs_stageio_global_clean_recent_hit_bytes;
static atomic64_t xfs_stageio_global_clean_recent_miss_reads;
static atomic64_t xfs_stageio_global_clean_recent_peak_bytes;
static atomic64_t xfs_stageio_global_clean_first_accesses;
static atomic64_t xfs_stageio_global_clean_promotions;
static atomic64_t xfs_stageio_global_clean_rereferences;
static atomic64_t xfs_stageio_global_clean_second_chances;
static atomic64_t xfs_stageio_global_clean_forced_evictions;
static atomic64_t xfs_stageio_global_clean_reuse_current_bytes;
static atomic64_t xfs_stageio_global_clean_reused_bytes;
static atomic64_t xfs_stageio_global_clean_reuse_peak_bytes;
static atomic64_t xfs_stageio_global_fragment_calls;
static atomic64_t xfs_stageio_global_fragment_bytes;
static atomic64_t xfs_stageio_global_fragment_ns;
static atomic64_t xfs_stageio_global_small_write_calls;
static atomic64_t xfs_stageio_global_small_write_bytes;
static atomic64_t xfs_stageio_global_small_write_ns;
static atomic64_t xfs_stageio_global_dio_pool_bytes;
static atomic64_t xfs_stageio_global_dio_pool_wait_calls;
static atomic64_t xfs_stageio_global_dio_pool_wait_ns;
static atomic64_t xfs_stageio_global_inode_records;
static atomic64_t xfs_stageio_global_inode_records_peak;
static atomic64_t xfs_stageio_global_range_drain_calls;
static atomic64_t xfs_stageio_global_range_drain_entries;
static atomic64_t xfs_stageio_global_range_drain_ns;
static atomic64_t xfs_stageio_global_range_drain_max_ns;
static atomic64_t xfs_stageio_global_prepare_ring_enqueues;
static atomic64_t xfs_stageio_global_prepare_ring_dequeues;
static atomic64_t xfs_stageio_global_prepare_ring_full;
static atomic64_t xfs_stageio_global_prepare_ring_peak;
static atomic64_t xfs_stageio_global_persist_ring_enqueues;
static atomic64_t xfs_stageio_global_persist_ring_dequeues;
static atomic64_t xfs_stageio_global_persist_ring_full;
static atomic64_t xfs_stageio_global_persist_ring_peak;
static atomic64_t xfs_stageio_global_prepare_workers_active;
static atomic64_t xfs_stageio_global_prepare_workers_peak;
static atomic64_t xfs_stageio_global_persist_workers_active;
static atomic64_t xfs_stageio_global_persist_workers_peak;

static bool xfs_stageio_region_range_has_entry(
		struct xfs_stageio_inode *wi, pgoff_t first, pgoff_t last);

module_param_named(stageio_enable, xfs_stageio_enable, bool, 0444);
MODULE_PARM_DESC(stageio_enable, "Enable experimental sparse write overlay");
module_param_named(stageio_batch, xfs_stageio_batch, uint, 0444);
MODULE_PARM_DESC(stageio_batch, "Maximum pages admitted by one flush scan");
module_param_named(stageio_qd, xfs_stageio_qd, uint, 0444);
MODULE_PARM_DESC(stageio_qd, "Maximum active flush batches per inode");
module_param_named(stageio_delay_ms, xfs_stageio_delay_ms, uint, 0444);
MODULE_PARM_DESC(stageio_delay_ms, "Dirty accumulation delay in milliseconds");
module_param_named(stageio_high_bytes, xfs_stageio_high_bytes, ulong, 0444);
MODULE_PARM_DESC(stageio_high_bytes, "Sparse overlay payload high watermark");
module_param_named(stageio_io_unit, xfs_stageio_io_unit, ulong, 0444);
MODULE_PARM_DESC(stageio_io_unit, "Buffered fragment and direct I/O split unit");
module_param_named(stageio_user_dio, xfs_stageio_user_dio, bool, 0444);
MODULE_PARM_DESC(stageio_user_dio, "Use aligned user iterators for middle DIO");
module_param_named(stageio_clean_handoff, xfs_stageio_clean_handoff, bool, 0444);
MODULE_PARM_DESC(stageio_clean_handoff,
		"Keep successful middle DIO folios as clean page cache");
module_param_named(stageio_clean_handoff_async,
		xfs_stageio_clean_handoff_async, bool, 0444);
MODULE_PARM_DESC(stageio_clean_handoff_async,
		"Pipeline clean handoff folios through asynchronous middle DIO");
module_param_named(stageio_clean_handoff_writebehind,
		xfs_stageio_clean_handoff_writebehind, bool, 0444);
MODULE_PARM_DESC(stageio_clean_handoff_writebehind,
		"Return after submitting ordered clean handoff DIO");
module_param_named(stageio_clean_recent, xfs_stageio_clean_recent,
		bool, 0444);
MODULE_PARM_DESC(stageio_clean_recent,
		"Retain completed DIO folios in the StageIO XArray");
module_param_named(stageio_clean_access_aware,
		xfs_stageio_clean_access_aware, bool, 0444);
MODULE_PARM_DESC(stageio_clean_access_aware,
		"Use two-hit admission and protected CLOCK clean retention");
module_param_named(stageio_clean_reuse, xfs_stageio_clean_reuse,
		bool, 0444);
MODULE_PARM_DESC(stageio_clean_reuse,
		"Reuse evicted clean folios for subsequent staged writes");
module_param_named(stageio_clean_recent_inode_bytes,
		xfs_stageio_clean_recent_inode_bytes, ulong, 0444);
MODULE_PARM_DESC(stageio_clean_recent_inode_bytes,
		"Maximum bytes retained per inode as recent clean DIO folios");
module_param_named(stageio_clean_reuse_bytes,
		xfs_stageio_clean_reuse_bytes, ulong, 0444);
MODULE_PARM_DESC(stageio_clean_reuse_bytes,
		"Maximum bytes retained as reusable clean folios per mount");
module_param_named(stageio_raw_order, xfs_stageio_raw_order, uint, 0444);
MODULE_PARM_DESC(stageio_raw_order,
		"Folio order for naturally aligned full-range staging");

unsigned long
xfs_stageio_io_unit_bytes(void)
{
	if (xfs_stageio_io_unit < PAGE_SIZE ||
	    !is_power_of_2(xfs_stageio_io_unit))
		return PAGE_SIZE;
	return xfs_stageio_io_unit;
}

bool
xfs_stageio_user_dio_enabled(void)
{
	return xfs_stageio_user_dio;
}

bool
xfs_stageio_clean_handoff_enabled(void)
{
	return xfs_stageio_clean_handoff;
}

bool
xfs_stageio_clean_handoff_async_enabled(void)
{
	return xfs_stageio_clean_handoff_async;
}

bool
xfs_stageio_clean_handoff_writebehind_enabled(void)
{
	return xfs_stageio_clean_handoff_writebehind;
}

static int
xfs_stageio_atomic64_get(
	char			*buffer,
	const struct kernel_param *kp)
{
	return sysfs_emit(buffer, "%lld\n", atomic64_read(kp->arg));
}

static const struct kernel_param_ops xfs_stageio_atomic64_ops = {
	.get = xfs_stageio_atomic64_get,
};

module_param_cb(stageio_dirty_bytes, &xfs_stageio_atomic64_ops,
		&xfs_stageio_global_mem_bytes, 0444);
module_param_cb(stageio_peak_bytes, &xfs_stageio_atomic64_ops,
		&xfs_stageio_global_peak_bytes, 0444);
module_param_cb(stageio_accepted_bytes, &xfs_stageio_atomic64_ops,
		&xfs_stageio_global_accepted_bytes, 0444);
module_param_cb(stageio_drained_bytes, &xfs_stageio_atomic64_ops,
		&xfs_stageio_global_drained_bytes, 0444);
module_param_cb(stageio_base_reads, &xfs_stageio_atomic64_ops,
		&xfs_stageio_global_base_reads, 0444);
module_param_cb(stageio_cache_bases, &xfs_stageio_atomic64_ops,
		&xfs_stageio_global_cache_bases, 0444);
module_param_cb(stageio_full_cancels, &xfs_stageio_atomic64_ops,
		&xfs_stageio_global_full_cancels, 0444);
module_param_cb(stageio_device_read_bytes, &xfs_stageio_atomic64_ops,
		&xfs_stageio_global_device_read_bytes, 0444);
module_param_cb(stageio_device_write_bytes, &xfs_stageio_atomic64_ops,
		&xfs_stageio_global_device_write_bytes, 0444);
module_param_cb(stageio_flush_errors, &xfs_stageio_atomic64_ops,
		&xfs_stageio_global_flush_errors, 0444);
module_param_cb(stageio_batches, &xfs_stageio_atomic64_ops,
		&xfs_stageio_global_batches, 0444);
module_param_cb(stageio_batch_pages, &xfs_stageio_atomic64_ops,
		&xfs_stageio_global_batch_pages, 0444);
module_param_cb(stageio_dio_read_calls, &xfs_stageio_atomic64_ops,
		&xfs_stageio_global_dio_read_calls, 0444);
module_param_cb(stageio_dio_read_pages, &xfs_stageio_atomic64_ops,
		&xfs_stageio_global_dio_read_pages, 0444);
module_param_cb(stageio_dio_write_calls, &xfs_stageio_atomic64_ops,
		&xfs_stageio_global_dio_write_calls, 0444);
module_param_cb(stageio_dio_write_pages, &xfs_stageio_atomic64_ops,
		&xfs_stageio_global_dio_write_pages, 0444);
module_param_cb(stageio_dio_peak, &xfs_stageio_atomic64_ops,
		&xfs_stageio_global_dio_peak, 0444);
module_param_cb(stageio_dio_weighted_ns, &xfs_stageio_atomic64_ops,
		&xfs_stageio_global_dio_weighted_ns, 0444);
module_param_cb(stageio_dio_busy_ns, &xfs_stageio_atomic64_ops,
		&xfs_stageio_global_dio_busy_ns, 0444);
module_param_cb(stageio_prepare_queue_ns, &xfs_stageio_atomic64_ops,
		&xfs_stageio_global_prepare_queue_ns, 0444);
module_param_cb(stageio_prepare_ns, &xfs_stageio_atomic64_ops,
		&xfs_stageio_global_prepare_ns, 0444);
module_param_cb(stageio_dispatch_queue_ns, &xfs_stageio_atomic64_ops,
		&xfs_stageio_global_dispatch_queue_ns, 0444);
module_param_cb(stageio_dio_read_ns, &xfs_stageio_atomic64_ops,
		&xfs_stageio_global_dio_read_ns, 0444);
module_param_cb(stageio_visibility_wait_ns, &xfs_stageio_atomic64_ops,
		&xfs_stageio_global_visibility_wait_ns, 0444);
module_param_cb(stageio_dio_write_ns, &xfs_stageio_atomic64_ops,
		&xfs_stageio_global_dio_write_ns, 0444);
module_param_cb(stageio_batch_active_peak, &xfs_stageio_atomic64_ops,
		&xfs_stageio_global_batch_active_peak, 0444);
module_param_cb(stageio_scan_ns, &xfs_stageio_atomic64_ops,
		&xfs_stageio_global_scan_ns, 0444);
module_param_cb(stageio_dispatch_ns, &xfs_stageio_atomic64_ops,
		&xfs_stageio_global_dispatch_ns, 0444);
module_param_cb(stageio_finish_ns, &xfs_stageio_atomic64_ops,
		&xfs_stageio_global_finish_ns, 0444);
module_param_cb(stageio_entry_bytes, &xfs_stageio_atomic64_ops,
		&xfs_stageio_global_entry_bytes, 0444);
module_param_cb(stageio_entry_peak_bytes, &xfs_stageio_atomic64_ops,
		&xfs_stageio_global_entry_peak_bytes, 0444);
module_param_cb(stageio_region_bytes, &xfs_stageio_atomic64_ops,
		&xfs_stageio_global_region_bytes, 0444);
module_param_cb(stageio_region_peak_bytes, &xfs_stageio_atomic64_ops,
		&xfs_stageio_global_region_peak_bytes, 0444);
module_param_cb(stageio_front_iolock_ns, &xfs_stageio_atomic64_ops,
		&xfs_stageio_global_front_iolock_ns, 0444);
module_param_cb(stageio_front_iolock_calls, &xfs_stageio_atomic64_ops,
		&xfs_stageio_global_front_iolock_calls, 0444);
module_param_cb(stageio_front_iolock_max_ns, &xfs_stageio_atomic64_ops,
		&xfs_stageio_global_front_iolock_max_ns, 0444);
module_param_cb(stageio_mapping_check_ns, &xfs_stageio_atomic64_ops,
		&xfs_stageio_global_mapping_check_ns, 0444);
module_param_cb(stageio_owner_file_refs, &xfs_stageio_atomic64_ops,
		&xfs_stageio_global_owner_file_refs, 0444);
module_param_cb(stageio_temp_file_refs, &xfs_stageio_atomic64_ops,
		&xfs_stageio_global_temp_file_refs, 0444);
module_param_cb(stageio_middle_calls, &xfs_stageio_atomic64_ops,
		&xfs_stageio_global_middle_calls, 0444);
module_param_cb(stageio_middle_bytes, &xfs_stageio_atomic64_ops,
		&xfs_stageio_global_middle_bytes, 0444);
module_param_cb(stageio_middle_prepare_ns, &xfs_stageio_atomic64_ops,
		&xfs_stageio_global_middle_prepare_ns, 0444);
module_param_cb(stageio_middle_bvec_ns, &xfs_stageio_atomic64_ops,
		&xfs_stageio_global_middle_bvec_ns, 0444);
module_param_cb(stageio_middle_dio_ns, &xfs_stageio_atomic64_ops,
		&xfs_stageio_global_middle_dio_ns, 0444);
module_param_cb(stageio_middle_release_ns, &xfs_stageio_atomic64_ops,
		&xfs_stageio_global_middle_release_ns, 0444);
module_param_cb(stageio_middle_copy_calls, &xfs_stageio_atomic64_ops,
		&xfs_stageio_global_middle_copy_calls, 0444);
module_param_cb(stageio_middle_copy_bytes, &xfs_stageio_atomic64_ops,
		&xfs_stageio_global_middle_copy_bytes, 0444);
module_param_cb(stageio_middle_copy_ns, &xfs_stageio_atomic64_ops,
		&xfs_stageio_global_middle_copy_ns, 0444);
module_param_cb(stageio_middle_direct_calls, &xfs_stageio_atomic64_ops,
		&xfs_stageio_global_middle_direct_calls, 0444);
module_param_cb(stageio_middle_direct_bytes, &xfs_stageio_atomic64_ops,
		&xfs_stageio_global_middle_direct_bytes, 0444);
module_param_cb(stageio_middle_direct_dio_ns, &xfs_stageio_atomic64_ops,
		&xfs_stageio_global_middle_direct_dio_ns, 0444);
module_param_cb(stageio_middle_staged_calls, &xfs_stageio_atomic64_ops,
		&xfs_stageio_global_middle_staged_calls, 0444);
module_param_cb(stageio_middle_staged_bytes, &xfs_stageio_atomic64_ops,
		&xfs_stageio_global_middle_staged_bytes, 0444);
module_param_cb(stageio_middle_staged_dio_ns, &xfs_stageio_atomic64_ops,
		&xfs_stageio_global_middle_staged_dio_ns, 0444);
module_param_cb(stageio_middle_async_submit_ns, &xfs_stageio_atomic64_ops,
		&xfs_stageio_global_middle_async_submit_ns, 0444);
module_param_cb(stageio_middle_async_wait_ns, &xfs_stageio_atomic64_ops,
		&xfs_stageio_global_middle_async_wait_ns, 0444);
module_param_cb(stageio_clean_handoff_bytes, &xfs_stageio_atomic64_ops,
		&xfs_stageio_global_clean_handoff_bytes, 0444);
module_param_cb(stageio_clean_handoff_pages, &xfs_stageio_atomic64_ops,
		&xfs_stageio_global_clean_handoff_pages, 0444);
module_param_cb(stageio_clean_handoff_conflicts, &xfs_stageio_atomic64_ops,
		&xfs_stageio_global_clean_handoff_conflicts, 0444);
module_param_cb(stageio_clean_handoff_ns, &xfs_stageio_atomic64_ops,
		&xfs_stageio_global_clean_handoff_ns, 0444);
module_param_cb(stageio_async_handoff_queued_bytes,
		&xfs_stageio_atomic64_ops,
		&xfs_stageio_global_async_handoff_queued_bytes, 0444);
module_param_cb(stageio_async_handoff_completed_bytes,
		&xfs_stageio_atomic64_ops,
		&xfs_stageio_global_async_handoff_completed_bytes, 0444);
module_param_cb(stageio_async_handoff_pending_bytes,
		&xfs_stageio_atomic64_ops,
		&xfs_stageio_global_async_handoff_pending_bytes, 0444);
module_param_cb(stageio_async_handoff_pending_peak_bytes,
		&xfs_stageio_atomic64_ops,
		&xfs_stageio_global_async_handoff_pending_peak_bytes, 0444);
module_param_cb(stageio_clean_recent_retained_bytes,
		&xfs_stageio_atomic64_ops,
		&xfs_stageio_global_clean_recent_retained_bytes, 0444);
module_param_cb(stageio_clean_recent_evicted_bytes,
		&xfs_stageio_atomic64_ops,
		&xfs_stageio_global_clean_recent_evicted_bytes, 0444);
module_param_cb(stageio_clean_recent_current_bytes,
		&xfs_stageio_atomic64_ops,
		&xfs_stageio_global_clean_recent_current_bytes, 0444);
module_param_cb(stageio_clean_recent_hit_bytes,
		&xfs_stageio_atomic64_ops,
		&xfs_stageio_global_clean_recent_hit_bytes, 0444);
module_param_cb(stageio_clean_recent_miss_reads,
		&xfs_stageio_atomic64_ops,
		&xfs_stageio_global_clean_recent_miss_reads, 0444);
module_param_cb(stageio_clean_recent_peak_bytes,
		&xfs_stageio_atomic64_ops,
		&xfs_stageio_global_clean_recent_peak_bytes, 0444);
module_param_cb(stageio_clean_first_accesses,
		&xfs_stageio_atomic64_ops,
		&xfs_stageio_global_clean_first_accesses, 0444);
module_param_cb(stageio_clean_promotions, &xfs_stageio_atomic64_ops,
		&xfs_stageio_global_clean_promotions, 0444);
module_param_cb(stageio_clean_rereferences, &xfs_stageio_atomic64_ops,
		&xfs_stageio_global_clean_rereferences, 0444);
module_param_cb(stageio_clean_second_chances,
		&xfs_stageio_atomic64_ops,
		&xfs_stageio_global_clean_second_chances, 0444);
module_param_cb(stageio_clean_forced_evictions,
		&xfs_stageio_atomic64_ops,
		&xfs_stageio_global_clean_forced_evictions, 0444);
module_param_cb(stageio_clean_reuse_current_bytes,
		&xfs_stageio_atomic64_ops,
		&xfs_stageio_global_clean_reuse_current_bytes, 0444);
module_param_cb(stageio_clean_reused_bytes,
		&xfs_stageio_atomic64_ops,
		&xfs_stageio_global_clean_reused_bytes, 0444);
module_param_cb(stageio_clean_reuse_peak_bytes,
		&xfs_stageio_atomic64_ops,
		&xfs_stageio_global_clean_reuse_peak_bytes, 0444);
module_param_cb(stageio_fragment_calls, &xfs_stageio_atomic64_ops,
		&xfs_stageio_global_fragment_calls, 0444);
module_param_cb(stageio_fragment_bytes, &xfs_stageio_atomic64_ops,
		&xfs_stageio_global_fragment_bytes, 0444);
module_param_cb(stageio_fragment_ns, &xfs_stageio_atomic64_ops,
		&xfs_stageio_global_fragment_ns, 0444);
module_param_cb(stageio_small_write_calls, &xfs_stageio_atomic64_ops,
		&xfs_stageio_global_small_write_calls, 0444);
module_param_cb(stageio_small_write_bytes, &xfs_stageio_atomic64_ops,
		&xfs_stageio_global_small_write_bytes, 0444);
module_param_cb(stageio_small_write_ns, &xfs_stageio_atomic64_ops,
		&xfs_stageio_global_small_write_ns, 0444);
module_param_cb(stageio_dio_pool_bytes, &xfs_stageio_atomic64_ops,
		&xfs_stageio_global_dio_pool_bytes, 0444);
module_param_cb(stageio_dio_pool_wait_calls, &xfs_stageio_atomic64_ops,
		&xfs_stageio_global_dio_pool_wait_calls, 0444);
module_param_cb(stageio_dio_pool_wait_ns, &xfs_stageio_atomic64_ops,
		&xfs_stageio_global_dio_pool_wait_ns, 0444);
module_param_cb(stageio_inode_records, &xfs_stageio_atomic64_ops,
		&xfs_stageio_global_inode_records, 0444);
module_param_cb(stageio_inode_records_peak, &xfs_stageio_atomic64_ops,
		&xfs_stageio_global_inode_records_peak, 0444);
module_param_cb(stageio_range_drain_calls, &xfs_stageio_atomic64_ops,
		&xfs_stageio_global_range_drain_calls, 0444);
module_param_cb(stageio_range_drain_entries, &xfs_stageio_atomic64_ops,
		&xfs_stageio_global_range_drain_entries, 0444);
module_param_cb(stageio_range_drain_ns, &xfs_stageio_atomic64_ops,
		&xfs_stageio_global_range_drain_ns, 0444);
module_param_cb(stageio_range_drain_max_ns, &xfs_stageio_atomic64_ops,
		&xfs_stageio_global_range_drain_max_ns, 0444);
module_param_cb(stageio_prepare_ring_enqueues, &xfs_stageio_atomic64_ops,
		&xfs_stageio_global_prepare_ring_enqueues, 0444);
module_param_cb(stageio_prepare_ring_dequeues, &xfs_stageio_atomic64_ops,
		&xfs_stageio_global_prepare_ring_dequeues, 0444);
module_param_cb(stageio_prepare_ring_full, &xfs_stageio_atomic64_ops,
		&xfs_stageio_global_prepare_ring_full, 0444);
module_param_cb(stageio_prepare_ring_peak, &xfs_stageio_atomic64_ops,
		&xfs_stageio_global_prepare_ring_peak, 0444);
module_param_cb(stageio_persist_ring_enqueues, &xfs_stageio_atomic64_ops,
		&xfs_stageio_global_persist_ring_enqueues, 0444);
module_param_cb(stageio_persist_ring_dequeues, &xfs_stageio_atomic64_ops,
		&xfs_stageio_global_persist_ring_dequeues, 0444);
module_param_cb(stageio_persist_ring_full, &xfs_stageio_atomic64_ops,
		&xfs_stageio_global_persist_ring_full, 0444);
module_param_cb(stageio_persist_ring_peak, &xfs_stageio_atomic64_ops,
		&xfs_stageio_global_persist_ring_peak, 0444);
module_param_cb(stageio_prepare_workers_peak, &xfs_stageio_atomic64_ops,
		&xfs_stageio_global_prepare_workers_peak, 0444);
module_param_cb(stageio_persist_workers_peak, &xfs_stageio_atomic64_ops,
		&xfs_stageio_global_persist_workers_peak, 0444);

void
xfs_stageio_record_middle_dio(
	size_t			bytes,
	u64			prepare_ns,
	u64			bvec_ns,
	u64			dio_ns,
	u64			release_ns)
{
	atomic64_inc(&xfs_stageio_global_middle_calls);
	atomic64_add(bytes, &xfs_stageio_global_middle_bytes);
	atomic64_add(prepare_ns, &xfs_stageio_global_middle_prepare_ns);
	atomic64_add(bvec_ns, &xfs_stageio_global_middle_bvec_ns);
	atomic64_add(dio_ns, &xfs_stageio_global_middle_dio_ns);
	atomic64_add(release_ns, &xfs_stageio_global_middle_release_ns);
}

void
xfs_stageio_record_middle_copy(
	size_t			bytes,
	u64			copy_ns)
{
	atomic64_inc(&xfs_stageio_global_middle_copy_calls);
	atomic64_add(bytes, &xfs_stageio_global_middle_copy_bytes);
	atomic64_add(copy_ns, &xfs_stageio_global_middle_copy_ns);
}

void
xfs_stageio_record_middle_direct(
	size_t			bytes,
	u64			dio_ns)
{
	atomic64_inc(&xfs_stageio_global_middle_direct_calls);
	atomic64_add(bytes, &xfs_stageio_global_middle_direct_bytes);
	atomic64_add(dio_ns, &xfs_stageio_global_middle_direct_dio_ns);
}

void
xfs_stageio_record_middle_staged(
	size_t			bytes,
	u64			dio_ns)
{
	atomic64_inc(&xfs_stageio_global_middle_staged_calls);
	atomic64_add(bytes, &xfs_stageio_global_middle_staged_bytes);
	atomic64_add(dio_ns, &xfs_stageio_global_middle_staged_dio_ns);
}

void
xfs_stageio_record_middle_async(
	u64			submit_ns,
	u64			wait_ns)
{
	atomic64_add(submit_ns, &xfs_stageio_global_middle_async_submit_ns);
	atomic64_add(wait_ns, &xfs_stageio_global_middle_async_wait_ns);
}

void
xfs_stageio_record_clean_handoff(
	size_t			bytes,
	unsigned int		cached,
	unsigned int		conflicts,
	u64			ns)
{
	atomic64_add(bytes, &xfs_stageio_global_clean_handoff_bytes);
	atomic64_add(cached, &xfs_stageio_global_clean_handoff_pages);
	atomic64_add(conflicts, &xfs_stageio_global_clean_handoff_conflicts);
	atomic64_add(ns, &xfs_stageio_global_clean_handoff_ns);
}

static void
xfs_stageio_async_handoff_queue(
	s64			bytes)
{
	s64			pending, peak;

	atomic64_add(bytes, &xfs_stageio_global_async_handoff_queued_bytes);
	pending = atomic64_add_return(bytes,
			&xfs_stageio_global_async_handoff_pending_bytes);
	peak = atomic64_read(
			&xfs_stageio_global_async_handoff_pending_peak_bytes);
	while (pending > peak) {
		s64 seen = atomic64_cmpxchg(
				&xfs_stageio_global_async_handoff_pending_peak_bytes,
				peak, pending);

		if (seen == peak)
			break;
		peak = seen;
	}
}

static void
xfs_stageio_async_handoff_complete(
	s64			bytes)
{
	atomic64_add(bytes, &xfs_stageio_global_async_handoff_completed_bytes);
	atomic64_sub(bytes, &xfs_stageio_global_async_handoff_pending_bytes);
}

void
xfs_stageio_record_fragment(
	size_t			bytes,
	u64			ns)
{
	atomic64_inc(&xfs_stageio_global_fragment_calls);
	atomic64_add(bytes, &xfs_stageio_global_fragment_bytes);
	atomic64_add(ns, &xfs_stageio_global_fragment_ns);
}

void
xfs_stageio_record_small_write(
	size_t			bytes,
	u64			ns)
{
	atomic64_inc(&xfs_stageio_global_small_write_calls);
	atomic64_add(bytes, &xfs_stageio_global_small_write_bytes);
	atomic64_add(ns, &xfs_stageio_global_small_write_ns);
}

static struct file *
xfs_stageio_owner_file_get(
	struct file		*file)
{
	atomic64_inc(&xfs_stageio_global_owner_file_refs);
	return get_file(file);
}

static void
xfs_stageio_owner_file_put(
	struct file		*file)
{
	atomic64_dec(&xfs_stageio_global_owner_file_refs);
	fput(file);
}

static void
xfs_stageio_inode_set_file(
	struct xfs_stageio_inode *wi,
	struct file		*file)
{
	struct file		*held = NULL;

	spin_lock(&wi->file_lock);
	if (!wi->io_file) {
		held = xfs_stageio_owner_file_get(file);
		wi->io_file = held;
	}
	spin_unlock(&wi->file_lock);
}

static void
xfs_stageio_inode_release_file_if_empty(
	struct xfs_stageio_inode *wi)
{
	struct file		*file = NULL;

	spin_lock(&wi->file_lock);
	if (!atomic_read(&wi->staging) &&
	    !atomic_read(&wi->batch_active) &&
	    !xa_marked(&wi->entries, XFS_STAGEIO_XA_DIRTY) &&
	    !xa_marked(&wi->regions, XFS_STAGEIO_XA_DIRTY)) {
		file = wi->io_file;
		wi->io_file = NULL;
	}
	spin_unlock(&wi->file_lock);
	if (file)
		xfs_stageio_owner_file_put(file);
}

static struct file *
xfs_stageio_temp_file_get(
	struct file		*file)
{
	atomic64_inc(&xfs_stageio_global_temp_file_refs);
	return get_file(file);
}

static void
xfs_stageio_temp_file_put(
	struct file		*file)
{
	atomic64_dec(&xfs_stageio_global_temp_file_refs);
	fput(file);
}

static struct file *
xfs_stageio_inode_temp_file_get(
	struct xfs_stageio_inode *wi)
{
	struct file		*file = NULL;

	spin_lock(&wi->file_lock);
	if (wi->io_file)
		file = xfs_stageio_temp_file_get(wi->io_file);
	spin_unlock(&wi->file_lock);
	return file;
}

enum xfs_stageio_flush_job_type {
	XFS_STAGEIO_FLUSH_PARTIAL = 0,
	XFS_STAGEIO_FLUSH_RAW,
};

struct xfs_stageio_flush_job {
	enum xfs_stageio_flush_job_type type;
};

struct xfs_stageio_batch {
	struct xfs_stageio_flush_job	job;
	unsigned int			nr;
	u64				dispatch_queued_ns;
	struct xfs_stageio_entry	*entries[];
};

struct xfs_stageio_raw_batch;

struct xfs_stageio_raw_item {
	pgoff_t			page_index;
	struct folio			*folio;
	struct xfs_stageio_raw_batch	*batch;
	atomic_long_t			clean_seen_pages;
	bool				handoff;
	bool				clean_recent;
	bool				clean_removed;
	bool				clean_indexed;
};

struct xfs_stageio_raw_batch {
	struct xfs_stageio_flush_job	job;
	struct work_struct		handoff_work;
	struct list_head		clean_node;
	struct list_head		clean_inode_node;
	atomic_t			clean_access_state;
	struct xfs_stageio_inode	*wi;
	struct file			*file;
	unsigned int			nr;
	size_t				clean_bytes;
	struct xfs_stageio_raw_item	items[];
};

enum xfs_stageio_clean_access_state {
	/* One-hit batches remain probationary; only a second read admits them. */
	XFS_STAGEIO_CLEAN_PROBATION_NEW = 0,
	XFS_STAGEIO_CLEAN_PROBATION_SEEN,
	XFS_STAGEIO_CLEAN_PROTECTED_COLD,
	XFS_STAGEIO_CLEAN_PROTECTED_HOT,
};

static void
xfs_stageio_note_clean_access(
	struct folio		*folio,
	pgoff_t		page_index)
{
	struct xfs_stageio_raw_item *item;
	struct xfs_stageio_raw_batch *batch;
	unsigned long		bit, seen;
	bool			repeated;
	int			old, new;

	if (!xfs_stageio_clean_access_aware ||
	    !folio_test_private(folio))
		return;
	item = folio_get_private(folio);
	if (!item || page_index < item->page_index ||
	    page_index >= item->page_index + folio_nr_pages(folio))
		return;
	batch = item->batch;
	bit = BIT(page_index - item->page_index);
	seen = atomic_long_fetch_or(bit, &item->clean_seen_pages);
	repeated = seen & bit;

	old = atomic_read(&batch->clean_access_state);
	for (;;) {
		switch (old) {
		case XFS_STAGEIO_CLEAN_PROBATION_NEW:
			new = repeated ? XFS_STAGEIO_CLEAN_PROTECTED_HOT :
				XFS_STAGEIO_CLEAN_PROBATION_SEEN;
			break;
		case XFS_STAGEIO_CLEAN_PROBATION_SEEN:
			if (!repeated)
				return;
			new = XFS_STAGEIO_CLEAN_PROTECTED_HOT;
			break;
		case XFS_STAGEIO_CLEAN_PROTECTED_COLD:
			new = XFS_STAGEIO_CLEAN_PROTECTED_HOT;
			break;
		default:
			return;
		}
		if (atomic_try_cmpxchg(&batch->clean_access_state,
				&old, new))
			break;
	}

	if (old == XFS_STAGEIO_CLEAN_PROBATION_NEW && !repeated)
		atomic64_inc(&xfs_stageio_global_clean_first_accesses);
	else if (old <= XFS_STAGEIO_CLEAN_PROBATION_SEEN)
		atomic64_inc(&xfs_stageio_global_clean_promotions);
	else
		atomic64_inc(&xfs_stageio_global_clean_rereferences);
}

struct xfs_stageio_mpmc_slot {
	atomic_long_t			sequence;
	void				*data;
};

struct xfs_stageio_mpmc_ring {
	struct xfs_stageio_mpmc_slot	*slots;
	unsigned long			mask;
	atomic_long_t			enqueue_pos;
	atomic_long_t			dequeue_pos;
	atomic_t			next_worker;
	wait_queue_head_t		not_full;
	atomic64_t			*enqueues;
	atomic64_t			*dequeues;
	atomic64_t			*full;
	atomic64_t			*peak;
};

struct xfs_stageio_mpmc_worker {
	struct work_struct		work;
	struct xfs_stageio_mount	*wm;
	bool				prepare;
};

static void xfs_stageio_finish_entry(struct xfs_stageio_entry *entry);
static void xfs_stageio_prepare_batch(struct xfs_stageio_batch *batch);
static void xfs_stageio_dispatch_batch(struct xfs_stageio_batch *batch);
static void xfs_stageio_dispatch_raw_batch(
		struct xfs_stageio_raw_batch *batch);
static void xfs_stageio_mpmc_worker(struct work_struct *work);
static void xfs_stageio_raw_batch_handoff_work(struct work_struct *work);
static void xfs_stageio_raw_batch_clean_reclaim_work(
		struct work_struct *work);
static void xfs_stageio_inode_flush_work(struct work_struct *work);
static void xfs_stageio_remove_entry(struct xfs_stageio_entry *entry);
static void xfs_stageio_raw_entry_del(void);
static void xfs_stageio_kick_inode(struct xfs_stageio_inode *wi,
		unsigned long delay);
static bool xfs_stageio_evict_one_clean(struct xfs_stageio_mount *wm);
static bool xfs_stageio_evict_one_reuse(struct xfs_stageio_mount *wm);
static void xfs_stageio_evict_inode_clean(struct xfs_stageio_inode *wi);

static void
xfs_stageio_atomic64_update_peak(
	atomic64_t		*peak,
	s64			value)
{
	s64			old = atomic64_read(peak);

	while (value > old) {
		s64 seen = atomic64_cmpxchg(peak, old, value);

		if (seen == old)
			break;
		old = seen;
	}
}

static struct xfs_stageio_mpmc_ring *
xfs_stageio_mpmc_ring_alloc(
	unsigned int		slots,
	atomic64_t		*enqueues,
	atomic64_t		*dequeues,
	atomic64_t		*full,
	atomic64_t		*peak,
	gfp_t			gfp)
{
	struct xfs_stageio_mpmc_ring *ring;
	unsigned int		i;

	slots = roundup_pow_of_two(max(slots, XFS_STAGEIO_MPMC_MIN_SLOTS));
	ring = kzalloc(sizeof(*ring), gfp);
	if (!ring)
		return NULL;
	ring->slots = kcalloc(slots, sizeof(*ring->slots), gfp);
	if (!ring->slots) {
		kfree(ring);
		return NULL;
	}
	ring->mask = slots - 1;
	atomic_long_set(&ring->enqueue_pos, 0);
	atomic_long_set(&ring->dequeue_pos, 0);
	atomic_set(&ring->next_worker, 0);
	init_waitqueue_head(&ring->not_full);
	ring->enqueues = enqueues;
	ring->dequeues = dequeues;
	ring->full = full;
	ring->peak = peak;
	for (i = 0; i < slots; i++)
		atomic_long_set(&ring->slots[i].sequence, i);
	return ring;
}

static void
xfs_stageio_mpmc_ring_free(
	struct xfs_stageio_mpmc_ring *ring)
{
	if (!ring)
		return;
	WARN_ON_ONCE(atomic_long_read(&ring->enqueue_pos) !=
			atomic_long_read(&ring->dequeue_pos));
	kfree(ring->slots);
	kfree(ring);
}

static bool
xfs_stageio_mpmc_try_enqueue(
	struct xfs_stageio_mpmc_ring *ring,
	void			*data)
{
	struct xfs_stageio_mpmc_slot *slot;
	long			pos, sequence, difference;

	pos = atomic_long_read(&ring->enqueue_pos);
	for (;;) {
		slot = &ring->slots[pos & ring->mask];
		sequence = atomic_long_read_acquire(&slot->sequence);
		difference = sequence - pos;
		if (!difference &&
		    atomic_long_try_cmpxchg_relaxed(&ring->enqueue_pos,
				&pos, pos + 1))
			break;
		if (difference < 0) {
			atomic64_inc(ring->full);
			return false;
		}
		pos = atomic_long_read(&ring->enqueue_pos);
		cpu_relax();
	}

	WRITE_ONCE(slot->data, data);
	atomic_long_set_release(&slot->sequence, pos + 1);
	atomic64_inc(ring->enqueues);
	xfs_stageio_atomic64_update_peak(ring->peak,
			(pos + 1) - atomic_long_read(&ring->dequeue_pos));
	return true;
}

static void *
xfs_stageio_mpmc_try_dequeue(
	struct xfs_stageio_mpmc_ring *ring)
{
	struct xfs_stageio_mpmc_slot *slot;
	long			pos, sequence, difference;
	void			*data;

	pos = atomic_long_read(&ring->dequeue_pos);
	for (;;) {
		slot = &ring->slots[pos & ring->mask];
		sequence = atomic_long_read_acquire(&slot->sequence);
		difference = sequence - (pos + 1);
		if (!difference &&
		    atomic_long_try_cmpxchg_relaxed(&ring->dequeue_pos,
				&pos, pos + 1))
			break;
		if (difference < 0)
			return NULL;
		pos = atomic_long_read(&ring->dequeue_pos);
		cpu_relax();
	}

	data = READ_ONCE(slot->data);
	atomic_long_set_release(&slot->sequence, pos + ring->mask + 1);
	atomic64_inc(ring->dequeues);
	wake_up(&ring->not_full);
	return data;
}

static bool
xfs_stageio_mpmc_has_space(
	struct xfs_stageio_mpmc_ring *ring)
{
	return atomic_long_read(&ring->enqueue_pos) -
		atomic_long_read(&ring->dequeue_pos) <= ring->mask;
}

static void
xfs_stageio_mpmc_kick(
	struct xfs_stageio_mount *wm,
	bool			prepare,
	bool			raw)
{
	struct xfs_stageio_mpmc_ring *ring = prepare ?
			wm->prepare_ring : wm->persist_ring;
	struct xfs_stageio_mpmc_worker *workers = prepare ?
			wm->prepare_workers : wm->persist_workers;
	struct workqueue_struct *wq = prepare ? wm->prepare_wq : wm->persist_wq;
	unsigned int		workers_count = prepare ?
			wm->prepare_worker_count : wm->persist_worker_count;
	unsigned int		next;

	if (raw)
		workers_count = min(workers_count, xfs_stageio_qd);
	next = atomic_inc_return(&ring->next_worker) - 1;
	queue_work(wq, &workers[next % workers_count].work);
}

static void
xfs_stageio_mpmc_enqueue(
	struct xfs_stageio_mount *wm,
	bool			prepare,
	bool			raw,
	void			*data)
{
	struct xfs_stageio_mpmc_ring *ring = prepare ?
			wm->prepare_ring : wm->persist_ring;

	while (!xfs_stageio_mpmc_try_enqueue(ring, data)) {
		xfs_stageio_mpmc_kick(wm, prepare, raw);
		wait_event_timeout(ring->not_full,
				xfs_stageio_mpmc_has_space(ring), 1);
	}
	xfs_stageio_mpmc_kick(wm, prepare, raw);
}

static bool
xfs_stageio_raw_persist_try_acquire(
	struct xfs_stageio_mount *wm)
{
	int			active;

	active = atomic_read(&wm->raw_persist_active);
	while (active < xfs_stageio_qd)
		if (atomic_try_cmpxchg(&wm->raw_persist_active, &active,
				active + 1))
			return true;
	return false;
}

static void
xfs_stageio_raw_persist_release(
	struct xfs_stageio_mount *wm)
{
	atomic_dec(&wm->raw_persist_active);
}

static void
xfs_stageio_update_peak(
	s64			value)
{
	s64			old;

	old = atomic64_read(&xfs_stageio_global_peak_bytes);
	while (value > old) {
		s64 seen = atomic64_cmpxchg(&xfs_stageio_global_peak_bytes,
				old, value);

		if (seen == old)
			break;
		old = seen;
	}
}

void
xfs_stageio_record_front_iolock(
	u64			ns)
{
	s64			old;

	atomic64_add(ns, &xfs_stageio_global_front_iolock_ns);
	atomic64_inc(&xfs_stageio_global_front_iolock_calls);
	old = atomic64_read(&xfs_stageio_global_front_iolock_max_ns);
	while (ns > old) {
		s64 seen = atomic64_cmpxchg(
				&xfs_stageio_global_front_iolock_max_ns, old, ns);

		if (seen == old)
			break;
		old = seen;
	}
}

void
xfs_stageio_record_mapping_check(
	u64			ns)
{
	atomic64_add(ns, &xfs_stageio_global_mapping_check_ns);
}

static void
xfs_stageio_dio_account(
	u64			now)
{
	s64			active = atomic64_read(
				&xfs_stageio_global_dio_current);
	u64			elapsed = now - xfs_stageio_dio_stats_last_ns;

	atomic64_add(elapsed * active, &xfs_stageio_global_dio_weighted_ns);
	if (active)
		atomic64_add(elapsed, &xfs_stageio_global_dio_busy_ns);
	xfs_stageio_dio_stats_last_ns = now;
}

static void
xfs_stageio_dio_start(void)
{
	s64			active, peak;
	u64			now = ktime_get_ns();

	spin_lock(&xfs_stageio_dio_stats_lock);
	xfs_stageio_dio_account(now);
	active = atomic64_inc_return(&xfs_stageio_global_dio_current);
	spin_unlock(&xfs_stageio_dio_stats_lock);
	peak = atomic64_read(&xfs_stageio_global_dio_peak);
	while (active > peak) {
		s64 seen = atomic64_cmpxchg(&xfs_stageio_global_dio_peak,
				peak, active);

		if (seen == peak)
			break;
		peak = seen;
	}
}

static void
xfs_stageio_dio_finish(void)
{
	u64			now = ktime_get_ns();

	spin_lock(&xfs_stageio_dio_stats_lock);
	xfs_stageio_dio_account(now);
	atomic64_dec(&xfs_stageio_global_dio_current);
	spin_unlock(&xfs_stageio_dio_stats_lock);
}

static void
xfs_stageio_update_batch_peak(
	s64			active)
{
	s64			peak;

	peak = atomic64_read(&xfs_stageio_global_batch_active_peak);
	while (active > peak) {
		s64 seen = atomic64_cmpxchg(
				&xfs_stageio_global_batch_active_peak,
				peak, active);

		if (seen == peak)
			break;
		peak = seen;
	}
}

static int
xfs_stageio_charge(
	struct xfs_stageio_mount *wm,
	s64			bytes,
	bool			throttle,
	struct xfs_stageio_inode *wi)
{
	s64			old;

	for (;;) {
		old = atomic64_read(&wm->total_dirty_bytes);
		if (!throttle || old + bytes <= xfs_stageio_high_bytes) {
			if (atomic64_cmpxchg(&wm->total_dirty_bytes, old,
					old + bytes) == old)
				break;
			continue;
		}
		if (wi)
			xfs_stageio_kick_inode(wi, 0);
		if (xfs_stageio_evict_one_clean(wm))
			continue;
		if (xfs_stageio_evict_one_reuse(wm))
			continue;

		if (wait_event_killable(wm->dirty_wait,
				atomic64_read(&wm->total_dirty_bytes) + bytes <=
				xfs_stageio_high_bytes))
			return -ERESTARTSYS;
	}

	old = atomic64_add_return(bytes, &xfs_stageio_global_mem_bytes);
	xfs_stageio_update_peak(old);
	return 0;
}

static void
xfs_stageio_uncharge(
	struct xfs_stageio_mount *wm,
	s64			bytes)
{
	atomic64_sub(bytes, &wm->total_dirty_bytes);
	atomic64_sub(bytes, &xfs_stageio_global_mem_bytes);
	wake_up_all(&wm->dirty_wait);
}

static void
xfs_stageio_update_clean_reuse_peak(
	s64			bytes)
{
	s64			peak;

	peak = atomic64_read(&xfs_stageio_global_clean_reuse_peak_bytes);
	while (bytes > peak) {
		s64 seen = atomic64_cmpxchg(
				&xfs_stageio_global_clean_reuse_peak_bytes,
				peak, bytes);

		if (seen == peak)
			break;
		peak = seen;
	}
}

static bool
xfs_stageio_put_reuse_folio(
	struct xfs_stageio_mount *wm,
	struct folio		*folio)
{
	s64			bytes = folio_size(folio);
	s64			reuse_total;
	bool			pooled = false;

	if (!xfs_stageio_clean_reuse || !xfs_stageio_raw_order ||
	    folio_order(folio) != xfs_stageio_raw_order)
		return false;
	spin_lock(&wm->reuse_lock);
	if (wm->reuse_bytes + bytes <= xfs_stageio_clean_reuse_bytes) {
		list_add_tail(&folio->lru, &wm->reuse_folios);
		wm->reuse_bytes += bytes;
		reuse_total = atomic64_add_return(bytes,
				&xfs_stageio_global_clean_reuse_current_bytes);
		pooled = true;
	}
	spin_unlock(&wm->reuse_lock);
	if (!pooled)
		return false;
	xfs_stageio_update_clean_reuse_peak(reuse_total);
	return true;
}

static struct folio *
xfs_stageio_take_reuse_folio(
	struct xfs_stageio_mount *wm,
	unsigned int		order)
{
	struct folio		*folio = NULL;
	s64			bytes;

	if (!xfs_stageio_clean_reuse || order != xfs_stageio_raw_order)
		return NULL;
	spin_lock(&wm->reuse_lock);
	if (!list_empty(&wm->reuse_folios)) {
		folio = list_first_entry(&wm->reuse_folios,
				struct folio, lru);
		list_del_init(&folio->lru);
		bytes = folio_size(folio);
		wm->reuse_bytes -= bytes;
		atomic64_sub(bytes,
				&xfs_stageio_global_clean_reuse_current_bytes);
	}
	spin_unlock(&wm->reuse_lock);
	if (!folio)
		return NULL;
	atomic64_add(bytes, &xfs_stageio_global_clean_reused_bytes);
	return folio;
}

static bool
xfs_stageio_evict_one_reuse(
	struct xfs_stageio_mount *wm)
{
	struct folio		*folio;
	s64			bytes;

	spin_lock(&wm->reuse_lock);
	if (list_empty(&wm->reuse_folios)) {
		spin_unlock(&wm->reuse_lock);
		return false;
	}
	folio = list_first_entry(&wm->reuse_folios,
			struct folio, lru);
	list_del_init(&folio->lru);
	bytes = folio_size(folio);
	wm->reuse_bytes -= bytes;
	atomic64_sub(bytes,
				&xfs_stageio_global_clean_reuse_current_bytes);
	spin_unlock(&wm->reuse_lock);
	folio_put(folio);
	xfs_stageio_uncharge(wm, bytes);
	return true;
}

static void
xfs_stageio_drain_reuse_folios(
	struct xfs_stageio_mount *wm)
{
	while (xfs_stageio_evict_one_reuse(wm))
		cond_resched();
}

static void
xfs_stageio_release_clean_batch(
	struct xfs_stageio_raw_batch *batch,
	bool				recycle)
{
	struct xfs_stageio_inode *wi = batch->wi;
	XA_STATE(xas, &wi->entries, 0);
	s64			uncharge = 0;
	unsigned int		i;

	xas_lock(&xas);
	for (i = 0; i < batch->nr; i++) {
		struct xfs_stageio_raw_item *item = &batch->items[i];
		void			*node;

		if (!item->clean_recent)
			continue;
		xas_set_order(&xas, item->page_index,
				folio_order(item->folio));
		node = xas_load(&xas);
		item->clean_indexed = node == item->folio;
		if (node == item->folio &&
		    xas_get_mark(&xas, XFS_STAGEIO_XA_FULL) &&
		    !xas_get_mark(&xas, XFS_STAGEIO_XA_DIRTY) &&
		    !xas_get_mark(&xas, XFS_STAGEIO_XA_FLUSHING)) {
			xas_store(&xas, NULL);
			item->clean_removed = true;
		}
	}
	xas_unlock(&xas);

	for (i = 0; i < batch->nr; i++) {
		struct xfs_stageio_raw_item *item = &batch->items[i];
		size_t			bytes;

		if (!item->clean_recent)
			continue;
		if (folio_test_private(item->folio)) {
			WARN_ON_ONCE(folio_get_private(item->folio) != item);
			folio_detach_private(item->folio);
		}
		bytes = folio_size(item->folio);
		if (item->clean_removed) {
			atomic64_dec(&wi->nr_entries);
			xfs_stageio_raw_entry_del();
			folio_put(item->folio);
		}
		if (item->clean_indexed && !item->clean_removed) {
			folio_put(item->folio);
		} else if (!recycle ||
			   !xfs_stageio_put_reuse_folio(wi->wm,
					item->folio)) {
			folio_put(item->folio);
			uncharge += bytes;
		}
		item->clean_recent = false;
	}
	if (uncharge)
		xfs_stageio_uncharge(wi->wm, uncharge);
	atomic64_sub(batch->clean_bytes,
			&xfs_stageio_global_clean_recent_current_bytes);
	atomic64_add(batch->clean_bytes,
			&xfs_stageio_global_clean_recent_evicted_bytes);
	xfs_stageio_inode_release_file_if_empty(wi);
	xfs_stageio_inode_put(wi);
	kfree(batch);
}

static void
xfs_stageio_raw_batch_clean_reclaim_work(
	struct work_struct	*work)
{
	struct xfs_stageio_raw_batch *batch = container_of(work,
			struct xfs_stageio_raw_batch, handoff_work);

	xfs_stageio_release_clean_batch(batch, true);
}

static void
xfs_stageio_queue_clean_reclaim(
	struct xfs_stageio_raw_batch *batch)
{
	WARN_ON_ONCE(!queue_work(batch->wi->wm->handoff_wq,
			&batch->handoff_work));
}

static struct xfs_stageio_raw_batch *
xfs_stageio_take_one_clean(
	struct xfs_stageio_mount *wm)
{
	struct xfs_stageio_raw_batch *batch;

	mutex_lock(&wm->clean_lock);
	if (list_empty(&wm->clean_batches)) {
		mutex_unlock(&wm->clean_lock);
		return NULL;
	}
	batch = list_first_entry(&wm->clean_batches,
			struct xfs_stageio_raw_batch, clean_node);
	list_del_init(&batch->clean_node);
	list_del_init(&batch->clean_inode_node);
	atomic64_sub(batch->clean_bytes, &wm->clean_bytes);
	atomic64_sub(batch->clean_bytes, &batch->wi->clean_bytes);
	mutex_unlock(&wm->clean_lock);
	return batch;
}

static bool
xfs_stageio_evict_one_clean(
	struct xfs_stageio_mount *wm)
{
	struct xfs_stageio_raw_batch *batch;

	batch = xfs_stageio_take_one_clean(wm);
	if (!batch)
		return false;
	xfs_stageio_release_clean_batch(batch, false);
	return true;
}

static struct xfs_stageio_raw_batch *
xfs_stageio_take_one_inode_clean(
	struct xfs_stageio_inode *wi)
{
	struct xfs_stageio_mount *wm = wi->wm;
	struct xfs_stageio_raw_batch *batch;
	s64			scan_budget, scanned = 0;

	mutex_lock(&wm->clean_lock);
	if (list_empty(&wi->clean_batches)) {
		mutex_unlock(&wm->clean_lock);
		return NULL;
	}
	scan_budget = atomic64_read(&wi->clean_bytes);
	for (;;) {
		int state;

		batch = list_first_entry(&wi->clean_batches,
				struct xfs_stageio_raw_batch,
				clean_inode_node);
		if (!xfs_stageio_clean_access_aware)
			break;
		if (scanned >= scan_budget) {
			/* A bounded revolution guarantees progress if all are hot. */
			atomic64_inc(
				&xfs_stageio_global_clean_forced_evictions);
			break;
		}
		state = atomic_read(&batch->clean_access_state);
		if (state != XFS_STAGEIO_CLEAN_PROTECTED_HOT)
			break;
		if (atomic_cmpxchg(&batch->clean_access_state,
				XFS_STAGEIO_CLEAN_PROTECTED_HOT,
				XFS_STAGEIO_CLEAN_PROTECTED_COLD) !=
		    XFS_STAGEIO_CLEAN_PROTECTED_HOT)
			continue;
		list_move_tail(&batch->clean_inode_node,
				&wi->clean_batches);
		list_move_tail(&batch->clean_node, &wm->clean_batches);
		scanned += batch->clean_bytes;
		atomic64_inc(&xfs_stageio_global_clean_second_chances);
	}
	list_del_init(&batch->clean_node);
	list_del_init(&batch->clean_inode_node);
	atomic64_sub(batch->clean_bytes, &wm->clean_bytes);
	atomic64_sub(batch->clean_bytes, &wi->clean_bytes);
	mutex_unlock(&wm->clean_lock);
	return batch;
}

static void
xfs_stageio_evict_inode_clean(
	struct xfs_stageio_inode *wi)
{
	struct xfs_stageio_mount *wm = wi->wm;
	struct xfs_stageio_raw_batch *batch;
	LIST_HEAD(dispose);

	mutex_lock(&wm->clean_lock);
	while (!list_empty(&wi->clean_batches)) {
		batch = list_first_entry(&wi->clean_batches,
				struct xfs_stageio_raw_batch,
				clean_inode_node);
		list_del_init(&batch->clean_inode_node);
		list_move_tail(&batch->clean_node, &dispose);
		atomic64_sub(batch->clean_bytes, &wm->clean_bytes);
		atomic64_sub(batch->clean_bytes, &wi->clean_bytes);
	}
	mutex_unlock(&wm->clean_lock);
	while (!list_empty(&dispose)) {
		batch = list_first_entry(&dispose,
				struct xfs_stageio_raw_batch, clean_node);
		list_del_init(&batch->clean_node);
		xfs_stageio_release_clean_batch(batch, false);
	}
}

static void
xfs_stageio_retain_clean_batch(
	struct xfs_stageio_raw_batch *batch)
{
	struct xfs_stageio_inode *wi = batch->wi;
	struct xfs_stageio_mount *wm = wi->wm;
	struct xfs_stageio_raw_batch *reclaim;
	s64			bytes, peak;

	refcount_inc(&wi->refcount);
	xfs_stageio_temp_file_put(batch->file);
	batch->file = NULL;
	mutex_lock(&wm->clean_lock);
	list_add_tail(&batch->clean_node, &wm->clean_batches);
	list_add_tail(&batch->clean_inode_node,
			&wi->clean_batches);
	bytes = atomic64_add_return(batch->clean_bytes, &wm->clean_bytes);
	atomic64_add(batch->clean_bytes, &wi->clean_bytes);
	mutex_unlock(&wm->clean_lock);
	atomic64_add(batch->clean_bytes,
			&xfs_stageio_global_clean_recent_retained_bytes);
	atomic64_add(batch->clean_bytes,
			&xfs_stageio_global_clean_recent_current_bytes);
	peak = atomic64_read(&xfs_stageio_global_clean_recent_peak_bytes);
	while (bytes > peak) {
		s64 seen = atomic64_cmpxchg(
				&xfs_stageio_global_clean_recent_peak_bytes,
				peak, bytes);

		if (seen == peak)
			break;
		peak = seen;
	}
	xfs_stageio_inode_release_file_if_empty(wi);
	while (atomic64_read(&wi->clean_bytes) >
	       xfs_stageio_clean_recent_inode_bytes) {
		reclaim = xfs_stageio_take_one_inode_clean(wi);
		if (!reclaim)
			break;
		xfs_stageio_queue_clean_reclaim(reclaim);
	}
}

static bool
xfs_stageio_entry_get(
	struct xfs_stageio_entry *entry)
{
	return refcount_inc_not_zero(&entry->refcount);
}

static void
xfs_stageio_entry_free_rcu(
	struct rcu_head		*rcu)
{
	struct xfs_stageio_entry *entry = container_of(rcu,
			struct xfs_stageio_entry, rcu);
	unsigned int		i;

	for (i = 0; i < XFS_STAGEIO_NR_SEGS; i++) {
		kfree(entry->active[i]);
		kfree(entry->flushing[i]);
		kfree(entry->active_valid[i]);
		kfree(entry->flushing_valid[i]);
	}
	if (entry->active_full)
		folio_put(entry->active_full);
	if (entry->prepared_folio)
		folio_put(entry->prepared_folio);
	if (WARN_ON_ONCE(entry->io_file))
		xfs_stageio_owner_file_put(entry->io_file);
	atomic64_sub(sizeof(*entry), &xfs_stageio_global_entry_bytes);
	xfs_stageio_uncharge(entry->wi->wm, sizeof(*entry));
	kfree(entry);
}

static void
xfs_stageio_entry_put(
	struct xfs_stageio_entry *entry)
{
	if (entry && refcount_dec_and_test(&entry->refcount))
		call_rcu(&entry->rcu, xfs_stageio_entry_free_rcu);
}

static struct xfs_stageio_entry *
xfs_stageio_entry_alloc(
	struct xfs_stageio_inode *wi,
	struct file		*file,
	pgoff_t		page_index,
	gfp_t			gfp)
{
	struct xfs_stageio_entry *entry;
	s64			entry_bytes, old;
	int			error;

	error = xfs_stageio_charge(wi->wm, sizeof(*entry), false, NULL);
	if (error)
		return NULL;
	entry = kzalloc(sizeof(*entry), XFS_STAGEIO_ACCOUNT_GFP(gfp));
	if (!entry) {
		xfs_stageio_uncharge(wi->wm, sizeof(*entry));
		return NULL;
	}
	entry_bytes = atomic64_add_return(sizeof(*entry),
			&xfs_stageio_global_entry_bytes);
	old = atomic64_read(&xfs_stageio_global_entry_peak_bytes);
	while (entry_bytes > old) {
		s64 seen = atomic64_cmpxchg(
				&xfs_stageio_global_entry_peak_bytes,
				old, entry_bytes);

		if (seen == old)
			break;
		old = seen;
	}

	entry->wi = wi;
	entry->io_file = xfs_stageio_owner_file_get(file);
	entry->page_index = page_index;
	entry->state = XFS_STAGEIO_ENTRY_DIRTY;
	refcount_set(&entry->refcount, 1);
	mutex_init(&entry->lock);
	return entry;
}

static void
xfs_stageio_region_account_add(
	s64			bytes)
{
	s64			bytes_now, peak;

	bytes_now = atomic64_add_return(bytes,
			&xfs_stageio_global_region_bytes);
	peak = atomic64_read(&xfs_stageio_global_region_peak_bytes);
	while (bytes_now > peak) {
		s64 seen = atomic64_cmpxchg(
				&xfs_stageio_global_region_peak_bytes,
				peak, bytes_now);

		if (seen == peak)
			break;
		peak = seen;
	}
}

static void
xfs_stageio_region_account_sub(
	s64			bytes)
{
	atomic64_sub(bytes, &xfs_stageio_global_region_bytes);
}

static size_t
xfs_stageio_region_slots_size(
	unsigned int		nr)
{
	return struct_size((struct xfs_stageio_region_slots *)NULL,
			entries, nr);
}

static struct xfs_stageio_region_slots *
xfs_stageio_region_slots_alloc(
	struct xfs_stageio_inode *wi,
	unsigned int		nr)
{
	struct xfs_stageio_region_slots *slots;
	size_t			bytes = xfs_stageio_region_slots_size(nr);

	if (xfs_stageio_charge(wi->wm, bytes, false, NULL))
		return NULL;
	slots = kzalloc(bytes, XFS_STAGEIO_ACCOUNT_GFP(GFP_NOFS));
	if (!slots) {
		xfs_stageio_uncharge(wi->wm, bytes);
		return NULL;
	}
	slots->capacity = nr;
	xfs_stageio_region_account_add(bytes);
	return slots;
}

static void
xfs_stageio_region_slots_free(
	struct xfs_stageio_inode *wi,
	struct xfs_stageio_region_slots *slots)
{
	size_t			bytes;

	if (!slots)
		return;
	bytes = xfs_stageio_region_slots_size(slots->capacity);
	xfs_stageio_region_account_sub(bytes);
	xfs_stageio_uncharge(wi->wm, bytes);
	kfree(slots);
}

static struct xfs_stageio_region *
xfs_stageio_region_alloc(
	struct xfs_stageio_inode *wi)
{
	struct xfs_stageio_region *region;

	if (xfs_stageio_charge(wi->wm, sizeof(*region), false, NULL))
		return NULL;
	region = kzalloc(sizeof(*region),
			XFS_STAGEIO_ACCOUNT_GFP(GFP_NOFS));
	if (!region) {
		xfs_stageio_uncharge(wi->wm, sizeof(*region));
		return NULL;
	}
	refcount_set(&region->refcount, 1);
	xfs_stageio_region_account_add(sizeof(*region));
	return region;
}

static bool
xfs_stageio_region_get(
	struct xfs_stageio_region *region)
{
	return refcount_inc_not_zero(&region->refcount);
}

static void
xfs_stageio_region_put(
	struct xfs_stageio_inode *wi,
	struct xfs_stageio_region *region)
{
	if (!region || !refcount_dec_and_test(&region->refcount))
		return;
	WARN_ON_ONCE(region->slots);
	xfs_stageio_region_account_sub(sizeof(*region));
	xfs_stageio_uncharge(wi->wm, sizeof(*region));
	kfree(region);
}

static struct xfs_stageio_region *
xfs_stageio_region_lookup_get(
	struct xfs_stageio_inode *wi,
	unsigned long		region_index)
{
	struct xfs_stageio_region *region;

	xa_lock(&wi->regions);
	region = xa_load(&wi->regions, region_index);
	if (region && !xfs_stageio_region_get(region))
		region = NULL;
	xa_unlock(&wi->regions);
	return region;
}

static bool
xfs_stageio_region_valid(
	struct xfs_stageio_region *region)
{
	struct xfs_stageio_region_slots *slots = region->slots;

	if (!slots)
		return !region->dirty;
	return slots->nr &&
		slots->nr <= slots->capacity &&
		slots->capacity <= XFS_STAGEIO_REGION_PAGES &&
		hweight16(slots->present) == slots->nr &&
		!(region->dirty & ~slots->present);
}

static void
xfs_stageio_region_report_invalid(
	unsigned long		region_index)
{
	atomic64_inc(&xfs_stageio_global_flush_errors);
	pr_err_ratelimited("XFS StageIO: invalid fragment region %lu\n",
			region_index);
}

static void
xfs_stageio_region_remove_empty(
	struct xfs_stageio_inode *wi,
	unsigned long		region_index,
	struct xfs_stageio_region *region)
{
	bool			removed = false;

	xa_lock(&wi->regions);
	if (xa_load(&wi->regions, region_index) == region &&
	    !region->slots) {
		__xa_erase(&wi->regions, region_index);
		removed = true;
	}
	xa_unlock(&wi->regions);
	if (removed)
		xfs_stageio_region_put(wi, region);
}

static int
xfs_stageio_region_insert_entry(
	struct xfs_stageio_entry *entry)
{
	struct xfs_stageio_inode *wi = entry->wi;
	unsigned long		region_index = entry->page_index >>
					XFS_STAGEIO_REGION_ORDER;
	unsigned int		slot = entry->page_index &
					XFS_STAGEIO_REGION_MASK;
	struct xfs_stageio_region *region, *candidate;
	struct xfs_stageio_region_slots *slots, *new_slots;
	unsigned int		rank, nr;
	u16			present;
	void			*old;

retry_region:
	region = xfs_stageio_region_lookup_get(wi, region_index);
	if (!region) {
		candidate = xfs_stageio_region_alloc(wi);
		if (!candidate)
			return -ENOMEM;
		old = xa_cmpxchg(&wi->regions, region_index, NULL, candidate,
				XFS_STAGEIO_ACCOUNT_GFP(GFP_NOFS));
		if (xa_is_err(old)) {
			xfs_stageio_region_put(wi, candidate);
			return xa_err(old);
		}
		if (old) {
			xfs_stageio_region_put(wi, candidate);
			cond_resched();
			goto retry_region;
		}
		region = candidate;
		xfs_stageio_region_get(region);
	}

retry_slots:
	xa_lock(&wi->regions);
	if (xa_load(&wi->regions, region_index) != region) {
		xa_unlock(&wi->regions);
		xfs_stageio_region_put(wi, region);
		goto retry_region;
	}
	if (!xfs_stageio_region_valid(region)) {
		xa_unlock(&wi->regions);
		xfs_stageio_region_report_invalid(region_index);
		xfs_stageio_region_put(wi, region);
		return -EUCLEAN;
	}
	slots = region->slots;
	if (slots && (slots->present & BIT(slot))) {
		xa_unlock(&wi->regions);
		xfs_stageio_region_put(wi, region);
		return -EEXIST;
	}
	nr = slots ? slots->nr : 0;
	present = slots ? slots->present : 0;
	xa_unlock(&wi->regions);

	new_slots = xfs_stageio_region_slots_alloc(wi, nr + 1);
	if (!new_slots) {
		xfs_stageio_region_remove_empty(wi, region_index, region);
		xfs_stageio_region_put(wi, region);
		return -ENOMEM;
	}
	xa_lock(&wi->regions);
	if (xa_load(&wi->regions, region_index) != region ||
	    region->slots != slots ||
	    (slots && (slots->nr != nr || slots->present != present))) {
		xa_unlock(&wi->regions);
		xfs_stageio_region_slots_free(wi, new_slots);
		cond_resched();
		goto retry_slots;
	}
	rank = slots ? hweight16(slots->present & (BIT(slot) - 1)) : 0;
	new_slots->present = present | BIT(slot);
	new_slots->nr = nr + 1;
	if (rank)
		memcpy(new_slots->entries, slots->entries,
			rank * sizeof(*new_slots->entries));
	new_slots->entries[rank] = entry;
	if (rank < nr)
		memcpy(&new_slots->entries[rank + 1],
			&slots->entries[rank],
			(nr - rank) * sizeof(*new_slots->entries));
	region->slots = new_slots;
	xa_unlock(&wi->regions);
	xfs_stageio_region_slots_free(wi, slots);
	xfs_stageio_region_put(wi, region);
	return 0;
}

static void
xfs_stageio_entry_drop_buffers(
	struct xfs_stageio_entry *entry)
{
	struct xfs_stageio_mount *wm = entry->wi->wm;
	struct file		*file;
	unsigned int		i;

	mutex_lock(&entry->lock);
	for (i = 0; i < XFS_STAGEIO_NR_SEGS; i++) {
		if (entry->active[i]) {
			kfree(entry->active[i]);
			kfree(entry->active_valid[i]);
			entry->active[i] = NULL;
			entry->active_valid[i] = NULL;
			xfs_stageio_uncharge(wm, XFS_STAGEIO_SEG_SIZE +
					XFS_STAGEIO_VALID_SIZE);
		}
		if (entry->flushing[i]) {
			kfree(entry->flushing[i]);
			kfree(entry->flushing_valid[i]);
			entry->flushing[i] = NULL;
			entry->flushing_valid[i] = NULL;
			xfs_stageio_uncharge(wm, XFS_STAGEIO_SEG_SIZE +
					XFS_STAGEIO_VALID_SIZE);
		}
	}
	if (entry->active_full) {
		folio_put(entry->active_full);
		entry->active_full = NULL;
		xfs_stageio_uncharge(wm, PAGE_SIZE);
	}
	if (entry->prepared_folio) {
		folio_put(entry->prepared_folio);
		entry->prepared_folio = NULL;
		xfs_stageio_uncharge(wm, PAGE_SIZE);
	}
	entry->active_mask = 0;
	entry->flushing_mask = 0;
	entry->flushing_full = false;
	file = entry->io_file;
	entry->io_file = NULL;
	entry->state = XFS_STAGEIO_ENTRY_INVALID;
	mutex_unlock(&entry->lock);
	if (file)
		xfs_stageio_owner_file_put(file);
}

static void
xfs_stageio_inode_purge_entries(
	struct xfs_stageio_inode *wi)
{
	struct xfs_stageio_region *region;
	struct xfs_stageio_region_slots *slots;
	void			*node;
	unsigned long		index;
	unsigned int		i;

	xa_for_each(&wi->entries, index, node) {
		bool full = xa_get_mark(&wi->entries, index,
				XFS_STAGEIO_XA_FULL);

		xa_erase(&wi->entries, index);
		if (full) {
			size_t bytes = folio_size(node);

			folio_put(node);
			xfs_stageio_uncharge(wi->wm, bytes);
			xfs_stageio_raw_entry_del();
		} else {
			struct xfs_stageio_entry *entry = node;

			xfs_stageio_entry_drop_buffers(entry);
			xfs_stageio_entry_put(entry);
		}
		atomic64_dec(&wi->nr_entries);
	}
	xa_for_each(&wi->regions, index, region) {
		xa_erase(&wi->regions, index);
		slots = region->slots;
		region->slots = NULL;
		for (i = 0; slots && i < slots->nr; i++) {
			struct xfs_stageio_entry *entry = slots->entries[i];

			xfs_stageio_entry_drop_buffers(entry);
			xfs_stageio_entry_put(entry);
			atomic64_dec(&wi->nr_entries);
		}
		xfs_stageio_region_slots_free(wi, slots);
		xfs_stageio_region_put(wi, region);
	}
	xfs_stageio_inode_release_file_if_empty(wi);
}

static void
xfs_stageio_inode_destroy(
	struct xfs_stageio_inode *wi)
{
	xfs_stageio_inode_purge_entries(wi);
	xa_destroy(&wi->entries);
	xa_destroy(&wi->regions);
}

static struct xfs_stageio_inode *
xfs_stageio_inode_alloc(
	struct xfs_stageio_mount *wm,
	struct xfs_inode	*ip,
	gfp_t			gfp)
{
	struct xfs_stageio_inode *wi;

	wi = kzalloc(sizeof(*wi), XFS_STAGEIO_ACCOUNT_GFP(gfp));
	if (!wi)
		return NULL;
	if (xfs_stageio_charge(wm, sizeof(*wi), true, NULL)) {
		kfree(wi);
		return NULL;
	}

	wi->wm = wm;
	wi->ip = ip;
	spin_lock_init(&wi->file_lock);
	xa_init(&wi->entries);
	xa_init(&wi->regions);
	atomic64_set(&wi->dirty_bytes, 0);
	atomic64_set(&wi->nr_entries, 0);
	atomic64_set(&wi->seq, 0);
	atomic_set(&wi->staging, 0);
	atomic_set(&wi->batch_active, 0);
	wi->state = XFS_STAGEIO_INODE_ACTIVE;
	mutex_init(&wi->drain_mutex);
	spin_lock_init(&wi->drain_lock);
	init_rwsem(&wi->visibility_sem);
	INIT_LIST_HEAD(&wi->clean_batches);
	atomic64_set(&wi->clean_bytes, 0);
	INIT_DELAYED_WORK(&wi->flush_work, xfs_stageio_inode_flush_work);
	INIT_LIST_HEAD(&wi->mount_node);
	refcount_set(&wi->refcount, 1);
	{
		s64 records = atomic64_inc_return(
				&xfs_stageio_global_inode_records);
		s64 peak = atomic64_read(
				&xfs_stageio_global_inode_records_peak);

		while (records > peak) {
			s64 seen = atomic64_cmpxchg(
					&xfs_stageio_global_inode_records_peak,
					peak, records);

			if (seen == peak)
				break;
			peak = seen;
		}
	}
	return wi;
}

static void
xfs_stageio_inode_free_rcu(
	struct rcu_head		*rcu)
{
	struct xfs_stageio_inode *wi = container_of(rcu,
			struct xfs_stageio_inode, rcu);
	struct file		*file;

	xfs_stageio_inode_destroy(wi);
	file = xchg(&wi->io_file, NULL);
	if (file)
		xfs_stageio_owner_file_put(file);
	atomic64_dec(&xfs_stageio_global_inode_records);
	xfs_stageio_uncharge(wi->wm, sizeof(*wi));
	kfree(wi);
}

void
xfs_stageio_inode_put(
	struct xfs_stageio_inode *wi)
{
	if (wi && refcount_dec_and_test(&wi->refcount))
		call_rcu(&wi->rcu, xfs_stageio_inode_free_rcu);
}

static void
xfs_stageio_reset_stats(void)
{
	atomic64_set(&xfs_stageio_global_mem_bytes, 0);
	atomic64_set(&xfs_stageio_global_peak_bytes, 0);
	atomic64_set(&xfs_stageio_global_accepted_bytes, 0);
	atomic64_set(&xfs_stageio_global_drained_bytes, 0);
	atomic64_set(&xfs_stageio_global_base_reads, 0);
	atomic64_set(&xfs_stageio_global_cache_bases, 0);
	atomic64_set(&xfs_stageio_global_full_cancels, 0);
	atomic64_set(&xfs_stageio_global_device_read_bytes, 0);
	atomic64_set(&xfs_stageio_global_device_write_bytes, 0);
	atomic64_set(&xfs_stageio_global_flush_errors, 0);
	atomic64_set(&xfs_stageio_global_batches, 0);
	atomic64_set(&xfs_stageio_global_batch_pages, 0);
	atomic64_set(&xfs_stageio_global_dio_read_calls, 0);
	atomic64_set(&xfs_stageio_global_dio_read_pages, 0);
	atomic64_set(&xfs_stageio_global_dio_write_calls, 0);
	atomic64_set(&xfs_stageio_global_dio_write_pages, 0);
	atomic64_set(&xfs_stageio_global_dio_current, 0);
	atomic64_set(&xfs_stageio_global_dio_peak, 0);
	atomic64_set(&xfs_stageio_global_dio_weighted_ns, 0);
	atomic64_set(&xfs_stageio_global_dio_busy_ns, 0);
	xfs_stageio_dio_stats_last_ns = ktime_get_ns();
	atomic64_set(&xfs_stageio_global_prepare_queue_ns, 0);
	atomic64_set(&xfs_stageio_global_prepare_ns, 0);
	atomic64_set(&xfs_stageio_global_dispatch_queue_ns, 0);
	atomic64_set(&xfs_stageio_global_dio_read_ns, 0);
	atomic64_set(&xfs_stageio_global_visibility_wait_ns, 0);
	atomic64_set(&xfs_stageio_global_dio_write_ns, 0);
	atomic64_set(&xfs_stageio_global_batch_active_peak, 0);
	atomic64_set(&xfs_stageio_global_scan_ns, 0);
	atomic64_set(&xfs_stageio_global_dispatch_ns, 0);
	atomic64_set(&xfs_stageio_global_finish_ns, 0);
	atomic64_set(&xfs_stageio_global_entry_bytes, 0);
	atomic64_set(&xfs_stageio_global_entry_peak_bytes, 0);
	atomic64_set(&xfs_stageio_global_region_bytes, 0);
	atomic64_set(&xfs_stageio_global_region_peak_bytes, 0);
	atomic64_set(&xfs_stageio_global_front_iolock_ns, 0);
	atomic64_set(&xfs_stageio_global_front_iolock_calls, 0);
	atomic64_set(&xfs_stageio_global_front_iolock_max_ns, 0);
	atomic64_set(&xfs_stageio_global_mapping_check_ns, 0);
	atomic64_set(&xfs_stageio_global_middle_calls, 0);
	atomic64_set(&xfs_stageio_global_middle_bytes, 0);
	atomic64_set(&xfs_stageio_global_middle_prepare_ns, 0);
	atomic64_set(&xfs_stageio_global_middle_bvec_ns, 0);
	atomic64_set(&xfs_stageio_global_middle_dio_ns, 0);
	atomic64_set(&xfs_stageio_global_middle_release_ns, 0);
	atomic64_set(&xfs_stageio_global_middle_copy_calls, 0);
	atomic64_set(&xfs_stageio_global_middle_copy_bytes, 0);
	atomic64_set(&xfs_stageio_global_middle_copy_ns, 0);
	atomic64_set(&xfs_stageio_global_middle_direct_calls, 0);
	atomic64_set(&xfs_stageio_global_middle_direct_bytes, 0);
	atomic64_set(&xfs_stageio_global_middle_direct_dio_ns, 0);
	atomic64_set(&xfs_stageio_global_middle_staged_calls, 0);
	atomic64_set(&xfs_stageio_global_middle_staged_bytes, 0);
	atomic64_set(&xfs_stageio_global_middle_staged_dio_ns, 0);
	atomic64_set(&xfs_stageio_global_middle_async_submit_ns, 0);
	atomic64_set(&xfs_stageio_global_middle_async_wait_ns, 0);
	atomic64_set(&xfs_stageio_global_clean_handoff_bytes, 0);
	atomic64_set(&xfs_stageio_global_clean_handoff_pages, 0);
	atomic64_set(&xfs_stageio_global_clean_handoff_conflicts, 0);
	atomic64_set(&xfs_stageio_global_clean_handoff_ns, 0);
	atomic64_set(&xfs_stageio_global_async_handoff_queued_bytes, 0);
	atomic64_set(&xfs_stageio_global_async_handoff_completed_bytes, 0);
	atomic64_set(&xfs_stageio_global_async_handoff_pending_bytes, 0);
	atomic64_set(&xfs_stageio_global_async_handoff_pending_peak_bytes, 0);
	atomic64_set(&xfs_stageio_global_clean_recent_retained_bytes, 0);
	atomic64_set(&xfs_stageio_global_clean_recent_evicted_bytes, 0);
	atomic64_set(&xfs_stageio_global_clean_recent_current_bytes, 0);
	atomic64_set(&xfs_stageio_global_clean_recent_hit_bytes, 0);
	atomic64_set(&xfs_stageio_global_clean_recent_miss_reads, 0);
	atomic64_set(&xfs_stageio_global_clean_recent_peak_bytes, 0);
	atomic64_set(&xfs_stageio_global_clean_first_accesses, 0);
	atomic64_set(&xfs_stageio_global_clean_promotions, 0);
	atomic64_set(&xfs_stageio_global_clean_rereferences, 0);
	atomic64_set(&xfs_stageio_global_clean_second_chances, 0);
	atomic64_set(&xfs_stageio_global_clean_forced_evictions, 0);
	atomic64_set(&xfs_stageio_global_clean_reuse_current_bytes, 0);
	atomic64_set(&xfs_stageio_global_clean_reused_bytes, 0);
	atomic64_set(&xfs_stageio_global_clean_reuse_peak_bytes, 0);
	atomic64_set(&xfs_stageio_global_fragment_calls, 0);
	atomic64_set(&xfs_stageio_global_fragment_bytes, 0);
	atomic64_set(&xfs_stageio_global_fragment_ns, 0);
	atomic64_set(&xfs_stageio_global_small_write_calls, 0);
	atomic64_set(&xfs_stageio_global_small_write_bytes, 0);
	atomic64_set(&xfs_stageio_global_small_write_ns, 0);
	atomic64_set(&xfs_stageio_global_dio_pool_wait_calls, 0);
	atomic64_set(&xfs_stageio_global_dio_pool_wait_ns, 0);
	atomic64_set(&xfs_stageio_global_inode_records, 0);
	atomic64_set(&xfs_stageio_global_inode_records_peak, 0);
	atomic64_set(&xfs_stageio_global_range_drain_calls, 0);
	atomic64_set(&xfs_stageio_global_range_drain_entries, 0);
	atomic64_set(&xfs_stageio_global_range_drain_ns, 0);
	atomic64_set(&xfs_stageio_global_range_drain_max_ns, 0);
	atomic64_set(&xfs_stageio_global_prepare_ring_enqueues, 0);
	atomic64_set(&xfs_stageio_global_prepare_ring_dequeues, 0);
	atomic64_set(&xfs_stageio_global_prepare_ring_full, 0);
	atomic64_set(&xfs_stageio_global_prepare_ring_peak, 0);
	atomic64_set(&xfs_stageio_global_persist_ring_enqueues, 0);
	atomic64_set(&xfs_stageio_global_persist_ring_dequeues, 0);
	atomic64_set(&xfs_stageio_global_persist_ring_full, 0);
	atomic64_set(&xfs_stageio_global_persist_ring_peak, 0);
	atomic64_set(&xfs_stageio_global_prepare_workers_active, 0);
	atomic64_set(&xfs_stageio_global_prepare_workers_peak, 0);
	atomic64_set(&xfs_stageio_global_persist_workers_active, 0);
	atomic64_set(&xfs_stageio_global_persist_workers_peak, 0);
}

static void
xfs_stageio_dio_pool_free(
	struct xfs_stageio_mount *wm)
{
	unsigned int		i;
	u64			bytes;

	if (!wm->dio_slots)
		return;

	bytes = XFS_STAGEIO_DIO_SLOTS *
		(XFS_STAGEIO_DIO_SLOT_SIZE +
		 XFS_STAGEIO_DIO_SLOT_PAGES * sizeof(struct bio_vec) +
		 sizeof(struct xfs_stageio_dio_slot));
	for (i = 0; i < XFS_STAGEIO_DIO_SLOTS; i++) {
		vfree(wm->dio_slots[i].data);
		kfree(wm->dio_slots[i].bvec);
	}
	kfree(wm->dio_slots);
	wm->dio_slots = NULL;
	atomic64_sub(bytes, &xfs_stageio_global_dio_pool_bytes);
}

static int
xfs_stageio_dio_pool_init(
	struct xfs_stageio_mount *wm,
	gfp_t			gfp)
{
	struct xfs_stageio_dio_slot *slot;
	unsigned int		i, j;
	u64			bytes;

	spin_lock_init(&wm->dio_slot_lock);
	INIT_LIST_HEAD(&wm->dio_free_slots);
	init_waitqueue_head(&wm->dio_slot_wait);
	wm->dio_slots = kcalloc(XFS_STAGEIO_DIO_SLOTS,
			sizeof(*wm->dio_slots), XFS_STAGEIO_ACCOUNT_GFP(gfp));
	if (!wm->dio_slots)
		return -ENOMEM;

	for (i = 0; i < XFS_STAGEIO_DIO_SLOTS; i++) {
		slot = &wm->dio_slots[i];
		slot->data = __vmalloc(XFS_STAGEIO_DIO_SLOT_SIZE,
				XFS_STAGEIO_ACCOUNT_GFP(gfp) | __GFP_ZERO);
		slot->bvec = kcalloc(XFS_STAGEIO_DIO_SLOT_PAGES,
				sizeof(*slot->bvec),
				XFS_STAGEIO_ACCOUNT_GFP(gfp));
		if (!slot->data || !slot->bvec)
			goto out_free;
		for (j = 0; j < XFS_STAGEIO_DIO_SLOT_PAGES; j++) {
			slot->bvec[j].bv_page = vmalloc_to_page(
					(char *)slot->data + (j << PAGE_SHIFT));
			slot->bvec[j].bv_len = PAGE_SIZE;
		}
		INIT_LIST_HEAD(&slot->list);
		list_add_tail(&slot->list, &wm->dio_free_slots);
		wm->dio_slots_available++;
	}

	bytes = XFS_STAGEIO_DIO_SLOTS *
		(XFS_STAGEIO_DIO_SLOT_SIZE +
		 XFS_STAGEIO_DIO_SLOT_PAGES * sizeof(struct bio_vec) +
		 sizeof(struct xfs_stageio_dio_slot));
	atomic64_add(bytes, &xfs_stageio_global_dio_pool_bytes);
	return 0;

out_free:
	for (i = 0; i < XFS_STAGEIO_DIO_SLOTS; i++) {
		vfree(wm->dio_slots[i].data);
		kfree(wm->dio_slots[i].bvec);
	}
	kfree(wm->dio_slots);
	wm->dio_slots = NULL;
	return -ENOMEM;
}

struct xfs_stageio_dio_slot *
xfs_stageio_dio_slot_get(
	struct xfs_stageio_mount *wm)
{
	struct xfs_stageio_dio_slot *slot;
	u64			start = 0;
	int			ret;

	for (;;) {
		spin_lock(&wm->dio_slot_lock);
		if (!list_empty(&wm->dio_free_slots)) {
			slot = list_first_entry(&wm->dio_free_slots,
					struct xfs_stageio_dio_slot, list);
			list_del_init(&slot->list);
			wm->dio_slots_available--;
			spin_unlock(&wm->dio_slot_lock);
			if (start)
				atomic64_add(ktime_get_ns() - start,
						&xfs_stageio_global_dio_pool_wait_ns);
			return slot;
		}
		spin_unlock(&wm->dio_slot_lock);

		if (!start) {
			start = ktime_get_ns();
			atomic64_inc(&xfs_stageio_global_dio_pool_wait_calls);
		}
		ret = wait_event_killable(wm->dio_slot_wait,
				READ_ONCE(wm->dio_slots_available));
		if (ret) {
			atomic64_add(ktime_get_ns() - start,
					&xfs_stageio_global_dio_pool_wait_ns);
			return ERR_PTR(ret);
		}
	}
}

void
xfs_stageio_dio_slot_put(
	struct xfs_stageio_mount *wm,
	struct xfs_stageio_dio_slot *slot)
{
	spin_lock(&wm->dio_slot_lock);
	list_add_tail(&slot->list, &wm->dio_free_slots);
	wm->dio_slots_available++;
	spin_unlock(&wm->dio_slot_lock);
	wake_up(&wm->dio_slot_wait);
}

struct xfs_stageio_mount *
xfs_stageio_mount_alloc(
	gfp_t			gfp)
{
	struct xfs_stageio_mount *wm;
	unsigned int		i;
	int			error;

	wm = kzalloc(sizeof(*wm), gfp);
	if (!wm)
		return ERR_PTR(-ENOMEM);

	mutex_init(&wm->inode_lock);
	for (i = 0; i < XFS_STAGEIO_NR_ADMISSION_LOCKS; i++)
		mutex_init(&wm->admission_locks[i]);
	INIT_LIST_HEAD(&wm->inodes);
	init_waitqueue_head(&wm->dirty_wait);
	atomic64_set(&wm->total_dirty_bytes, 0);
	mutex_init(&wm->clean_lock);
	INIT_LIST_HEAD(&wm->clean_batches);
	atomic64_set(&wm->clean_bytes, 0);
	spin_lock_init(&wm->reuse_lock);
	INIT_LIST_HEAD(&wm->reuse_folios);
	wm->reuse_bytes = 0;

	wm->enabled = xfs_stageio_enable;
	if (!wm->enabled)
		return wm;

	xfs_stageio_batch = clamp_t(unsigned int, xfs_stageio_batch, 1, 1024);
	xfs_stageio_qd = clamp_t(unsigned int, xfs_stageio_qd, 1, 128);
	if (xfs_stageio_raw_order != 4)
		xfs_stageio_raw_order = 0;
	if (xfs_stageio_high_bytes < PAGE_SIZE * xfs_stageio_qd)
		xfs_stageio_high_bytes = PAGE_SIZE * xfs_stageio_qd;
	if (xfs_stageio_clean_recent_inode_bytes < PAGE_SIZE)
		xfs_stageio_clean_recent_inode_bytes = PAGE_SIZE;
	if (xfs_stageio_clean_reuse_bytes < PAGE_SIZE)
		xfs_stageio_clean_reuse_bytes = PAGE_SIZE;

	wm->prepare_worker_count = min_t(unsigned int, num_online_cpus(),
			XFS_STAGEIO_MPMC_MAX_WORKERS);
	wm->persist_worker_count = wm->prepare_worker_count;
	atomic_set(&wm->raw_persist_active, 0);
	wm->prepare_ring = xfs_stageio_mpmc_ring_alloc(
			wm->prepare_worker_count * 4,
			&xfs_stageio_global_prepare_ring_enqueues,
			&xfs_stageio_global_prepare_ring_dequeues,
			&xfs_stageio_global_prepare_ring_full,
			&xfs_stageio_global_prepare_ring_peak, gfp);
	if (!wm->prepare_ring) {
		error = -ENOMEM;
		goto out_free;
	}
	wm->persist_ring = xfs_stageio_mpmc_ring_alloc(
			wm->persist_worker_count * 4,
			&xfs_stageio_global_persist_ring_enqueues,
			&xfs_stageio_global_persist_ring_dequeues,
			&xfs_stageio_global_persist_ring_full,
			&xfs_stageio_global_persist_ring_peak, gfp);
	if (!wm->persist_ring) {
		error = -ENOMEM;
		goto out_prepare_ring;
	}
	wm->prepare_workers = kcalloc(wm->prepare_worker_count,
			sizeof(*wm->prepare_workers), gfp);
	wm->persist_workers = kcalloc(wm->persist_worker_count,
			sizeof(*wm->persist_workers), gfp);
	if (!wm->prepare_workers || !wm->persist_workers) {
		error = -ENOMEM;
		goto out_workers;
	}
	for (i = 0; i < wm->prepare_worker_count; i++) {
		wm->prepare_workers[i].wm = wm;
		wm->prepare_workers[i].prepare = true;
		INIT_WORK(&wm->prepare_workers[i].work,
				xfs_stageio_mpmc_worker);
	}
	for (i = 0; i < wm->persist_worker_count; i++) {
		wm->persist_workers[i].wm = wm;
		wm->persist_workers[i].prepare = false;
		INIT_WORK(&wm->persist_workers[i].work,
				xfs_stageio_mpmc_worker);
	}

	wm->control_wq = alloc_workqueue("xfs-stageio-ctl",
			WQ_UNBOUND | WQ_MEM_RECLAIM, xfs_stageio_qd);
	if (!wm->control_wq) {
		error = -ENOMEM;
		goto out_workers;
	}
	wm->prepare_wq = alloc_workqueue("xfs-stageio-prepare",
			WQ_UNBOUND | WQ_MEM_RECLAIM, xfs_stageio_qd);
	if (!wm->prepare_wq) {
		error = -ENOMEM;
		goto out_control;
	}
	wm->persist_wq = alloc_workqueue("xfs-stageio-persist",
			WQ_UNBOUND | WQ_MEM_RECLAIM, xfs_stageio_qd);
	if (!wm->persist_wq) {
		error = -ENOMEM;
		goto out_prepare;
	}
	wm->handoff_wq = alloc_workqueue("xfs-stageio-handoff",
			WQ_UNBOUND | WQ_MEM_RECLAIM,
			XFS_STAGEIO_HANDOFF_WORKERS);
	if (!wm->handoff_wq) {
		error = -ENOMEM;
		goto out_persist;
	}
	xfs_stageio_reset_stats();
	error = xfs_stageio_dio_pool_init(wm, gfp);
	if (error)
		goto out_handoff;
	return wm;

out_handoff:
	destroy_workqueue(wm->handoff_wq);
out_persist:
	destroy_workqueue(wm->persist_wq);
out_prepare:
	destroy_workqueue(wm->prepare_wq);
out_control:
	destroy_workqueue(wm->control_wq);
out_workers:
	kfree(wm->persist_workers);
	kfree(wm->prepare_workers);
	xfs_stageio_mpmc_ring_free(wm->persist_ring);
out_prepare_ring:
	xfs_stageio_mpmc_ring_free(wm->prepare_ring);
out_free:
	kfree(wm);
	return ERR_PTR(error);
}

struct mutex *
xfs_stageio_admission_lock(
	struct xfs_stageio_mount *wm,
	struct xfs_inode	*ip)
{
	unsigned long		index = (unsigned long)ip >> 6;

	return &wm->admission_locks[index & XFS_STAGEIO_ADMISSION_MASK];
}

static void
xfs_stageio_kick_inode(
	struct xfs_stageio_inode *wi,
	unsigned long		delay)
{
	if (!READ_ONCE(wi->wm->enabled) ||
	    READ_ONCE(wi->state) != XFS_STAGEIO_INODE_ACTIVE ||
	    !wi->wm->control_wq)
		return;
	if (!queue_delayed_work(wi->wm->control_wq, &wi->flush_work, delay))
		mod_delayed_work(wi->wm->control_wq, &wi->flush_work, delay);
}

static bool
xfs_stageio_mount_has_dirty(
	struct xfs_stageio_mount *wm)
{
	struct xfs_stageio_inode *wi;
	bool			dirty = false;

	mutex_lock(&wm->inode_lock);
	list_for_each_entry(wi, &wm->inodes, mount_node) {
		if (atomic64_read(&wi->nr_entries)) {
			dirty = true;
			break;
		}
	}
	mutex_unlock(&wm->inode_lock);
	return dirty;
}

void
xfs_stageio_mount_free(
	struct xfs_stageio_mount *wm)
{
	struct xfs_stageio_inode *wi;
	unsigned int		loops = 0;

	if (!wm)
		return;

	if (wm->enabled) {
		while (xfs_stageio_evict_one_clean(wm))
			cond_resched();
		flush_workqueue(wm->handoff_wq);
		while (xfs_stageio_mount_has_dirty(wm) && loops++ < 4096) {
			while (xfs_stageio_evict_one_clean(wm))
				cond_resched();
			mutex_lock(&wm->inode_lock);
			list_for_each_entry(wi, &wm->inodes, mount_node)
				xfs_stageio_kick_inode(wi, 0);
			mutex_unlock(&wm->inode_lock);
			flush_workqueue(wm->control_wq);
			flush_workqueue(wm->prepare_wq);
			flush_workqueue(wm->persist_wq);
			flush_workqueue(wm->handoff_wq);
		}
		if (xfs_stageio_mount_has_dirty(wm))
			pr_err("XFS StageIO: unmount with dirty entries\n");
		while (xfs_stageio_evict_one_clean(wm))
			cond_resched();
		flush_workqueue(wm->handoff_wq);
		xfs_stageio_drain_reuse_folios(wm);

		WRITE_ONCE(wm->enabled, false);
		mutex_lock(&wm->inode_lock);
		list_for_each_entry(wi, &wm->inodes, mount_node) {
			WRITE_ONCE(wi->state, XFS_STAGEIO_INODE_DYING);
			cancel_delayed_work_sync(&wi->flush_work);
		}
		mutex_unlock(&wm->inode_lock);
		destroy_workqueue(wm->control_wq);
		destroy_workqueue(wm->prepare_wq);
		destroy_workqueue(wm->persist_wq);
		destroy_workqueue(wm->handoff_wq);
		kfree(wm->persist_workers);
		kfree(wm->prepare_workers);
		xfs_stageio_mpmc_ring_free(wm->persist_ring);
		xfs_stageio_mpmc_ring_free(wm->prepare_ring);
		xfs_stageio_dio_pool_free(wm);
	}

	/*
	 * Entry RCU callbacks dereference their owning inode record and mount.
	 * Purge and wait for them while both table references are still alive.
	 */
	mutex_lock(&wm->inode_lock);
	list_for_each_entry(wi, &wm->inodes, mount_node)
		xfs_stageio_inode_purge_entries(wi);
	mutex_unlock(&wm->inode_lock);
	rcu_barrier();

	mutex_lock(&wm->inode_lock);
	while (!list_empty(&wm->inodes)) {
		wi = list_first_entry(&wm->inodes,
				struct xfs_stageio_inode, mount_node);
		WRITE_ONCE(wi->state, XFS_STAGEIO_INODE_DYING);
		RCU_INIT_POINTER(wi->ip->i_stageio, NULL);
		list_del_init(&wi->mount_node);
		xfs_stageio_inode_put(wi);
	}
	mutex_unlock(&wm->inode_lock);
	rcu_barrier();
	kfree(wm);
}

struct xfs_stageio_inode *
xfs_stageio_inode_lookup(
	struct xfs_stageio_mount *wm,
	struct xfs_inode	*ip)
{
	struct xfs_stageio_inode *wi;

	if (!wm || !ip || !wm->enabled)
		return NULL;

	rcu_read_lock();
	wi = rcu_dereference(ip->i_stageio);
	if (wi && !refcount_inc_not_zero(&wi->refcount)) {
		wi = NULL;
	} else if (wi && READ_ONCE(wi->state) !=
			XFS_STAGEIO_INODE_ACTIVE) {
		xfs_stageio_inode_put(wi);
		wi = NULL;
	}
	rcu_read_unlock();
	return wi;
}

struct xfs_stageio_inode *
xfs_stageio_inode_get_or_create(
	struct xfs_stageio_mount *wm,
	struct xfs_inode	*ip,
	gfp_t			gfp)
{
	struct xfs_stageio_inode *wi, *old;

	if (!wm || !ip || !wm->enabled)
		return ERR_PTR(-EOPNOTSUPP);

	wi = xfs_stageio_inode_lookup(wm, ip);
	if (wi)
		return wi;

	wi = xfs_stageio_inode_alloc(wm, ip, gfp);
	if (!wi)
		return ERR_PTR(-ENOMEM);

	mutex_lock(&wm->inode_lock);
	old = rcu_dereference_protected(ip->i_stageio,
			lockdep_is_held(&wm->inode_lock));
	if (old) {
		if (!refcount_inc_not_zero(&old->refcount)) {
			old = ERR_PTR(-ENOENT);
		} else if (READ_ONCE(old->state) !=
				XFS_STAGEIO_INODE_ACTIVE) {
			xfs_stageio_inode_put(old);
			old = ERR_PTR(-ENOENT);
		}
		mutex_unlock(&wm->inode_lock);
		xfs_stageio_inode_put(wi);
		return old;
	}

	refcount_inc(&wi->refcount);
	rcu_assign_pointer(ip->i_stageio, wi);
	list_add_tail(&wi->mount_node, &wm->inodes);
	mutex_unlock(&wm->inode_lock);
	return wi;
}

void
xfs_stageio_inode_detach(
	struct xfs_inode	*ip)
{
	struct xfs_stageio_mount *wm;
	struct xfs_stageio_inode *wi;

	if (!ip || !ip->i_mount)
		return;
	wm = ip->i_mount->m_stageio;
	if (!wm)
		return;
	flush_workqueue(wm->handoff_wq);

	mutex_lock(&wm->inode_lock);
	wi = rcu_dereference_protected(ip->i_stageio,
			lockdep_is_held(&wm->inode_lock));
	if (!wi) {
		mutex_unlock(&wm->inode_lock);
		return;
	}
	xfs_stageio_evict_inode_clean(wi);
	WARN_ON_ONCE(atomic64_read(&wi->nr_entries));
	WRITE_ONCE(wi->state, XFS_STAGEIO_INODE_DYING);
	RCU_INIT_POINTER(ip->i_stageio, NULL);
	list_del_init(&wi->mount_node);
	mutex_unlock(&wm->inode_lock);

	cancel_delayed_work_sync(&wi->flush_work);
	WARN_ON_ONCE(atomic_read(&wi->batch_active));
	xfs_stageio_inode_put(wi);
}

static struct xfs_stageio_entry *
xfs_stageio_lookup_entry(
	struct xfs_stageio_inode *wi,
	pgoff_t		page_index)
{
	unsigned long		region_index = page_index >>
					XFS_STAGEIO_REGION_ORDER;
	unsigned int		slot = page_index & XFS_STAGEIO_REGION_MASK;
	struct xfs_stageio_region *region;
	struct xfs_stageio_region_slots *slots;
	struct xfs_stageio_entry *entry = NULL;
	unsigned int		rank;

	xa_lock(&wi->regions);
	region = xa_load(&wi->regions, region_index);
	slots = region ? region->slots : NULL;
	if (region && !xfs_stageio_region_valid(region)) {
		xfs_stageio_region_report_invalid(region_index);
	} else if (slots && (slots->present & BIT(slot))) {
		rank = hweight16(slots->present & (BIT(slot) - 1));
		entry = slots->entries[rank];
		if (!xfs_stageio_entry_get(entry) ||
		    READ_ONCE(entry->state) == XFS_STAGEIO_ENTRY_INVALID) {
			if (refcount_read(&entry->refcount))
				xfs_stageio_entry_put(entry);
			entry = NULL;
		}
	}
	xa_unlock(&wi->regions);
	return entry;
}

static struct folio *
xfs_stageio_lookup_full(
	struct xfs_stageio_inode *wi,
	pgoff_t		page_index)
{
	XA_STATE(xas, &wi->entries, page_index);
	struct folio		*folio = NULL;
	void			*node;

	xas_lock(&xas);
	node = xas_load(&xas);
	if (node && xas_get_mark(&xas, XFS_STAGEIO_XA_FULL)) {
		folio = node;
		folio_get(folio);
	}
	xas_unlock(&xas);
	return folio;
}

static struct folio *
xfs_stageio_lookup_clean_full(
	struct xfs_stageio_inode *wi,
	pgoff_t		page_index)
{
	XA_STATE(xas, &wi->entries, page_index);
	struct folio		*folio = NULL;
	void			*node;

	xas_lock(&xas);
	node = xas_load(&xas);
	if (node && xas_get_mark(&xas, XFS_STAGEIO_XA_FULL) &&
	    !xas_get_mark(&xas, XFS_STAGEIO_XA_DIRTY) &&
	    !xas_get_mark(&xas, XFS_STAGEIO_XA_FLUSHING)) {
		folio = node;
		folio_get(folio);
		xfs_stageio_note_clean_access(folio, page_index);
	}
	xas_unlock(&xas);
	return folio;
}

static struct folio *
xfs_stageio_lookup_next_clean_full(
	struct xfs_stageio_inode *wi,
	pgoff_t		*page_index,
	pgoff_t		last)
{
	XA_STATE(xas, &wi->entries, *page_index);
	struct folio		*folio = NULL;
	void			*node;

	xas_lock(&xas);
	xas_for_each(&xas, node, last) {
		if (!xas_get_mark(&xas, XFS_STAGEIO_XA_FULL) ||
		    xas_get_mark(&xas, XFS_STAGEIO_XA_DIRTY) ||
		    xas_get_mark(&xas, XFS_STAGEIO_XA_FLUSHING))
			continue;
		folio = node;
		folio_get(folio);
		*page_index = xas.xa_index;
		break;
	}
	xas_unlock(&xas);
	return folio;
}

static bool
xfs_stageio_range_has_large_index(
	struct xfs_stageio_inode *wi,
	pgoff_t		first,
	pgoff_t		last)
{
	unsigned long		index;
	void			*node;
	bool			found = false;

	xa_lock(&wi->entries);
	xa_for_each_range(&wi->entries, index, node, first, last) {
		if (!xa_get_mark(&wi->entries, index, XFS_STAGEIO_XA_FULL))
			continue;
		if (folio_order(node) || xfs_stageio_clean_recent) {
			found = true;
			break;
		}
	}
	xa_unlock(&wi->entries);
	return found;
}

static bool
xfs_stageio_raw_is_flushing(
	struct xfs_stageio_inode *wi,
	pgoff_t		page_index)
{
	XA_STATE(xas, &wi->entries, page_index);
	bool			flushing;

	xas_lock(&xas);
	flushing = xas_load(&xas) &&
			xas_get_mark(&xas, XFS_STAGEIO_XA_FULL) &&
			xas_get_mark(&xas, XFS_STAGEIO_XA_FLUSHING);
	xas_unlock(&xas);
	return flushing;
}

static void
xfs_stageio_wait_raw_flush(
	struct xfs_stageio_inode *wi,
	pgoff_t		page_index)
{
	wait_event(wi->wm->dirty_wait,
			!xfs_stageio_raw_is_flushing(wi, page_index));
}

static void
xfs_stageio_raw_entry_add(void)
{
	s64			bytes, peak;

	bytes = atomic64_add_return(sizeof(void *),
			&xfs_stageio_global_entry_bytes);
	peak = atomic64_read(&xfs_stageio_global_entry_peak_bytes);
	while (bytes > peak) {
		s64 seen = atomic64_cmpxchg(
				&xfs_stageio_global_entry_peak_bytes,
				peak, bytes);

		if (seen == peak)
			break;
		peak = seen;
	}
}

static void
xfs_stageio_raw_entry_del(void)
{
	atomic64_sub(sizeof(void *), &xfs_stageio_global_entry_bytes);
}

static struct xfs_stageio_entry *
xfs_stageio_promote_full(
	struct xfs_stageio_inode *wi,
	pgoff_t		page_index)
{
	struct folio		*folio;
	int			error;

retry:
	folio = xfs_stageio_lookup_full(wi, page_index);
	if (!folio)
		return NULL;
	folio_put(folio);
	error = xfs_stageio_range_drain(wi,
			(loff_t)page_index << PAGE_SHIFT, PAGE_SIZE);
	if (error)
		return ERR_PTR(error);
	goto retry;
}

static struct xfs_stageio_entry *
xfs_stageio_get_or_create_entry(
	struct xfs_stageio_inode *wi,
	struct file		*file,
	pgoff_t		page_index)
{
	struct xfs_stageio_entry *entry, *old;

retry:
	entry = xfs_stageio_lookup_entry(wi, page_index);
	if (entry)
		return entry;
	entry = xfs_stageio_promote_full(wi, page_index);
	if (entry)
		return entry;

	entry = xfs_stageio_entry_alloc(wi, file, page_index, GFP_NOFS);
	if (!entry)
		return ERR_PTR(-ENOMEM);

	old = NULL;
	{
		int error = xfs_stageio_region_insert_entry(entry);

		if (error == -EEXIST)
			old = entry;
		else if (error) {
			struct file *held = entry->io_file;

			entry->io_file = NULL;
			xfs_stageio_owner_file_put(held);
			xfs_stageio_entry_put(entry);
			return ERR_PTR(error);
		}
	}
	if (old) {
		struct file *held = entry->io_file;

		entry->io_file = NULL;
		xfs_stageio_owner_file_put(held);
		xfs_stageio_entry_put(entry);
		cond_resched();
		goto retry;
	}

	atomic64_inc(&wi->nr_entries);
	xfs_stageio_entry_get(entry);
	return entry;
}

static void
xfs_stageio_mark_dirty(
	struct xfs_stageio_entry *entry)
{
	struct xfs_stageio_inode *wi = entry->wi;
	unsigned long		region_index = entry->page_index >>
					XFS_STAGEIO_REGION_ORDER;
	unsigned int		slot = entry->page_index & XFS_STAGEIO_REGION_MASK;
	struct xfs_stageio_region *region;
	struct xfs_stageio_region_slots *slots;
	unsigned int		rank;

	xa_lock(&wi->regions);
	region = xa_load(&wi->regions, region_index);
	slots = region ? region->slots : NULL;
	if (region && !xfs_stageio_region_valid(region)) {
		xfs_stageio_region_report_invalid(region_index);
	} else if (slots && (slots->present & BIT(slot))) {
		rank = hweight16(slots->present & (BIT(slot) - 1));
		if (slots->entries[rank] != entry || entry->queued)
			goto out_unlock;
		region->dirty |= BIT(slot);
		__xa_set_mark(&wi->regions, region_index,
				XFS_STAGEIO_XA_DIRTY);
	}
out_unlock:
	xa_unlock(&wi->regions);
}

bool
xfs_stageio_can_stage(
	struct kiocb		*iocb,
	struct iov_iter		*from)
{
	size_t			count;

	if (!iocb || !from)
		return false;
	count = iov_iter_count(from);
	if (!count)
		return false;
	if (iocb->ki_flags & (IOCB_DIRECT | IOCB_DSYNC | IOCB_SYNC |
			IOCB_APPEND | IOCB_NOWAIT))
		return false;
	return true;
}

#define XFS_STAGEIO_STAGE_BATCH_PAGES	64
#define XFS_STAGEIO_STAGE_ALLOC_ORDER	6

static int
xfs_stageio_stage_full_folio(
	struct xfs_stageio_entry *entry,
	struct folio		*folio)
{
	struct xfs_stageio_mount *wm = entry->wi->wm;
	struct folio		*old;
	unsigned int		i;

	mutex_lock(&entry->lock);
	if (entry->state == XFS_STAGEIO_ENTRY_INVALID) {
		mutex_unlock(&entry->lock);
		return -EAGAIN;
	}
	old = entry->active_full;
	entry->active_full = folio;
	for (i = 0; i < XFS_STAGEIO_NR_SEGS; i++) {
		if (!entry->active[i])
			continue;
		kfree(entry->active[i]);
		bitmap_free(entry->active_valid[i]);
		entry->active[i] = NULL;
		entry->active_valid[i] = NULL;
		xfs_stageio_uncharge(wm, XFS_STAGEIO_SEG_SIZE +
				XFS_STAGEIO_VALID_SIZE);
	}
	entry->active_mask = 0;
	entry->seq = atomic64_inc_return(&entry->wi->seq);
	mutex_unlock(&entry->lock);
	if (old) {
		folio_put(old);
		xfs_stageio_uncharge(wm, PAGE_SIZE);
	}
	return 0;
}

static int
xfs_stageio_store_raw_folio(
	struct xfs_stageio_inode *wi,
	pgoff_t		page_index,
	struct folio		*folio)
{
	struct xfs_stageio_entry *entry;
	struct folio		*old_folio = NULL;
	XA_STATE_ORDER(xas, &wi->entries, page_index, folio_order(folio));
	void			*old = NULL;
	bool			inserted = false;
	int			error;
	unsigned int		order = folio_order(folio);

	if (order && xfs_stageio_region_range_has_entry(wi, page_index,
			page_index + folio_nr_pages(folio) - 1))
		return -EEXIST;
	if (!order) {
		entry = xfs_stageio_lookup_entry(wi, page_index);
		if (entry) {
			error = xfs_stageio_stage_full_folio(entry, folio);
			if (!error)
				xfs_stageio_mark_dirty(entry);
			xfs_stageio_entry_put(entry);
			return error;
		}
	}

	if (order) {
		bool conflict = false;

		do {
			void *node;

			xas_lock(&xas);
			xas_for_each_conflict(&xas, node) {
				conflict = true;
				break;
			}
			if (!conflict) {
				xas_store(&xas, folio);
				if (!xas_error(&xas)) {
					xas_set_mark(&xas,
						XFS_STAGEIO_XA_DIRTY);
					xas_set_mark(&xas,
						XFS_STAGEIO_XA_FULL);
				}
			}
			xas_unlock(&xas);
			if (conflict)
				return -EEXIST;
		} while (xas_nomem(&xas,
				XFS_STAGEIO_ACCOUNT_GFP(GFP_NOFS)));
		error = xas_error(&xas);
		if (error)
			return error;
		atomic64_inc(&wi->nr_entries);
		xfs_stageio_raw_entry_add();
		return 0;
	}

retry_store:
	do {
		xas_lock(&xas);
		old = xas_load(&xas);
		if (old && xas_get_mark(&xas, XFS_STAGEIO_XA_FULL) &&
		    xas_get_mark(&xas, XFS_STAGEIO_XA_FLUSHING)) {
			xas_unlock(&xas);
			xfs_stageio_wait_raw_flush(wi, page_index);
			xas_reset(&xas);
			goto retry_store;
		}
		xas_store(&xas, folio);
		if (!xas_error(&xas)) {
			xas_set_mark(&xas, XFS_STAGEIO_XA_DIRTY);
			xas_set_mark(&xas, XFS_STAGEIO_XA_FULL);
			inserted = !old;
			if (old)
				old_folio = old;
		}
		xas_unlock(&xas);
	} while (xas_nomem(&xas, XFS_STAGEIO_ACCOUNT_GFP(GFP_NOFS)));
	error = xas_error(&xas);
	if (error)
		return error;

	if (inserted) {
		atomic64_inc(&wi->nr_entries);
		xfs_stageio_raw_entry_add();
	} else if (old_folio) {
		size_t old_size = folio_size(old_folio);

		folio_put(old_folio);
		xfs_stageio_uncharge(wi->wm, old_size);
	}
	return 0;
}

static int
xfs_stageio_stage_large_folio(
	struct xfs_stageio_inode *wi,
	struct iov_iter		*from,
	loff_t			pos)
{
	struct xfs_stageio_mount *wm = wi->wm;
	struct iov_iter		iter = *from;
	struct folio		*folio;
	size_t			bytes = PAGE_SIZE << xfs_stageio_raw_order;
	bool			reused = false;
	int			error;

	folio = xfs_stageio_take_reuse_folio(wm,
			xfs_stageio_raw_order);
	if (folio) {
		reused = true;
	} else {
		error = xfs_stageio_charge(wm, bytes, true, wi);
		if (error)
			return error;
		folio = folio_alloc(XFS_STAGEIO_ACCOUNT_GFP(GFP_NOFS) |
				__GFP_NORETRY | __GFP_NOWARN,
				xfs_stageio_raw_order);
		if (!folio) {
			xfs_stageio_uncharge(wm, bytes);
			return -ENOMEM;
		}
	}
	if (copy_folio_from_iter_atomic(folio, 0, bytes, &iter) != bytes) {
		if (!reused || !xfs_stageio_put_reuse_folio(wm, folio)) {
			folio_put(folio);
			xfs_stageio_uncharge(wm, bytes);
		}
		return -EFAULT;
	}
	flush_dcache_folio(folio);
	folio->index = pos >> PAGE_SHIFT;
	error = xfs_stageio_store_raw_folio(wi, folio->index, folio);
	if (error) {
		if (!reused || !xfs_stageio_put_reuse_folio(wm, folio)) {
			folio_put(folio);
			xfs_stageio_uncharge(wm, bytes);
		}
		return error;
	}
	iov_iter_advance(from, bytes);
	return 0;
}

static unsigned int
xfs_stageio_alloc_stage_pages(
	unsigned int		nr,
	struct page		**pages)
{
	gfp_t gfp = XFS_STAGEIO_ACCOUNT_GFP(GFP_NOFS);
	unsigned int allocated = 0;

	while (allocated < nr) {
		unsigned int remaining = nr - allocated;
		unsigned int order = min_t(unsigned int,
				ilog2(remaining), XFS_STAGEIO_STAGE_ALLOC_ORDER);
		struct page *page = NULL;
		unsigned int i;

		while (order) {
			page = alloc_pages(gfp | __GFP_NORETRY | __GFP_NOWARN,
					order);
			if (page)
				break;
			order--;
		}
		if (!page) {
			page = alloc_page(gfp);
			order = 0;
		}
		if (!page)
			break;
		if (order)
			split_page(page, order);
		for (i = 0; i < 1U << order; i++)
			pages[allocated++] = page + i;
	}
	return allocated;
}

static ssize_t
xfs_stageio_stage_raw_run(
	struct xfs_stageio_inode *wi,
	struct iov_iter		*from,
	loff_t			pos,
	size_t			count)
{
	struct xfs_stageio_mount *wm = wi->wm;
	struct page		*pages[XFS_STAGEIO_STAGE_BATCH_PAGES];
	size_t			done = 0;
	int			error = 0;

	while (done < count) {
		size_t large_bytes = PAGE_SIZE << xfs_stageio_raw_order;
		unsigned int nr, allocated, stored = 0;
		struct iov_iter iter;
		unsigned int i;

		if (xfs_stageio_raw_order &&
		    IS_ALIGNED(pos + done, large_bytes) &&
		    count - done >= large_bytes) {
			error = xfs_stageio_stage_large_folio(wi, from,
					pos + done);
			if (!error) {
				done += large_bytes;
				continue;
			}
			if (error != -ENOMEM && error != -EEXIST)
				break;
			error = 0;
		}

		nr = min_t(size_t,
				(count - done) >> PAGE_SHIFT,
				XFS_STAGEIO_STAGE_BATCH_PAGES);
		iter = *from;

		if (xfs_stageio_raw_order) {
			unsigned int boundary = (large_bytes -
					((pos + done) & (large_bytes - 1))) >>
					PAGE_SHIFT;

			nr = min(nr, boundary);
		}

		memset(pages, 0, sizeof(pages));
		error = xfs_stageio_charge(wm, nr * PAGE_SIZE, true, wi);
		if (error)
			break;
		allocated = xfs_stageio_alloc_stage_pages(nr, pages);
		if (allocated != nr) {
			error = -ENOMEM;
			goto out_release;
		}

		for (i = 0; i < nr; i++) {
			struct folio *folio = page_folio(pages[i]);
			void *addr = kmap_local_folio(folio, 0);

			if (copy_from_iter(addr, PAGE_SIZE, &iter) != PAGE_SIZE)
				error = -EFAULT;
			kunmap_local(addr);
			if (error)
				break;
		}
		if (error)
			goto out_release;

		for (i = 0; i < nr; i++) {
			struct folio *folio = page_folio(pages[i]);

			folio->index = (pos + done +
					((loff_t)i << PAGE_SHIFT)) >> PAGE_SHIFT;
			error = xfs_stageio_store_raw_folio(wi, folio->index,
					folio);
			if (error)
				break;
			pages[i] = NULL;
			stored++;
		}

out_release:
		for (i = stored; i < allocated; i++) {
			if (pages[i])
				folio_put(page_folio(pages[i]));
		}
		if (stored != nr)
			xfs_stageio_uncharge(wm,
					(nr - stored) * PAGE_SIZE);
		if (stored) {
			size_t bytes = (size_t)stored << PAGE_SHIFT;

			iov_iter_advance(from, bytes);
			done += bytes;
		}
		if (error)
			break;
	}
	return done ? done : error;
}

static int
xfs_stageio_stage_chunk(
	struct xfs_stageio_entry *entry,
	unsigned int		seg,
	unsigned int		offset,
	size_t			bytes,
	struct iov_iter		*from)
{
	struct xfs_stageio_mount *wm = entry->wi->wm;
	u8			*new_data = NULL;
	unsigned long		*new_valid = NULL;
	struct iov_iter		iter;
	size_t			copied;
	int			error;

	mutex_lock(&entry->lock);
	if (entry->state == XFS_STAGEIO_ENTRY_INVALID) {
		mutex_unlock(&entry->lock);
		return -EAGAIN;
	}
	if (entry->active_full) {
		void *addr;

		folio_lock(entry->active_full);
		addr = kmap_local_folio(entry->active_full, 0);
		iter = *from;
		copied = copy_from_iter(addr +
				(seg << XFS_STAGEIO_SEG_SHIFT) + offset,
				bytes, &iter);
		kunmap_local(addr);
		if (copied == bytes) {
			entry->seq = atomic64_inc_return(&entry->wi->seq);
			iov_iter_advance(from, bytes);
		}
		folio_unlock(entry->active_full);
		mutex_unlock(&entry->lock);
		return copied == bytes ? 0 : -EFAULT;
	}
	if (entry->active[seg] && entry->active_valid[seg]) {
		iter = *from;
		copied = copy_from_iter(entry->active[seg] + offset,
				bytes, &iter);
		if (copied == bytes) {
			bitmap_set(entry->active_valid[seg], offset, bytes);
			entry->active_mask |= BIT(seg);
			entry->seq = atomic64_inc_return(&entry->wi->seq);
			iov_iter_advance(from, bytes);
		}
		mutex_unlock(&entry->lock);
		return copied == bytes ? 0 : -EFAULT;
	}
	mutex_unlock(&entry->lock);

	error = xfs_stageio_charge(wm, XFS_STAGEIO_SEG_SIZE +
			XFS_STAGEIO_VALID_SIZE, true, entry->wi);
	if (error)
		return error;
	new_data = kmalloc(XFS_STAGEIO_SEG_SIZE,
			XFS_STAGEIO_ACCOUNT_GFP(GFP_NOFS));
	new_valid = bitmap_zalloc(XFS_STAGEIO_SEG_SIZE,
			XFS_STAGEIO_ACCOUNT_GFP(GFP_NOFS));
	if (!new_data || !new_valid) {
		kfree(new_data);
		bitmap_free(new_valid);
		xfs_stageio_uncharge(wm, XFS_STAGEIO_SEG_SIZE +
				XFS_STAGEIO_VALID_SIZE);
		return -ENOMEM;
	}
	iter = *from;
	copied = copy_from_iter(new_data + offset, bytes, &iter);
	if (copied != bytes) {
		kfree(new_data);
		bitmap_free(new_valid);
		xfs_stageio_uncharge(wm, XFS_STAGEIO_SEG_SIZE +
				XFS_STAGEIO_VALID_SIZE);
		return -EFAULT;
	}
	bitmap_set(new_valid, offset, bytes);

	mutex_lock(&entry->lock);
	if (entry->state == XFS_STAGEIO_ENTRY_INVALID) {
		mutex_unlock(&entry->lock);
		kfree(new_data);
		bitmap_free(new_valid);
		xfs_stageio_uncharge(wm, XFS_STAGEIO_SEG_SIZE +
				XFS_STAGEIO_VALID_SIZE);
		return -EAGAIN;
	}
	if (entry->active[seg] && entry->active_valid[seg]) {
		memcpy(entry->active[seg] + offset, new_data + offset, bytes);
		bitmap_set(entry->active_valid[seg], offset, bytes);
		kfree(new_data);
		bitmap_free(new_valid);
		xfs_stageio_uncharge(wm, XFS_STAGEIO_SEG_SIZE +
				XFS_STAGEIO_VALID_SIZE);
	} else {
		entry->active[seg] = new_data;
		entry->active_valid[seg] = new_valid;
	}
	entry->active_mask |= BIT(seg);
	entry->seq = atomic64_inc_return(&entry->wi->seq);
	iov_iter_advance(from, bytes);
	mutex_unlock(&entry->lock);
	return 0;
}

ssize_t
xfs_stageio_stage_iter(
	struct xfs_stageio_inode *wi,
	struct file		*file,
	struct iov_iter		*from,
	loff_t			pos,
	size_t			count)
{
	size_t			written = 0;
	unsigned long		delay;
	int			error = 0;

	atomic_inc(&wi->staging);
	xfs_stageio_inode_set_file(wi, file);

	while (written < count) {
		pgoff_t index = (pos + written) >> PAGE_SHIFT;
		unsigned int seg = offset_in_page(pos + written) >>
				XFS_STAGEIO_SEG_SHIFT;
		unsigned int chunk_offset = offset_in_page(pos + written) &
				(XFS_STAGEIO_SEG_SIZE - 1);
		size_t bytes = min_t(size_t, count - written,
				XFS_STAGEIO_SEG_SIZE - chunk_offset);
		struct xfs_stageio_entry *entry;
		bool full = !offset_in_page(pos + written) &&
				count - written >= PAGE_SIZE;

		if (full)
			bytes = PAGE_SIZE;

		if (full) {
			size_t run = round_down(count - written, PAGE_SIZE);
			ssize_t staged;

			staged = xfs_stageio_stage_raw_run(wi, from,
					pos + written, run);
			if (staged <= 0) {
				error = staged;
				break;
			}
			written += staged;
			if (staged != run)
				break;
			continue;
		}

retry_entry:
		entry = xfs_stageio_get_or_create_entry(wi, file, index);
		if (IS_ERR(entry)) {
			error = PTR_ERR(entry);
			break;
		}
		error = xfs_stageio_stage_chunk(entry, seg, chunk_offset,
					bytes, from);
		if (error == -EAGAIN) {
			xfs_stageio_entry_put(entry);
			cond_resched();
			goto retry_entry;
		}
		if (!error) {
			xfs_stageio_mark_dirty(entry);
		} else {
			struct file		*owned_file = NULL;
			bool			empty;

			mutex_lock(&entry->lock);
			empty = !entry->active_mask && !entry->flushing_mask &&
				!entry->active_full && !entry->prepared_folio;
			if (empty) {
				entry->state = XFS_STAGEIO_ENTRY_INVALID;
				owned_file = entry->io_file;
				entry->io_file = NULL;
			}
			mutex_unlock(&entry->lock);
			if (empty)
				xfs_stageio_remove_entry(entry);
			if (owned_file)
				xfs_stageio_owner_file_put(owned_file);
		}
		xfs_stageio_entry_put(entry);
		if (error)
			break;
		written += bytes;
	}

	atomic64_add(written, &xfs_stageio_global_accepted_bytes);
	atomic64_add(written, &wi->dirty_bytes);
	if (written) {
		delay = xfs_stageio_clean_handoff_writebehind_enabled() ? 0 :
			msecs_to_jiffies(xfs_stageio_delay_ms);
		if (atomic64_read(&wi->wm->total_dirty_bytes) >=
		    xfs_stageio_high_bytes * 3 / 4)
			delay = 0;
		xfs_stageio_kick_inode(wi, delay);
	}
	if (atomic_dec_and_test(&wi->staging)) {
		wake_up_all(&wi->wm->dirty_wait);
		xfs_stageio_inode_release_file_if_empty(wi);
	}
	return written ? written : error;
}

static int
xfs_stageio_overlay_folio(
	struct folio		*folio,
	struct iov_iter		*to,
	loff_t			pos,
	size_t			count)
{
	loff_t			folio_start;
	loff_t			copy_start;
	loff_t			copy_end;
	struct iov_iter		dst;
	size_t			bytes;

	if (!folio)
		return 0;
	folio_start = (loff_t)folio->index << PAGE_SHIFT;
	copy_start = max_t(loff_t, pos, folio_start);
	copy_end = min_t(loff_t, pos + count,
			folio_start + folio_size(folio));
	dst = *to;

	if (copy_start >= copy_end)
		return 0;
	bytes = copy_end - copy_start;
	iov_iter_advance(&dst, copy_start - pos);
	if (copy_folio_to_iter(folio, copy_start - folio_start, bytes,
			&dst) != bytes)
		return -EFAULT;
	return 0;
}

ssize_t
xfs_stageio_read_clean_iter(
	struct xfs_stageio_inode *wi,
	struct iov_iter		*to,
	loff_t			pos,
	size_t			count)
{
	struct iov_iter		iter = *to;
	loff_t			end = pos + count;
	ssize_t			copied = 0;

	if (!xfs_stageio_clean_recent || !count)
		return -EOPNOTSUPP;
	while (pos < end) {
		struct folio		*folio;
		loff_t			folio_start;
		size_t			offset, bytes;

		folio = xfs_stageio_lookup_clean_full(wi,
				pos >> PAGE_SHIFT);
		if (!folio) {
			atomic64_inc(
					&xfs_stageio_global_clean_recent_miss_reads);
			return -EOPNOTSUPP;
		}
		folio_start = (loff_t)folio->index << PAGE_SHIFT;
		offset = pos - folio_start;
		bytes = min_t(size_t, end - pos,
				folio_size(folio) - offset);
		if (copy_folio_to_iter(folio, offset, bytes, &iter) != bytes) {
			folio_put(folio);
			return -EFAULT;
		}
		folio_put(folio);
		pos += bytes;
		copied += bytes;
	}
	*to = iter;
	atomic64_add(copied, &xfs_stageio_global_clean_recent_hit_bytes);
	return copied;
}

static int
xfs_stageio_overlay_data(
	struct xfs_stageio_entry *entry,
	u8			**segments,
	unsigned long		**valid,
	unsigned long		mask,
	struct iov_iter		*to,
	loff_t			pos,
	size_t			count)
{
	loff_t			end = pos + count;
	unsigned int		i;

	for_each_set_bit(i, &mask, XFS_STAGEIO_NR_SEGS) {
		loff_t seg_start = ((loff_t)entry->page_index << PAGE_SHIFT) +
				((loff_t)i << XFS_STAGEIO_SEG_SHIFT);
		loff_t copy_start = max_t(loff_t, pos, seg_start);
		loff_t copy_end = min_t(loff_t, end,
				seg_start + XFS_STAGEIO_SEG_SIZE);
		unsigned int first, last, bit;

		if (copy_start >= copy_end || !segments[i] || !valid[i])
			continue;
		first = copy_start - seg_start;
		last = copy_end - seg_start;
		bit = find_next_bit(valid[i], last, first);
		while (bit < last) {
			struct iov_iter dst = *to;
			unsigned int run_end = find_next_zero_bit(valid[i],
					last, bit);
			size_t bytes = run_end - bit;

			iov_iter_advance(&dst, seg_start + bit - pos);
			if (copy_to_iter(segments[i] + bit, bytes, &dst) !=
			    bytes)
				return -EFAULT;
			bit = find_next_bit(valid[i], last, run_end);
		}
	}
	return 0;
}

int
xfs_stageio_overlay_iter(
	struct xfs_stageio_inode *wi,
	struct iov_iter		*to,
	loff_t			pos,
	size_t			count)
{
	pgoff_t		first = pos >> PAGE_SHIFT;
	pgoff_t		last = (pos + count - 1) >> PAGE_SHIFT;
	pgoff_t		index;
	int			error = 0;

	for (index = first; index <= last; index++) {
		struct xfs_stageio_entry *entry;
		struct folio		*folio;

		folio = xfs_stageio_lookup_full(wi, index);
		if (folio) {
			folio_lock(folio);
			error = xfs_stageio_overlay_folio(folio, to,
					pos, count);
			index = folio->index + folio_nr_pages(folio) - 1;
			folio_unlock(folio);
			folio_put(folio);
			if (error)
				break;
			continue;
		}

		entry = xfs_stageio_lookup_entry(wi, index);
		if (!entry)
			continue;
		mutex_lock(&entry->lock);
		if (entry->flushing_full)
			error = xfs_stageio_overlay_folio(entry->prepared_folio,
					to, pos, count);
		if (!error)
			error = xfs_stageio_overlay_data(entry, entry->flushing,
				entry->flushing_valid,
				entry->flushing_mask, to, pos, count);
		if (!error)
			error = xfs_stageio_overlay_folio(entry->active_full, to,
					pos, count);
		if (!error)
			error = xfs_stageio_overlay_data(entry, entry->active,
					entry->active_valid,
					entry->active_mask, to, pos, count);
		mutex_unlock(&entry->lock);
		xfs_stageio_entry_put(entry);
		if (error)
			break;
	}
	return error;
}

bool
xfs_stageio_inode_has_dirty(
	struct xfs_stageio_inode *wi)
{
	return atomic64_read(&wi->nr_entries) != 0;
}

static bool
xfs_stageio_region_range_has_entry(
	struct xfs_stageio_inode *wi,
	pgoff_t		first,
	pgoff_t		last)
{
	unsigned long		region_index;
	unsigned long		region_first = first >> XFS_STAGEIO_REGION_ORDER;
	unsigned long		region_last = last >> XFS_STAGEIO_REGION_ORDER;
	struct xfs_stageio_region *region;
	bool			found = false;
	unsigned int		i;

	xa_lock(&wi->regions);
	xa_for_each_range(&wi->regions, region_index, region,
			region_first, region_last) {
		struct xfs_stageio_region_slots *slots = region->slots;

		if (!xfs_stageio_region_valid(region)) {
			xfs_stageio_region_report_invalid(region_index);
			continue;
		}
		for (i = 0; slots && i < slots->nr; i++) {
			pgoff_t page_index = slots->entries[i]->page_index;

			if (page_index >= first && page_index <= last) {
				found = true;
				goto out_unlock;
			}
		}
	}
out_unlock:
	xa_unlock(&wi->regions);
	return found;
}

static unsigned long
xfs_stageio_region_range_entries(
	struct xfs_stageio_inode *wi,
	pgoff_t		first,
	pgoff_t		last)
{
	unsigned long		region_index;
	unsigned long		region_first = first >> XFS_STAGEIO_REGION_ORDER;
	unsigned long		region_last = last >> XFS_STAGEIO_REGION_ORDER;
	struct xfs_stageio_region *region;
	unsigned long		nr = 0;
	unsigned int		i;

	xa_lock(&wi->regions);
	xa_for_each_range(&wi->regions, region_index, region,
			region_first, region_last) {
		struct xfs_stageio_region_slots *slots = region->slots;

		if (!xfs_stageio_region_valid(region)) {
			xfs_stageio_region_report_invalid(region_index);
			continue;
		}
		for (i = 0; slots && i < slots->nr; i++) {
			pgoff_t page_index = slots->entries[i]->page_index;

			if (page_index >= first && page_index <= last)
				nr++;
		}
	}
	xa_unlock(&wi->regions);
	return nr;
}

static bool
xfs_stageio_index_range_has_entry(
	struct xfs_stageio_inode *wi,
	pgoff_t		first,
	pgoff_t		last)
{
	unsigned long		index = first;
	bool			found;

	rcu_read_lock();
	found = xa_find(&wi->entries, &index, last, XA_PRESENT) != NULL;
	rcu_read_unlock();
	return found || xfs_stageio_region_range_has_entry(wi, first, last);
}

static bool
xfs_stageio_index_range_has_dirty(
	struct xfs_stageio_inode *wi,
	pgoff_t		first,
	pgoff_t		last)
{
	unsigned long		index;
	void			*node;
	bool			found = false;

	xa_lock(&wi->entries);
	xa_for_each_range(&wi->entries, index, node, first, last) {
		if (xa_get_mark(&wi->entries, index,
				XFS_STAGEIO_XA_DIRTY) ||
		    xa_get_mark(&wi->entries, index,
				XFS_STAGEIO_XA_FLUSHING)) {
			found = true;
			break;
		}
	}
	xa_unlock(&wi->entries);
	return found || xfs_stageio_region_range_has_entry(wi, first, last);
}

static void
xfs_stageio_drop_clean_range(
	struct xfs_stageio_inode *wi,
	pgoff_t		first,
	pgoff_t		last)
{
	pgoff_t		index = first;

	while (index <= last) {
		struct folio		*folio;
		XA_STATE_ORDER(xas, &wi->entries, index, 0);
		pgoff_t			found = index;
		pgoff_t			next;
		void			*node;
		bool			removed = false;

		folio = xfs_stageio_lookup_next_clean_full(wi, &found, last);
		if (!folio)
			break;
		next = folio->index + folio_nr_pages(folio);
		xas_set_order(&xas, folio->index, folio_order(folio));
		xas_lock(&xas);
		node = xas_load(&xas);
		if (node == folio &&
		    xas_get_mark(&xas, XFS_STAGEIO_XA_FULL) &&
		    !xas_get_mark(&xas, XFS_STAGEIO_XA_DIRTY) &&
		    !xas_get_mark(&xas, XFS_STAGEIO_XA_FLUSHING)) {
			xas_store(&xas, NULL);
			removed = true;
		}
		xas_unlock(&xas);
		if (removed) {
			atomic64_dec(&wi->nr_entries);
			xfs_stageio_raw_entry_del();
			folio_put(folio);
		}
		folio_put(folio);
		if (next <= found || next > last)
			break;
		index = next;
	}
	xfs_stageio_inode_release_file_if_empty(wi);
}

static unsigned long
xfs_stageio_index_range_entries(
	struct xfs_stageio_inode *wi,
	pgoff_t		first,
	pgoff_t		last)
{
	unsigned long		index = first;
	unsigned long		nr = 0;

	rcu_read_lock();
	while (xa_find(&wi->entries, &index, last, XA_PRESENT)) {
		nr++;
		if (index == last)
			break;
		index++;
	}
	rcu_read_unlock();
	return nr + xfs_stageio_region_range_entries(wi, first, last);
}

static void
xfs_stageio_record_range_drain(
	unsigned long		entries,
	u64			ns)
{
	s64			old;

	atomic64_inc(&xfs_stageio_global_range_drain_calls);
	atomic64_add(entries, &xfs_stageio_global_range_drain_entries);
	atomic64_add(ns, &xfs_stageio_global_range_drain_ns);
	old = atomic64_read(&xfs_stageio_global_range_drain_max_ns);
	while (ns > old) {
		s64 seen = atomic64_cmpxchg(
				&xfs_stageio_global_range_drain_max_ns, old, ns);

		if (seen == old)
			break;
		old = seen;
	}
}

static int
xfs_stageio_index_range_drain(
	struct xfs_stageio_inode *wi,
	pgoff_t		first,
	pgoff_t		last,
	bool			account)
{
	unsigned long		entries;
	u64			start;
	int			ret;

	xfs_stageio_drop_clean_range(wi, first, last);
	if (!xfs_stageio_index_range_has_dirty(wi, first, last))
		return 0;

	mutex_lock(&wi->drain_mutex);
	entries = xfs_stageio_index_range_entries(wi, first, last);
	if (!entries) {
		mutex_unlock(&wi->drain_mutex);
		return 0;
	}
	start = ktime_get_ns();
	spin_lock(&wi->drain_lock);
	wi->drain_first = first;
	wi->drain_last = last;
	wi->drain_range_active = true;
	spin_unlock(&wi->drain_lock);

	mod_delayed_work(wi->wm->control_wq, &wi->flush_work, 0);
	ret = wait_event_killable(wi->wm->dirty_wait,
			!xfs_stageio_index_range_has_dirty(wi, first, last));
	xfs_stageio_drop_clean_range(wi, first, last);

	spin_lock(&wi->drain_lock);
	wi->drain_range_active = false;
	spin_unlock(&wi->drain_lock);
	if (xfs_stageio_index_range_has_dirty(wi, 0, ULONG_MAX))
		xfs_stageio_kick_inode(wi,
				msecs_to_jiffies(xfs_stageio_delay_ms));
	mutex_unlock(&wi->drain_mutex);

	if (account)
		xfs_stageio_record_range_drain(entries,
				ktime_get_ns() - start);
	return ret;
}

int
xfs_stageio_inode_drain(
	struct xfs_stageio_inode *wi)
{
	if (!xfs_stageio_inode_has_dirty(wi)) {
		xfs_stageio_inode_release_file_if_empty(wi);
		return 0;
	}
	return xfs_stageio_index_range_drain(wi, 0, ULONG_MAX, false);
}

int
xfs_stageio_range_drain(
	struct xfs_stageio_inode *wi,
	loff_t			pos,
	size_t			count)
{
	pgoff_t		first;
	pgoff_t		last;

	if (!count)
		return 0;
	first = pos >> PAGE_SHIFT;
	last = (pos + count - 1) >> PAGE_SHIFT;
	return xfs_stageio_index_range_drain(wi, first, last, true);
}

bool
xfs_stageio_range_has_entry(
	struct xfs_stageio_inode *wi,
	loff_t			pos,
	size_t			count)
{
	pgoff_t			first;
	pgoff_t			last;

	if (!count)
		return false;
	first = pos >> PAGE_SHIFT;
	last = (pos + count - 1) >> PAGE_SHIFT;
	return xfs_stageio_index_range_has_entry(wi, first, last);
}

bool
xfs_stageio_range_has_large_folio(
	struct xfs_stageio_inode *wi,
	loff_t			pos,
	size_t			count)
{
	pgoff_t			first;
	pgoff_t			last;

	if (!count)
		return false;
	first = pos >> PAGE_SHIFT;
	last = (pos + count - 1) >> PAGE_SHIFT;
	return xfs_stageio_range_has_large_index(wi, first, last);
}

void
xfs_stageio_read_lock(
	struct xfs_stageio_inode *wi)
{
	down_write(&wi->visibility_sem);
}

void
xfs_stageio_read_unlock(
	struct xfs_stageio_inode *wi)
{
	up_write(&wi->visibility_sem);
}

static bool
xfs_stageio_copy_cache_base(
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
xfs_stageio_apply_flushing(
	struct xfs_stageio_entry *entry,
	struct folio		*folio)
{
	void			*addr = kmap_local_folio(folio, 0);
	unsigned int		i, bit;

	mutex_lock(&entry->lock);
	for_each_set_bit(i, &entry->flushing_mask, XFS_STAGEIO_NR_SEGS) {
		if (!entry->flushing[i] || !entry->flushing_valid[i])
			continue;
		bit = find_first_bit(entry->flushing_valid[i],
				XFS_STAGEIO_SEG_SIZE);
		while (bit < XFS_STAGEIO_SEG_SIZE) {
			unsigned int run_end = find_next_zero_bit(
					entry->flushing_valid[i],
					XFS_STAGEIO_SEG_SIZE, bit);

			memcpy(addr + (i << XFS_STAGEIO_SEG_SHIFT) + bit,
					entry->flushing[i] + bit, run_end - bit);
			bit = find_next_bit(entry->flushing_valid[i],
					XFS_STAGEIO_SEG_SIZE, run_end);
		}
	}
	mutex_unlock(&entry->lock);
	kunmap_local(addr);
}

static bool
xfs_stageio_flushing_full(
	struct xfs_stageio_entry *entry)
{
	unsigned int		i;

	if (entry->flushing_mask != XFS_STAGEIO_FULL_MASK)
		return false;
	for (i = 0; i < XFS_STAGEIO_NR_SEGS; i++) {
		if (!entry->flushing_valid[i] ||
		    !bitmap_full(entry->flushing_valid[i],
			    XFS_STAGEIO_SEG_SIZE))
			return false;
	}
	return true;
}

static void
xfs_stageio_requeue_entry(
	struct xfs_stageio_entry *entry,
	bool			dirty)
{
	struct xfs_stageio_inode *wi = entry->wi;
	unsigned long		region_index = entry->page_index >>
					XFS_STAGEIO_REGION_ORDER;
	unsigned int		slot = entry->page_index & XFS_STAGEIO_REGION_MASK;
	struct xfs_stageio_region *region;
	struct xfs_stageio_region_slots *slots;
	unsigned int		rank;

	xa_lock(&wi->regions);
	region = xa_load(&wi->regions, region_index);
	slots = region ? region->slots : NULL;
	if (region && !xfs_stageio_region_valid(region)) {
		xfs_stageio_region_report_invalid(region_index);
	} else if (slots && (slots->present & BIT(slot))) {
		rank = hweight16(slots->present & (BIT(slot) - 1));
		if (slots->entries[rank] == entry) {
			entry->queued = false;
			if (dirty)
				region->dirty |= BIT(slot);
			else
				region->dirty &= ~BIT(slot);
			if (region->dirty)
				__xa_set_mark(&wi->regions, region_index,
						XFS_STAGEIO_XA_DIRTY);
			else
				__xa_clear_mark(&wi->regions, region_index,
						XFS_STAGEIO_XA_DIRTY);
		}
	}
	xa_unlock(&wi->regions);
}

static void
xfs_stageio_remove_entry(
	struct xfs_stageio_entry *entry)
{
	struct xfs_stageio_inode *wi = entry->wi;
	unsigned long		region_index = entry->page_index >>
					XFS_STAGEIO_REGION_ORDER;
	unsigned int		slot = entry->page_index & XFS_STAGEIO_REGION_MASK;
	struct xfs_stageio_region *region;
	struct xfs_stageio_region_slots *slots = NULL;
	unsigned int		rank;
	bool			removed = false, removed_region = false;

	xa_lock(&wi->regions);
	region = xa_load(&wi->regions, region_index);
	if (region && !xfs_stageio_region_valid(region)) {
		xfs_stageio_region_report_invalid(region_index);
		goto out_unlock;
	}
	if (!region || !region->slots ||
	    !(region->slots->present & BIT(slot)))
		goto out_unlock;
	slots = region->slots;
	rank = hweight16(slots->present & (BIT(slot) - 1));
	if (slots->entries[rank] != entry)
		goto out_unlock;
	if (rank + 1 < slots->nr)
		memmove(&slots->entries[rank], &slots->entries[rank + 1],
			(slots->nr - rank - 1) * sizeof(*slots->entries));
	slots->nr--;
	slots->present &= ~BIT(slot);
	region->dirty &= ~BIT(slot);
	if (!slots->nr) {
		region->slots = NULL;
		__xa_erase(&wi->regions, region_index);
		removed_region = true;
	} else if (!region->dirty) {
		__xa_clear_mark(&wi->regions, region_index,
				XFS_STAGEIO_XA_DIRTY);
	}
	removed = true;
out_unlock:
	xa_unlock(&wi->regions);
	if (removed_region) {
		xfs_stageio_region_slots_free(wi, slots);
		xfs_stageio_region_put(wi, region);
	}
	if (removed) {
		atomic64_dec(&wi->nr_entries);
		xfs_stageio_entry_put(entry);
		xfs_stageio_inode_release_file_if_empty(wi);
	}
}

static int
xfs_stageio_prepare_entry(
	struct xfs_stageio_entry *entry)
{
	struct xfs_stageio_mount *wm = entry->wi->wm;
	struct folio		*folio = NULL;
	struct file		*file = NULL;
	ssize_t			ret = 0;
	bool			charged = false, full;
	unsigned int		i;

	mutex_lock(&entry->lock);
	if (entry->state == XFS_STAGEIO_ENTRY_INVALID ||
	    (!entry->active_mask && !entry->active_full) ||
	    entry->flushing_mask ||
	    entry->prepared_folio) {
		ret = -EAGAIN;
		goto out_unlock;
	}
	if (entry->active_full) {
		entry->prepared_folio = entry->active_full;
		entry->active_full = NULL;
		entry->prepared_charged = true;
		entry->flushing_full = true;
		entry->state = XFS_STAGEIO_ENTRY_FLUSHING;
		entry->prepare_error = 0;
		atomic64_inc(&xfs_stageio_global_full_cancels);
		mutex_unlock(&entry->lock);
		return 0;
	}
	for (i = 0; i < XFS_STAGEIO_NR_SEGS; i++) {
		entry->flushing[i] = entry->active[i];
		entry->flushing_valid[i] = entry->active_valid[i];
		entry->active[i] = NULL;
		entry->active_valid[i] = NULL;
	}
	entry->flushing_mask = entry->active_mask;
	entry->active_mask = 0;
	entry->state = XFS_STAGEIO_ENTRY_FLUSHING;
	full = xfs_stageio_flushing_full(entry);
	file = xfs_stageio_temp_file_get(entry->io_file);
	mutex_unlock(&entry->lock);

	ret = xfs_stageio_charge(wm, PAGE_SIZE, false, NULL);
	if (ret)
		goto out;
	charged = true;
	folio = folio_alloc(XFS_STAGEIO_ACCOUNT_GFP(GFP_NOFS), 0);
	if (!folio) {
		xfs_stageio_uncharge(wm, PAGE_SIZE);
		charged = false;
		ret = -ENOMEM;
		goto out;
	}

	if (full) {
		folio_zero_range(folio, 0, PAGE_SIZE);
		atomic64_inc(&xfs_stageio_global_full_cancels);
	} else if (xfs_stageio_copy_cache_base(file, entry->page_index,
			folio)) {
		atomic64_inc(&xfs_stageio_global_cache_bases);
	} else {
		ret = -ENODATA;
	}

	if (!ret)
		xfs_stageio_apply_flushing(entry, folio);
out:
	mutex_lock(&entry->lock);
	entry->prepared_folio = folio;
	entry->prepared_charged = charged;
	entry->prepare_error = ret;
	mutex_unlock(&entry->lock);
	if (file)
		xfs_stageio_temp_file_put(file);
	return ret;

out_unlock:
	entry->prepare_error = ret;
	mutex_unlock(&entry->lock);
	return ret;
}

static void
xfs_stageio_prepare_batch(
	struct xfs_stageio_batch *batch)
{
	struct xfs_stageio_inode *wi = batch->entries[0]->wi;
	struct folio		**folios;
	struct file		*file;
	unsigned int		i, first, nr;
	ssize_t			ret;
	u64			start;

	for (i = 1; i < batch->nr; i++) {
		struct xfs_stageio_entry *entry = batch->entries[i];
		unsigned int j = i;

		while (j && batch->entries[j - 1]->page_index >
				entry->page_index) {
			batch->entries[j] = batch->entries[j - 1];
			j--;
		}
		batch->entries[j] = entry;
	}

	for (i = 0; i < batch->nr; i++) {
		struct xfs_stageio_entry *entry = batch->entries[i];

		start = ktime_get_ns();
		atomic64_add(start - READ_ONCE(entry->prepare_queued_ns),
				&xfs_stageio_global_prepare_queue_ns);
		xfs_stageio_prepare_entry(entry);
		atomic64_add(ktime_get_ns() - start,
				&xfs_stageio_global_prepare_ns);
	}

	folios = kcalloc(batch->nr, sizeof(*folios), GFP_NOFS);
	if (!folios) {
		for (i = 0; i < batch->nr; i++) {
			struct xfs_stageio_entry *entry = batch->entries[i];

			mutex_lock(&entry->lock);
			if (entry->prepare_error == -ENODATA)
				entry->prepare_error = -ENOMEM;
			mutex_unlock(&entry->lock);
		}
		goto queue_persist;
	}

	for (first = 0; first < batch->nr; first += nr) {
		struct xfs_stageio_entry *entry = batch->entries[first];

		mutex_lock(&entry->lock);
		if (entry->prepare_error != -ENODATA) {
			mutex_unlock(&entry->lock);
			nr = 1;
			continue;
		}
		file = xfs_stageio_temp_file_get(entry->io_file);
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
		atomic64_inc(&xfs_stageio_global_dio_read_calls);
		atomic64_add(nr, &xfs_stageio_global_dio_read_pages);
		start = ktime_get_ns();
		xfs_stageio_dio_start();
		ret = xfs_stageio_dio_read_folios(file,
				(loff_t)batch->entries[first]->page_index <<
				PAGE_SHIFT, folios, nr);
		xfs_stageio_dio_finish();
		atomic64_add(ktime_get_ns() - start,
				&xfs_stageio_global_dio_read_ns);
		xfs_stageio_temp_file_put(file);
		if (ret != (ssize_t)nr << PAGE_SHIFT)
			ret = ret < 0 ? ret : -EIO;
		for (i = 0; i < nr; i++) {
			entry = batch->entries[first + i];
			mutex_lock(&entry->lock);
			entry->prepare_error = ret < 0 ? ret : 0;
			mutex_unlock(&entry->lock);
			if (ret >= 0) {
				xfs_stageio_apply_flushing(entry, folios[i]);
				atomic64_inc(&xfs_stageio_global_base_reads);
				atomic64_add(PAGE_SIZE,
					&xfs_stageio_global_device_read_bytes);
			}
		}
	}
	kfree(folios);

queue_persist:
	batch->dispatch_queued_ns = ktime_get_ns();
	xfs_stageio_mpmc_enqueue(wi->wm, false, false, &batch->job);
}

static void
xfs_stageio_dispatch_batch(
	struct xfs_stageio_batch *batch)
{
	struct xfs_stageio_inode *wi = batch->entries[0]->wi;
	struct folio		**folios;
	struct file		*file;
	unsigned int		i, first, nr;
	ssize_t			ret;
	u64			dispatch_start = ktime_get_ns();
	u64			start;

	atomic64_add(ktime_get_ns() - batch->dispatch_queued_ns,
			&xfs_stageio_global_dispatch_queue_ns);

	folios = kcalloc(batch->nr, sizeof(*folios), GFP_NOFS);
	if (!folios) {
		for (i = 0; i < batch->nr; i++) {
			mutex_lock(&batch->entries[i]->lock);
			batch->entries[i]->prepare_error = -ENOMEM;
			mutex_unlock(&batch->entries[i]->lock);
			xfs_stageio_finish_entry(batch->entries[i]);
		}
		atomic_dec(&wi->batch_active);
		if (atomic64_read(&wi->nr_entries))
			xfs_stageio_kick_inode(wi, 0);
		xfs_stageio_inode_release_file_if_empty(wi);
		kfree(batch);
		return;
	}

	start = ktime_get_ns();
	down_read(&wi->visibility_sem);
	atomic64_add(ktime_get_ns() - start,
			&xfs_stageio_global_visibility_wait_ns);
	for (first = 0; first < batch->nr; first += nr) {
		struct xfs_stageio_entry *entry = batch->entries[first];

		mutex_lock(&entry->lock);
		if (entry->prepare_error) {
			mutex_unlock(&entry->lock);
			nr = 1;
			continue;
		}
		file = xfs_stageio_temp_file_get(entry->io_file);
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
		atomic64_inc(&xfs_stageio_global_dio_write_calls);
		atomic64_add(nr, &xfs_stageio_global_dio_write_pages);
		start = ktime_get_ns();
		xfs_stageio_dio_start();
		ret = xfs_stageio_dio_write_folios(file,
				(loff_t)batch->entries[first]->page_index <<
				PAGE_SHIFT, folios, nr);
		xfs_stageio_dio_finish();
		atomic64_add(ktime_get_ns() - start,
				&xfs_stageio_global_dio_write_ns);
		xfs_stageio_temp_file_put(file);
		if (ret != (ssize_t)nr << PAGE_SHIFT)
			ret = ret < 0 ? ret : -EIO;
		for (i = 0; i < nr; i++) {
			entry = batch->entries[first + i];
			mutex_lock(&entry->lock);
			entry->prepare_error = ret < 0 ? ret : PAGE_SIZE;
			mutex_unlock(&entry->lock);
			if (ret >= 0)
				atomic64_add(PAGE_SIZE,
					&xfs_stageio_global_device_write_bytes);
		}
	}
	up_read(&wi->visibility_sem);

	start = ktime_get_ns();
	for (i = 0; i < batch->nr; i++)
		xfs_stageio_finish_entry(batch->entries[i]);
	atomic64_add(ktime_get_ns() - start,
			&xfs_stageio_global_finish_ns);
	kfree(folios);
	atomic_dec(&wi->batch_active);
	if (atomic64_read(&wi->nr_entries))
		xfs_stageio_kick_inode(wi, 0);
	xfs_stageio_inode_release_file_if_empty(wi);
	atomic64_add(ktime_get_ns() - dispatch_start,
			&xfs_stageio_global_dispatch_ns);
	kfree(batch);
}

static void
xfs_stageio_merge_active_into_full(
	struct xfs_stageio_entry *entry,
	struct folio		*folio)
{
	struct xfs_stageio_mount *wm = entry->wi->wm;
	void			*addr = kmap_local_folio(folio, 0);
	unsigned int		i, bit;

	for_each_set_bit(i, &entry->active_mask, XFS_STAGEIO_NR_SEGS) {
		if (!entry->active[i] || !entry->active_valid[i])
			continue;
		bit = find_first_bit(entry->active_valid[i],
				XFS_STAGEIO_SEG_SIZE);
		while (bit < XFS_STAGEIO_SEG_SIZE) {
			unsigned int run_end = find_next_zero_bit(
					entry->active_valid[i],
					XFS_STAGEIO_SEG_SIZE, bit);

			memcpy(addr + (i << XFS_STAGEIO_SEG_SHIFT) + bit,
					entry->active[i] + bit, run_end - bit);
			bit = find_next_bit(entry->active_valid[i],
					XFS_STAGEIO_SEG_SIZE, run_end);
		}
		kfree(entry->active[i]);
		bitmap_free(entry->active_valid[i]);
		entry->active[i] = NULL;
		entry->active_valid[i] = NULL;
		xfs_stageio_uncharge(wm, XFS_STAGEIO_SEG_SIZE +
				XFS_STAGEIO_VALID_SIZE);
	}
	entry->active_mask = 0;
	kunmap_local(addr);
}

static bool
xfs_stageio_publish_clean_folio(
	struct address_space	*mapping,
	pgoff_t			index,
	struct folio		*folio)
{
	u64			start = ktime_get_ns();
	int			error;

	error = filemap_add_folio(mapping, folio, index,
			mapping_gfp_mask(mapping) | __GFP_WRITE);
	if (!error)
		folio_mark_uptodate(folio);
	folio_unlock(folio);
	xfs_stageio_record_clean_handoff(folio_size(folio),
			error ? 0 : folio_nr_pages(folio), !!error,
			ktime_get_ns() - start);
	return !error;
}

static void
xfs_stageio_finish_entry(
	struct xfs_stageio_entry *entry)
{
	struct xfs_stageio_inode *wi = entry->wi;
	struct xfs_stageio_mount *wm = wi->wm;
	struct folio		*folio;
	struct file		*owned_file = NULL;
	ssize_t			ret;
	bool			folio_charged;
	bool			flushing_full;
	bool			remove = false, requeue = false;
	unsigned int		i, bit;
	u64			flush_bytes = 0;

	mutex_lock(&entry->lock);
	folio = entry->prepared_folio;
	entry->prepared_folio = NULL;
	folio_charged = entry->prepared_charged;
	entry->prepared_charged = false;
	flushing_full = entry->flushing_full;
	entry->flushing_full = false;
	ret = entry->prepare_error;
	entry->prepare_error = 0;
	mutex_unlock(&entry->lock);

	if (!ret)
		ret = -EIO;

	mutex_lock(&entry->lock);
	if (ret == PAGE_SIZE) {
		if (flushing_full)
			flush_bytes += PAGE_SIZE;
		for (i = 0; i < XFS_STAGEIO_NR_SEGS; i++) {
			if (!entry->flushing[i])
				continue;
			if (entry->flushing_valid[i])
				flush_bytes += bitmap_weight(
						entry->flushing_valid[i],
						XFS_STAGEIO_SEG_SIZE);
			kfree(entry->flushing[i]);
			bitmap_free(entry->flushing_valid[i]);
			entry->flushing[i] = NULL;
			entry->flushing_valid[i] = NULL;
			xfs_stageio_uncharge(wm, XFS_STAGEIO_SEG_SIZE +
					XFS_STAGEIO_VALID_SIZE);
		}
		entry->flushing_mask = 0;
		atomic64_add(flush_bytes, &xfs_stageio_global_drained_bytes);

		if (entry->active_mask || entry->active_full) {
			entry->state = XFS_STAGEIO_ENTRY_DIRTY;
			requeue = true;
		} else {
			entry->state = XFS_STAGEIO_ENTRY_INVALID;
			owned_file = entry->io_file;
			entry->io_file = NULL;
			remove = true;
		}
	} else if (flushing_full) {
		if (!entry->active_full) {
			xfs_stageio_merge_active_into_full(entry, folio);
			entry->active_full = folio;
			folio = NULL;
			folio_charged = false;
		}
		entry->flushing_mask = 0;
		entry->state = XFS_STAGEIO_ENTRY_DIRTY;
		atomic64_inc(&xfs_stageio_global_flush_errors);
		requeue = true;
	} else {
		for (i = 0; i < XFS_STAGEIO_NR_SEGS; i++) {
			if (!entry->flushing[i])
				continue;
			if (entry->active_full) {
				kfree(entry->flushing[i]);
				bitmap_free(entry->flushing_valid[i]);
				xfs_stageio_uncharge(wm,
						XFS_STAGEIO_SEG_SIZE +
						XFS_STAGEIO_VALID_SIZE);
			} else if (entry->active[i] && entry->active_valid[i]) {
				for_each_set_bit(bit, entry->flushing_valid[i],
						XFS_STAGEIO_SEG_SIZE) {
					if (test_bit(bit, entry->active_valid[i]))
						continue;
					entry->active[i][bit] =
						entry->flushing[i][bit];
					set_bit(bit, entry->active_valid[i]);
				}
				kfree(entry->flushing[i]);
				bitmap_free(entry->flushing_valid[i]);
				xfs_stageio_uncharge(wm,
						XFS_STAGEIO_SEG_SIZE +
						XFS_STAGEIO_VALID_SIZE);
			} else {
				entry->active[i] = entry->flushing[i];
				entry->active_valid[i] =
						entry->flushing_valid[i];
				entry->active_mask |= BIT(i);
			}
			entry->flushing[i] = NULL;
			entry->flushing_valid[i] = NULL;
		}
		entry->flushing_mask = 0;
		entry->state = XFS_STAGEIO_ENTRY_DIRTY;
		atomic64_inc(&xfs_stageio_global_flush_errors);
		requeue = true;
	}
	entry->batch = NULL;
	mutex_unlock(&entry->lock);

	xfs_stageio_requeue_entry(entry, requeue);
	if (remove)
		xfs_stageio_remove_entry(entry);
	if (remove && folio && owned_file &&
	    xfs_stageio_clean_handoff_enabled())
		xfs_stageio_publish_clean_folio(owned_file->f_mapping,
				entry->page_index, folio);

	if (owned_file)
		xfs_stageio_owner_file_put(owned_file);
	if (folio) {
		folio_put(folio);
		if (folio_charged)
			xfs_stageio_uncharge(wm, PAGE_SIZE);
	}
	xfs_stageio_entry_put(entry);
}

static void
xfs_stageio_finish_raw_item(
	struct xfs_stageio_raw_batch *batch,
	struct xfs_stageio_raw_item *item,
	ssize_t			ret)
{
	struct xfs_stageio_inode *wi = batch->wi;
	XA_STATE_ORDER(xas, &wi->entries, item->page_index,
			folio_order(item->folio));
	void			*node;
	bool			removed = false;
	bool			clean_pending = false;
	bool			clean_recent = false;
	size_t			bytes = folio_size(item->folio);

	xas_lock(&xas);
	node = xas_load(&xas);
	if (node == item->folio &&
	    xas_get_mark(&xas, XFS_STAGEIO_XA_FULL) &&
	    xas_get_mark(&xas, XFS_STAGEIO_XA_FLUSHING)) {
		if (ret == bytes) {
			if (xfs_stageio_clean_recent) {
				if (WARN_ON_ONCE(
					    folio_test_private(item->folio))) {
					xas_store(&xas, NULL);
					removed = true;
				} else {
					folio_attach_private(item->folio, item);
					xas_clear_mark(&xas,
						XFS_STAGEIO_XA_FLUSHING);
					item->clean_recent = true;
					clean_recent = true;
				}
			} else if (xfs_stageio_clean_handoff_enabled() &&
			    xfs_stageio_clean_handoff_async_enabled()) {
				xas_clear_mark(&xas,
						XFS_STAGEIO_XA_FLUSHING);
				item->handoff = true;
				clean_pending = true;
			} else {
				xas_store(&xas, NULL);
				removed = true;
			}
		} else {
			xas_clear_mark(&xas, XFS_STAGEIO_XA_FLUSHING);
			xas_set_mark(&xas, XFS_STAGEIO_XA_DIRTY);
		}
	}
	xas_unlock(&xas);

	if (removed || clean_pending || clean_recent) {
		atomic64_add(bytes, &xfs_stageio_global_drained_bytes);
		atomic64_add(bytes,
				&xfs_stageio_global_device_write_bytes);
		if (!clean_recent)
			xfs_stageio_uncharge(wi->wm, bytes);
	}
	if (removed) {
		if (xfs_stageio_clean_handoff_enabled())
			xfs_stageio_publish_clean_folio(batch->file->f_mapping,
					item->page_index, item->folio);
		atomic64_dec(&wi->nr_entries);
		xfs_stageio_raw_entry_del();
		folio_put(item->folio);
	} else if (clean_pending) {
		xfs_stageio_async_handoff_queue(bytes);
	} else if (clean_recent) {
		batch->clean_bytes += bytes;
	} else if (ret != bytes) {
		atomic64_inc(&xfs_stageio_global_flush_errors);
	} else {
		WARN_ON_ONCE(1);
	}
	if (!clean_pending && !clean_recent)
		folio_put(item->folio);
	wake_up_all(&wi->wm->dirty_wait);
}

static void
xfs_stageio_raw_batch_handoff_work(
	struct work_struct	*work)
{
	struct xfs_stageio_raw_batch *batch = container_of(work,
			struct xfs_stageio_raw_batch, handoff_work);
	struct xfs_stageio_inode *wi = batch->wi;
	unsigned int		i;

	down_write(&wi->visibility_sem);

	for (i = 0; i < batch->nr; i++) {
		struct xfs_stageio_raw_item *item = &batch->items[i];
		XA_STATE_ORDER(xas, &wi->entries, item->page_index,
				folio_order(item->folio));
		size_t bytes;
		void *node;
		bool removed = false;

		if (!item->handoff)
			continue;
		bytes = folio_size(item->folio);
		xfs_stageio_publish_clean_folio(batch->file->f_mapping,
				item->page_index, item->folio);

		xas_lock(&xas);
		node = xas_load(&xas);
		if (node == item->folio &&
		    xas_get_mark(&xas, XFS_STAGEIO_XA_FULL) &&
		    !xas_get_mark(&xas, XFS_STAGEIO_XA_DIRTY) &&
		    !xas_get_mark(&xas, XFS_STAGEIO_XA_FLUSHING)) {
			xas_store(&xas, NULL);
			removed = true;
		}
		xas_unlock(&xas);

		if (WARN_ON_ONCE(!removed)) {
			folio_put(item->folio);
			continue;
		}
		atomic64_dec(&wi->nr_entries);
		xfs_stageio_raw_entry_del();
		xfs_stageio_async_handoff_complete(bytes);
		folio_put(item->folio);
		folio_put(item->folio);
		wake_up_all(&wi->wm->dirty_wait);
	}
	up_write(&wi->visibility_sem);

	xfs_stageio_inode_release_file_if_empty(wi);
	xfs_stageio_temp_file_put(batch->file);
	xfs_stageio_inode_put(wi);
	kfree(batch);
}

static void
xfs_stageio_dispatch_raw_batch(
	struct xfs_stageio_raw_batch *batch)
{
	struct xfs_stageio_inode *wi = batch->wi;
	struct folio		**folios;
	unsigned int		first, i, nr;
	ssize_t			ret;
	u64			start;

	folios = kcalloc(batch->nr, sizeof(*folios), GFP_NOFS);
	if (!folios) {
		for (i = 0; i < batch->nr; i++)
			xfs_stageio_finish_raw_item(batch, &batch->items[i],
					-ENOMEM);
		goto out;
	}

	start = ktime_get_ns();
	down_read(&wi->visibility_sem);
	atomic64_add(ktime_get_ns() - start,
			&xfs_stageio_global_visibility_wait_ns);
	for (first = 0; first < batch->nr; first += nr) {
		size_t bytes = folio_size(batch->items[first].folio);

		folios[0] = batch->items[first].folio;
		for (nr = 1; first + nr < batch->nr; nr++) {
			if (batch->items[first + nr].page_index !=
			    batch->items[first + nr - 1].page_index +
			    folio_nr_pages(batch->items[first + nr - 1].folio))
				break;
			folios[nr] = batch->items[first + nr].folio;
			bytes += folio_size(folios[nr]);
		}

		atomic64_inc(&xfs_stageio_global_dio_write_calls);
		atomic64_add(bytes >> PAGE_SHIFT,
				&xfs_stageio_global_dio_write_pages);
		start = ktime_get_ns();
		xfs_stageio_dio_start();
		ret = xfs_stageio_dio_write_folios(batch->file,
				(loff_t)batch->items[first].page_index <<
				PAGE_SHIFT, folios, nr);
		xfs_stageio_dio_finish();
		atomic64_add(ktime_get_ns() - start,
				&xfs_stageio_global_dio_write_ns);
		if (ret != bytes)
			ret = ret < 0 ? ret : -EIO;
		for (i = 0; i < nr; i++)
			xfs_stageio_finish_raw_item(batch,
					&batch->items[first + i],
					ret < 0 ? ret :
					folio_size(batch->items[first + i].folio));
	}
	up_read(&wi->visibility_sem);
	kfree(folios);

out:
	for (i = 0; i < batch->nr; i++)
		if (batch->items[i].handoff ||
		    batch->items[i].clean_recent)
			break;
	atomic_dec(&wi->batch_active);
	wake_up_all(&wi->wm->dirty_wait);
	if (atomic64_read(&wi->nr_entries))
		xfs_stageio_kick_inode(wi, 0);
	if (i < batch->nr) {
		if (batch->clean_bytes) {
			xfs_stageio_retain_clean_batch(batch);
			return;
		}
		refcount_inc(&wi->refcount);
		if (WARN_ON_ONCE(!queue_work(wi->wm->handoff_wq,
				&batch->handoff_work)))
			xfs_stageio_raw_batch_handoff_work(
					&batch->handoff_work);
		return;
	}
	xfs_stageio_inode_release_file_if_empty(wi);
	xfs_stageio_temp_file_put(batch->file);
	kfree(batch);
}

static void
xfs_stageio_mpmc_worker(
	struct work_struct	*work)
{
	struct xfs_stageio_mpmc_worker *worker = container_of(work,
			struct xfs_stageio_mpmc_worker, work);
	struct xfs_stageio_mpmc_ring *ring = worker->prepare ?
			worker->wm->prepare_ring : worker->wm->persist_ring;
	struct xfs_stageio_flush_job *job;
	atomic64_t		*active = worker->prepare ?
			&xfs_stageio_global_prepare_workers_active :
			&xfs_stageio_global_persist_workers_active;
	atomic64_t		*peak = worker->prepare ?
			&xfs_stageio_global_prepare_workers_peak :
			&xfs_stageio_global_persist_workers_peak;

	while ((job = xfs_stageio_mpmc_try_dequeue(ring)) != NULL) {
		xfs_stageio_atomic64_update_peak(peak,
				atomic64_inc_return(active));
		if (worker->prepare) {
			if (WARN_ON_ONCE(job->type !=
					XFS_STAGEIO_FLUSH_PARTIAL))
				goto next;
			xfs_stageio_prepare_batch(container_of(job,
					struct xfs_stageio_batch, job));
		} else if (job->type == XFS_STAGEIO_FLUSH_PARTIAL) {
			xfs_stageio_dispatch_batch(container_of(job,
					struct xfs_stageio_batch, job));
		} else if (job->type == XFS_STAGEIO_FLUSH_RAW) {
			if (!xfs_stageio_raw_persist_try_acquire(worker->wm)) {
				xfs_stageio_mpmc_enqueue(worker->wm, false, true,
						job);
				atomic64_dec(active);
				break;
			}
			xfs_stageio_dispatch_raw_batch(container_of(job,
					struct xfs_stageio_raw_batch, job));
			xfs_stageio_raw_persist_release(worker->wm);
		} else {
			WARN_ON_ONCE(1);
		}
next:
		atomic64_dec(active);
		cond_resched();
	}
}

enum xfs_stageio_raw_queue_result {
	XFS_STAGEIO_RAW_NONE,
	XFS_STAGEIO_RAW_QUEUED,
	XFS_STAGEIO_RAW_BUSY,
};

static void
xfs_stageio_flush_bounds(
	struct xfs_stageio_inode *wi,
	pgoff_t		*first,
	pgoff_t		*last)
{
	spin_lock(&wi->drain_lock);
	if (wi->drain_range_active) {
		*first = wi->drain_first;
		*last = wi->drain_last;
	} else {
		*first = 0;
		*last = ULONG_MAX;
	}
	spin_unlock(&wi->drain_lock);
}

static enum xfs_stageio_raw_queue_result
xfs_stageio_queue_raw_batch(
	struct xfs_stageio_inode *wi,
	pgoff_t		first,
	pgoff_t		last)
{
	struct xfs_stageio_raw_batch *batch;
	XA_STATE(xas, &wi->entries, first);
	struct folio		*folio;
	unsigned int		queued = 0, queued_pages = 0;
	int			active;

	active = atomic_inc_return(&wi->batch_active);
	if (active > xfs_stageio_qd) {
		atomic_dec(&wi->batch_active);
		return XFS_STAGEIO_RAW_BUSY;
	}
	xfs_stageio_update_batch_peak(active);
	batch = kzalloc(struct_size(batch, items, xfs_stageio_batch),
			XFS_STAGEIO_ACCOUNT_GFP(GFP_NOFS));
	if (!batch) {
		atomic_dec(&wi->batch_active);
		return XFS_STAGEIO_RAW_NONE;
	}
	batch->wi = wi;
	INIT_LIST_HEAD(&batch->clean_node);
	INIT_LIST_HEAD(&batch->clean_inode_node);
	atomic_set(&batch->clean_access_state,
			XFS_STAGEIO_CLEAN_PROBATION_NEW);
	batch->file = xfs_stageio_inode_temp_file_get(wi);
	if (!batch->file) {
		kfree(batch);
		atomic_dec(&wi->batch_active);
		return XFS_STAGEIO_RAW_NONE;
	}

	xas_lock(&xas);
	xas_for_each_marked(&xas, folio, last,
			XFS_STAGEIO_XA_DIRTY) {
		unsigned int pages;

		if (!xas_get_mark(&xas, XFS_STAGEIO_XA_FULL))
			continue;
		pages = folio_nr_pages(folio);
		if (queued && queued_pages + pages > xfs_stageio_batch)
			break;
		folio_get(folio);
		xas_clear_mark(&xas, XFS_STAGEIO_XA_DIRTY);
		xas_set_mark(&xas, XFS_STAGEIO_XA_FLUSHING);
		batch->items[queued].page_index = folio->index;
		batch->items[queued].folio = folio;
		batch->items[queued].batch = batch;
		atomic_long_set(&batch->items[queued].clean_seen_pages, 0);
		queued++;
		queued_pages += pages;
		if (queued_pages >= xfs_stageio_batch)
			break;
	}
	xas_unlock(&xas);

	if (!queued) {
		xfs_stageio_temp_file_put(batch->file);
		kfree(batch);
		atomic_dec(&wi->batch_active);
		return XFS_STAGEIO_RAW_NONE;
	}
	batch->nr = queued;
	atomic64_inc(&xfs_stageio_global_batches);
	atomic64_add(queued_pages, &xfs_stageio_global_batch_pages);
	batch->job.type = XFS_STAGEIO_FLUSH_RAW;
	if (xfs_stageio_clean_recent)
		INIT_WORK(&batch->handoff_work,
				xfs_stageio_raw_batch_clean_reclaim_work);
	else
		INIT_WORK(&batch->handoff_work,
				xfs_stageio_raw_batch_handoff_work);
	xfs_stageio_mpmc_enqueue(wi->wm, false, true, &batch->job);
	return XFS_STAGEIO_RAW_QUEUED;
}

static void
xfs_stageio_inode_flush_work(
	struct work_struct	*work)
{
	struct xfs_stageio_inode *wi = container_of(to_delayed_work(work),
			struct xfs_stageio_inode, flush_work);
	struct xfs_stageio_batch *batch;
	struct xfs_stageio_entry *entry;
	pgoff_t		first, last;
	unsigned int		queued = 0, i;
	u64			scan_start;
	int			active;
	bool			more_dirty;
	enum xfs_stageio_raw_queue_result raw_result;

retry_raw:
	xfs_stageio_flush_bounds(wi, &first, &last);
	raw_result = xfs_stageio_queue_raw_batch(wi, first, last);
	if (raw_result == XFS_STAGEIO_RAW_BUSY) {
		wait_event(wi->wm->dirty_wait,
				atomic_read(&wi->batch_active) < xfs_stageio_qd);
		goto retry_raw;
	}
	if (raw_result == XFS_STAGEIO_RAW_QUEUED)
		goto retry_raw;

	active = atomic_inc_return(&wi->batch_active);
	if (active > xfs_stageio_qd) {
		atomic_dec(&wi->batch_active);
		xfs_stageio_kick_inode(wi, msecs_to_jiffies(1));
		return;
	}
	xfs_stageio_update_batch_peak(active);
	batch = kzalloc(struct_size(batch, entries, xfs_stageio_batch),
			XFS_STAGEIO_ACCOUNT_GFP(GFP_NOFS));
	if (!batch) {
		atomic_dec(&wi->batch_active);
		xfs_stageio_kick_inode(wi, msecs_to_jiffies(1));
		return;
	}

	scan_start = ktime_get_ns();
	xa_lock(&wi->regions);
	{
		XA_STATE(xas, &wi->regions,
				first >> XFS_STAGEIO_REGION_ORDER);
		unsigned long region_last = last >> XFS_STAGEIO_REGION_ORDER;
		struct xfs_stageio_region *region;

		xas_for_each_marked(&xas, region, region_last,
				XFS_STAGEIO_XA_DIRTY) {
			struct xfs_stageio_region_slots *slots = region->slots;
			unsigned long dirty = region->dirty;
			unsigned int slot;

			if (!xfs_stageio_region_valid(region)) {
				xfs_stageio_region_report_invalid(xas.xa_index);
				xas_clear_mark(&xas, XFS_STAGEIO_XA_DIRTY);
				continue;
			}

			for_each_set_bit(slot, &dirty,
					XFS_STAGEIO_REGION_PAGES) {
				pgoff_t page_index = (xas.xa_index <<
						XFS_STAGEIO_REGION_ORDER) + slot;
				unsigned int rank;

				if (page_index < first || page_index > last)
					continue;
				rank = hweight16(slots->present &
						(BIT(slot) - 1));
				entry = slots->entries[rank];
				if (!xfs_stageio_entry_get(entry)) {
					xfs_stageio_region_report_invalid(
							xas.xa_index);
					break;
				}
				region->dirty &= ~BIT(slot);
				entry->queued = true;
				batch->entries[queued++] = entry;
				if (queued == xfs_stageio_batch)
					break;
			}
			if (!region->dirty)
				xas_clear_mark(&xas, XFS_STAGEIO_XA_DIRTY);
			if (queued == xfs_stageio_batch)
				break;
		}
	}
	more_dirty = xa_marked(&wi->entries, XFS_STAGEIO_XA_DIRTY) ||
		xa_marked(&wi->regions, XFS_STAGEIO_XA_DIRTY);
	xa_unlock(&wi->regions);
	atomic64_add(ktime_get_ns() - scan_start,
			&xfs_stageio_global_scan_ns);

	if (!queued) {
		kfree(batch);
		atomic_dec(&wi->batch_active);
		xfs_stageio_inode_release_file_if_empty(wi);
		return;
	}
	batch->nr = queued;
	atomic64_inc(&xfs_stageio_global_batches);
	atomic64_add(queued, &xfs_stageio_global_batch_pages);
	batch->job.type = XFS_STAGEIO_FLUSH_PARTIAL;
	for (i = 0; i < queued; i++) {
		mutex_lock(&batch->entries[i]->lock);
		batch->entries[i]->batch = batch;
		batch->entries[i]->prepare_queued_ns = ktime_get_ns();
		mutex_unlock(&batch->entries[i]->lock);
	}
	xfs_stageio_mpmc_enqueue(wi->wm, true, false, &batch->job);
	if (more_dirty)
		xfs_stageio_kick_inode(wi, 0);
}
