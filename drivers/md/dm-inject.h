/*
 * dm-inject device mapper target
 * referencing dm-linear, dm-zero etc
 */

#ifndef DM_INJECT_H
#define DM_INJECT_H

#include <linux/device-mapper.h>
#include <linux/module.h>
#include <linux/init.h>
#include <linux/bio.h>
#include <linux/fs.h>
#include <linux/f2fs_fs.h>
#include <linux/magic.h>
#include <linux/list.h>
#include "../../fs/f2fs/f2fs.h"
#include "../../fs/f2fs/segment.h"
#include "../../fs/f2fs/node.h"

#define RW(op) (((op)==REQ_OP_READ) ? "R" : \
				(((op)==REQ_OP_WRITE) ? "W" : "U")) 

/*	iterate through bvec inside bio using only the sizes
	doesn't "advance" or otherwise change the iterator of the bio
	1) If intercepting bio before issued to device (e.g. write)
	bi_size is the total size
	2) If intercepting after issue (end_io path, e.g. read)
	then bi_idx * PAGE_SIZE = total size since bi_idx is advanced
	past the last bvec
	3) Partially completed would have value in bi_bvec_done.
	This is reset when advancing to next bvec
	Check bvec_iter_advance for details on how these are changed.
*/
#define for_each_bvec_no_advance(size, bvec, bio, start)	\
for(size = (start), bvec = (bio)->bi_io_vec;				\
	size < ((bio)->bi_iter.bi_size + (bio)->bi_iter.bi_bvec_done \
	+ (bio)->bi_iter.bi_idx*PAGE_SIZE);						\
	size += (bvec)->bv_len, (bvec)++)

// Supported filesystems
enum fs {
	FS_UNKNOWN,
	FS_EXT4,
	FS_F2FS
};

//Generic classifications
#define DM_INJECT_NONE			0x0000
#define	DM_INJECT_SECTOR		0x0001
#define	DM_INJECT_BLOCK			0x0002

//F2FS Meta area
#define DM_INJECT_F2FS_SB		0x0004
#define DM_INJECT_F2FS_CP		0x0008
#define DM_INJECT_F2FS_SIT		0x0010
#define DM_INJECT_F2FS_NAT		0x0020
#define	DM_INJECT_F2FS_SSA		0x0040
#define DM_INJECT_F2FS_META 	(DM_INJECT_F2FS_SB | DM_INJECT_F2FS_CP | \
			DM_INJECT_F2FS_SIT | DM_INJECT_F2FS_NAT | DM_INJECT_F2FS_SSA)

//F2FS Main area
#define DM_INJECT_F2FS_INODE	0x0100
#define DM_INJECT_F2FS_DNODE	0x0200
#define DM_INJECT_F2FS_INDNODE	0x0400
#define DM_INJECT_F2FS_NODE 	(DM_INJECT_F2FS_INODE | DM_INJECT_F2FS_DNODE | DM_INJECT_F2FS_INDNODE)
#define DM_INJECT_F2FS_DATA		0x0800
#define DM_INJECT_F2FS_MAIN		(DM_INJECT_F2FS_NODE | DM_INJECT_F2FS_DATA)

enum inject_method {
	DM_INJECT_FAIL_BLOCK,
	DM_INJECT_ZERO,
	DM_INJECT_RAND
};

struct inject_rec {
	struct list_head list;
	int type;
	int op;
	union {
		sector_t sector_num;
		block_t block_num;
		struct {
		nid_t inode_num;
		int offset;
		}
	};
	char inode_member[64];
};

struct inject_fs_type {
	char *name;
	struct module *module;
	void *context;
};

// info about the target
struct inject_c {
	//context for device mapper
	struct dm_dev *dev; //underlying device
	struct block_device *src_bdev; //source of bio requests
	sector_t start;
	//info related to corruption
	unsigned int num_corrupt;
	sector_t *corrupt_sector;
	block_t *corrupt_block;
	bool inject_enable;
	struct list_head inject_list;
	struct f2fs_sb_info *f2fs_sbi;
	//prior to full mount, we can already get partial sbi
	//via reading block 0 directly in inject_ctr
	//use it until the actual sb is setup properly
	bool partial_sbi;
	struct f2fs_sb_info f2fs_sbi_copy;
	struct f2fs_super_block f2fs_sb_copy;
	struct inject_fs_type fs;
};

static inline struct super_block *get_bdev_sb(struct inject_c *ic)
{
	if(ic && ic->src_bdev)
		return ic->src_bdev->bd_super;
	return NULL;
}

#define IS_F2FS(sb) ((sb) && ((sb)->s_magic == F2FS_SUPER_MAGIC))
#define IS_EXT4(sb) ((sb) && ((sb)->s_magic == EXT4_SUPER_MAGIC))

bool f2fs_corrupt_block_to_dev(struct inject_c *ic, struct bio *bio);
bool f2fs_corrupt_block_from_dev(struct inject_c *ic, struct bio *bio);
int f2fs_corrupt_data_to_dev(struct inject_c *ic, struct bio *bio);
int f2fs_corrupt_data_from_dev(struct inject_c *ic, struct bio *bio);
void init_sb_info(struct f2fs_sb_info *sbi);

int dm_register_inject_fs(struct inject_fs_type *fs);
int dm_unregister_inject_fs(struct inject_fs_type *fs);
#endif
