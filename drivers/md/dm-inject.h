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

#define DM_MSG_PREFIX "inject"
#define RW(op) (((op)==REQ_OP_READ) ? "R" : \
				(((op)==REQ_OP_WRITE) ? "W" : "U")) 

// Supported filesystems
enum fs {
	FS_UNKNOWN,
	FS_EXT4,
	FS_F2FS
};

enum corrupt_type {
	INJECT_SECTOR,
	INJECT_BLOCK,
	INJECT_INODE
};

struct inject_rec {
	struct list_head list;
	enum corrupt_type type;
	union {
		sector_t sector_num;
		block_t block_num;
		nid_t inode_num;
	};
};

// info about the target
struct inject_c {
	//context for device mapper
	struct dm_dev *dev;
	struct block_device *src_bdev;
	sector_t start;
	//info related to corruption
	unsigned int num_corrupt;
	sector_t *corrupt_sector;
	block_t *corrupt_block;
	struct list_head inject_list;
	struct f2fs_sb_info *f2fs_sbi;
};

static inline struct super_block *get_bdev_sb(struct inject_c *ic)
{
	if(ic && ic->src_bdev)
		return ic->src_bdev->bd_super;
	return NULL;
}

static inline enum fs check_fs_type(struct super_block *sb)
{
	struct file_system_type *type;
	int magic;
	if(sb == NULL) {
		DMDEBUG("%s unknown fs", __func__);
		return FS_UNKNOWN;
	}
	type = sb->s_type;
	magic = sb->s_magic;
	if(type == NULL) {
		DMDEBUG("%s unknown fs", __func__);
		return FS_UNKNOWN;
	}
	//else if(strcmp(type->name, "ext4") == 0) {
	else if(magic == EXT4_SUPER_MAGIC) {
		//DMDEBUG("%s ext4", __func__);
		return FS_EXT4;
	}
	//else if(strcmp(type->name, "f2fs") == 0) {
	else if(magic == F2FS_SUPER_MAGIC) {
		//DMDEBUG("%s f2fs", __func__);
		return FS_F2FS;
	}
	else {
		DMDEBUG("%s unknown fs", __func__);
		return FS_UNKNOWN;
	}
}
#define IS_F2FS(sb) ((sb) && ((sb)->s_magic == F2FS_SUPER_MAGIC))
#define IS_EXT4(sb) ((sb) && ((sb)->s_magic == EXT4_SUPER_MAGIC))

bool f2fs_corrupt_block_to_dev(struct inject_c *ic, struct bio *bio);
bool f2fs_corrupt_block_from_dev(struct inject_c *ic, struct bio *bio);
#endif
