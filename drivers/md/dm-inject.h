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
#include <linux/magic.h>
#include <linux/list.h>

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
		u32 block_num;
		struct {
		u32 inode_num;
		int offset;
		};
	};
	char inode_member[64];
};

struct inject_fs_type;

// info about the target
struct inject_c {
	//context for device mapper
	struct dm_dev *dev; //underlying device
	struct block_device *src_bdev; //source of bio requests
	sector_t start;
	//info related to corruption
	unsigned int num_corrupt;
	sector_t *corrupt_sector;
	u32 *corrupt_block;
	bool inject_enable;
	struct list_head inject_list;
	//fs-specific data
	struct inject_fs_type *fs_t;
	void *context;
};

//points to fs-specific operations in modules
struct inject_fs_type {
	struct list_head list;
	char *name;
	struct module *module;
	int (*ctr)(struct inject_c *ic);
	void (*dtr)(struct inject_c *ic);
	int (*parse_args)(struct inject_c *ic, struct dm_arg_set *as, char *error);
	bool (*block_from_dev)(struct inject_c *ic, struct bio *bio);
	bool (*block_to_dev)(struct inject_c *ic, struct bio *bio);
	int (*data_from_dev)(struct inject_c *ic, struct bio *bio);
	int (*data_to_dev)(struct inject_c *ic, struct bio *bio);
};

static inline struct super_block *get_bdev_sb(struct inject_c *ic)
{
	if(ic && ic->src_bdev)
		return ic->src_bdev->bd_super;
	return NULL;
}

#define IS_EXT4(sb) ((sb) && ((sb)->s_magic == EXT4_SUPER_MAGIC))

int dm_register_inject_fs(struct inject_fs_type *fs);
int dm_unregister_inject_fs(struct inject_fs_type *fs);
struct inject_fs_type *dm_find_inject_fs(const char* name);
#endif
