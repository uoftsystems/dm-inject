/*
 * dm-inject device mapper target
 * referencing dm-linear, dm-zero etc
 */

#include <linux/device-mapper.h>
#include <linux/module.h>
#include <linux/init.h>
#include <linux/bio.h>
#include <linux/fs.h>
#include <linux/f2fs_fs.h>
#include "../../fs/f2fs/f2fs.h"
#include "../../fs/f2fs/segment.h"

#define DM_MSG_PREFIX "inject"

// Supported filesystems
enum fs {
	FS_UNKNOWN,
	FS_EXT4,
	FS_F2FS
};

// info about the target
struct inject_c {
	struct dm_dev *dev;
	struct block_device *src_bdev;
	sector_t start;
	unsigned int num_corrupt;
	sector_t *corrupt_sector;
	block_t *corrupt_block;
	struct f2fs_super_block *f2fs_super;
};

// Constructor
static int inject_ctr(struct dm_target *ti, unsigned int argc, char **argv)
{
	struct inject_c *ic;
	unsigned long long tmp;
	struct dm_arg_set as;
	const char *devname;
	char dummy;
	int i, ret = 0;

	as.argc = argc;
	as.argv = argv;

	if (argc < 2) {
		ti->error = "Invalid argument count";
		return -EINVAL;
	}

	ic = kmalloc(sizeof(*ic), GFP_KERNEL);
	if (ic == NULL) {
		ti->error = "Cannot allocate inject context";
		return -ENOMEM;
	}

	// regular arguments, similar to linear/flakey target
	ret = -EINVAL;

	// argv[0], device name string
	devname = dm_shift_arg(&as);

	// argv[1], offset sector within device
	if (sscanf(dm_shift_arg(&as), "%llu%c", &tmp, &dummy) != 1) {
		ti->error = "Invalid device sector";
		goto bad;
	}
	ic->start = tmp;

	// read sectors to corrupt
	if (argc > 2) {
		ic->num_corrupt = argc - 2;
		//ic->corrupt_sector = (sector_t*) kmalloc(ic->num_corrupt * sizeof(sector_t), GFP_KERNEL);
		ic->corrupt_sector = NULL;
		ic->corrupt_block = (block_t*) kmalloc(ic->num_corrupt * sizeof(block_t), GFP_KERNEL);
		DMDEBUG("%s num %d size %d", __func__, ic->num_corrupt, sizeof(block_t));
	} else {
		ic->num_corrupt = 0;
		ic->corrupt_sector = NULL;
		ic->corrupt_block = NULL;
	}
	
	for (i=0;i<ic->num_corrupt;i++) {
		if (sscanf(dm_shift_arg(&as), "%llu%c", &tmp, &dummy) != 1) {
			ti->error = "Invalid sector to corrupt";
			goto bad;
		}
		//ic->corrupt_sector[i] = tmp;
		//DMDEBUG("%s corrupt sector %d", __func__, ic->corrupt_sector[i]);
		ic->corrupt_block[i] = tmp;
		DMDEBUG("%s corrupt block %d", __func__, ic->corrupt_block[i]);
	}

	// do this last since it impacts ref count
	ret = dm_get_device(ti, devname, dm_table_get_mode(ti->table), &ic->dev);
	if (ret) {
		ti->error = "Device lookup failed";
		goto bad;
	}

	ic->src_bdev = NULL;
	ti->num_discard_bios = 1;
	ti->private = ic;
	return 0;

	bad:
		if(ic->corrupt_sector)
			kfree(ic->corrupt_sector);
		if(ic->corrupt_block)
			kfree(ic->corrupt_block);
		kfree(ic);
		return ret;
}

// Destructor
static void inject_dtr(struct dm_target *ti)
{
	struct inject_c *ic = (struct inject_c *) ti->private;
	dm_put_device(ti, ic->dev);
	if(ic->corrupt_sector)
		kfree(ic->corrupt_sector);
	if(ic->corrupt_block)
		kfree(ic->corrupt_block);
	kfree(ic);
}

// check if a particular sector is in the list to corrupt
static int check_corrupt_sector(struct inject_c *ic, sector_t s)
{
	int i;
	if (ic->corrupt_sector == NULL){
		DMDEBUG("%s NULL", __func__);
		return 0;
	}
	for (i=0;i<ic->num_corrupt;i++) {
		DMDEBUG("%s %d %d", __func__, i, ic->corrupt_sector[i]);
		if(s == ic->corrupt_sector[i])
			return 1;
	}
	return 0;
}

static int check_corrupt_block(struct inject_c *ic, block_t blk)
{
	int i;
	//DMDEBUG("%s %d", __func__, blk);
	if(ic->corrupt_block == NULL) {
		//DMDEBUG("%s NULL", __func__);
		return 0;
	}
	for (i=0;i<ic->num_corrupt;i++) {
		//DMDEBUG("%s %d %d", __func__, i, ic->corrupt_block[i]);
		if(blk == ic->corrupt_block[i])
			return 1;
	}
	return 0;
}

static int check_f2fs_blkaddr(struct f2fs_super_block *super, block_t blk)
{
	if(!super)
		return 0;
	DMDEBUG("%s blk %d", __func__, blk);
	if(blk == super->cp_blkaddr) {
		DMDEBUG("%s cp blk %d", __func__, blk);
		return 1;
	} else if(blk == super->sit_blkaddr) {
		DMDEBUG("%s sit blk %d", __func__, blk);
		return 1;
	} else if (blk == super->nat_blkaddr) {
		DMDEBUG("%s blk blk %d", __func__, blk);
		return 1;
	}
	DMDEBUG("%s cp %d sit %d nat %d", __func__, super->cp_blkaddr
		, super->sit_blkaddr, super->nat_blkaddr);
	return 0;
}

static enum fs check_fs_type(struct file_system_type *type)
{
	if(type == NULL) {
		DMDEBUG("%s unknown fs", __func__);
		return FS_UNKNOWN;
	}
	else if(strcmp(type->name, "ext4") == 0) {
		//DMDEBUG("%s ext4", __func__);
		return FS_EXT4;
	}
	else if(strcmp(type->name, "f2fs") == 0) {
		//DMDEBUG("%s f2fs", __func__);
		return FS_F2FS;
	}
	else {
		DMDEBUG("%s unknown fs", __func__);
		return FS_UNKNOWN;
	}
}

// Mapper
static int inject_map(struct dm_target *ti, struct bio *bio)
{
	struct inject_c *ic = (struct inject_c *) ti->private;
	struct super_block *sb;
	struct f2fs_sb_info *sbi;
	struct f2fs_super_block *raw_super;
	int ret = DM_MAPIO_SUBMITTED;

	DMDEBUG("%s bio op %d sector %d blk %d", __func__, bio_op(bio), bio->bi_iter.bi_sector, SECTOR_TO_BLOCK(bio->bi_iter.bi_sector));
	switch(bio_op(bio)) {
	case REQ_OP_READ:
		if(bio->bi_opf & REQ_RAHEAD) {
			DMDEBUG("%s fail RAHEAD", __func__);
			return -EIO;
		}
		//linear map
		if(bio->bi_bdev != NULL) {
			ic->src_bdev = bio->bi_bdev;
		}
		bio->bi_bdev = ic->dev->bdev;
		if(bio_sectors(bio)) {
			bio->bi_iter.bi_sector += ic->start;
			bio->bi_iter.bi_sector -= ti->begin;
			DMDEBUG("%s bio mapped R sector %d start %d begin %d", __func__, bio->bi_iter.bi_sector, ic->start, ti->begin);

			//corrupt sectors/blocks
			//if(check_corrupt_sector(ic, bio->bi_iter.bi_sector)) {	
			if(check_corrupt_block(ic, SECTOR_TO_BLOCK(bio->bi_iter.bi_sector))) {	
				DMDEBUG("%s corrupting sector %d", __func__, bio->bi_iter.bi_sector);
				return -EIO;
				//return zero
				//zero_fill_bio(bio);
				//ret = DM_MAPIO_SUBMITTED;
			}
		}
		ret = DM_MAPIO_REMAPPED;
		break;
	case REQ_OP_WRITE:
		//drop writes for now
		bio->bi_bdev = ic->dev->bdev;
		if(bio_sectors(bio)) {
			bio->bi_iter.bi_sector += ic->start;
			bio->bi_iter.bi_sector -= ti->begin;
			DMDEBUG("%s bio mapped W sector %d start %d begin %d", __func__, bio->bi_iter.bi_sector, ic->start, ti->begin);
		}
		ret = DM_MAPIO_REMAPPED;
		break;
	default:
		return -EIO;
	}
	if(ret==DM_MAPIO_SUBMITTED)
		bio_endio(bio);
	//dump_stack();

	//try to get superblock and fs-specific info
	if(ic->src_bdev) {
		sb = ic->src_bdev->bd_super;
	}
	if(sb && sb->s_type && sb->s_type->name) {
		DMDEBUG("%s fs type %s", __func__, sb->s_type->name);
		if(check_fs_type(sb->s_type) == FS_F2FS) {
			sbi = F2FS_SB(sb);
			raw_super = F2FS_RAW_SUPER(sbi);
			DMDEBUG("%s f2fs sbi %p raw_sb %p", __func__, sbi, raw_super);
			check_f2fs_blkaddr(raw_super, SECTOR_TO_BLOCK(bio->bi_iter.bi_sector));
		}
	}
	return ret;
}

static int inject_end_io(struct dm_target *ti, struct bio *bio, int error)
{
	struct inject_c *ic = (struct inject_c *) ti->private;
	struct super_block *sb;
	struct gendisk *gd;
	struct f2fs_sb_info *sbi;
	struct f2fs_super_block *raw_super;

	//try to get superblock
	//sb = bio->bi_bdev->bd_super;
	//gd = bio->bi_bdev->bd_disk;
	if(ic->src_bdev) {
		sb = ic->src_bdev->bd_super;
		gd = ic->src_bdev->bd_disk;
	}
	//DMDEBUG("%s bdev %p gendisk %p sb %p", __func__, bio->bi_bdev, gd, sb);
	/*
	if(gd && gd->disk_name)
		DMDEBUG("%s gendisk %s", __func__, gd->disk_name);
	if(sb)
		DMDEBUG("%s sb %p magic %lx", __func__, sb, sb->s_magic);
	*/
	/*
	if(sb && sb->s_type && sb->s_type->name) {
		DMDEBUG("%s fs type %s", __func__, sb->s_type->name);
		if(check_fs_type(sb->s_type) == FS_F2FS) {
			sbi = F2FS_SB(sb);
			raw_super = F2FS_RAW_SUPER(sbi);
			DMDEBUG("%s f2fs sbi %p raw_sb %p", __func__, sbi, raw_super);
			check_f2fs_blkaddr(raw_super, SECTOR_TO_BLOCK(bio->bi_iter.bi_sector));
		}
	}
	DMDEBUG("%s bio sector %d sb %p", __func__, bio->bi_iter.bi_sector, sb);
	*/
	return 0;
}

static struct target_type inject_target = {
	.name = "inject",
	.version = {0, 1, 0},
	.module = THIS_MODULE,
	.ctr	= inject_ctr,
	.dtr	= inject_dtr,
	.map	= inject_map,
	.end_io = inject_end_io,
};

static int __init dm_inject_init(void)
{
	int r = dm_register_target(&inject_target);
	if (r<0)
		DMERR("dm_inject register failed %d", r);
	DMDEBUG("target registered");
	return r;
}

static void __exit dm_inject_exit(void)
{
	dm_unregister_target(&inject_target);
	DMDEBUG("target unregistered");
}

module_init(dm_inject_init)
module_exit(dm_inject_exit)

MODULE_AUTHOR("Andy Hwang <hwang@cs.toronto.edu>");
MODULE_DESCRIPTION(DM_NAME " target for error injection");
MODULE_LICENSE("GPL");
