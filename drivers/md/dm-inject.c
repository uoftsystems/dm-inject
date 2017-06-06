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
#include "../../fs/f2fs/node.h"

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

//from f2fs/node.c get_node_info
//without reading the disk

/*
static int f2fs_get_node_info(struct f2fs_sb_info *sbi, nid_t nid, struct node_info *ni)
{
	struct f2fs_nm_info *nm_i = NM_I(sbi);
	struct curseg_info *curseg = CURSEG_I(sbi, CURSEG_HOT_DATA);
	struct f2fs_journal *journal = curseg->journal;
	struct f2fs_nat_entry ne;
	struct nat_entry *e;
	int i;

	ni->nid = nid;

	//chck nat cache
	down_read(&nm_i->nat_tree_lock);
	//e = __lookup_nat_cache(nm_i, nid);
	if (e) {
		ni->ino = nat_get_ino(e);
		ni->blk_addr = nat_get_blkaddr(e)
	}
	
	//check current segment summary
	down_read(&curseg->journal_rwsem);
	//i = lookup_journal_in_cursum(journal, NAT_JOURNAL, nid, 0);
	if (i >= 0) {
		ne = nat_in_journal(journal, i);
		node_info_from_raw_nat(ni, &ne);
	}
	up_read(&curseg->journal_rwsem);
	if (i >= 0)
		goto found;

found:
	up_read(&nm_i->nat_tree_lock);
}*/

//generic function to read pages
//from f2fs_target_device
struct block_device *f2fs_target_device(struct f2fs_sb_info *sbi,
				block_t blk_addr, struct bio *bio)
{
	struct block_device *bdev = sbi->sb->s_bdev;
	int i;

	for (i = 0; i < sbi->s_ndevs; i++) {
		if (FDEV(i).start_blk <= blk_addr &&
					FDEV(i).end_blk >= blk_addr) {
			blk_addr -= FDEV(i).start_blk;
			bdev = FDEV(i).bdev;
			break;
		}
	}
	if (bio) {
		bio->bi_bdev = bdev;
		bio->bi_iter.bi_sector = SECTOR_FROM_BLOCK(blk_addr);
	}
	return bdev;
}
//from f2fs_submit_page_bio
int f2fs_inject_submit_page_bio(struct f2fs_sb_info *sbi, struct page *page, block_t blk_addr, int op)
{
	struct bio *bio;
	//__bio_alloc / f2fs_bio_alloc
	bio = bio_alloc(GFP_NOIO | __GFP_NOFAIL, 1);
	f2fs_target_device(sbi, blk_addr, bio);
	bio->bi_end_io = NULL;
	bio->bi_private = NULL;
	
	//bio_add_page
	if (bio_add_page(bio, page, PAGE_SIZE, 0) < PAGE_SIZE) {
		bio_put(bio);
		return -EFAULT;
	}
	bio->bi_opf = op;

	//submit_bio_wait
	submit_bio_wait(bio);
	return 0;
}
//issue bios to read pages
static struct page *f2fs_inject_read_page(struct f2fs_sb_info *sbi, block_t blk_addr)
{
	struct page *page;
	page = alloc_page(GFP_NOIO | __GFP_NOFAIL);
	if(!page)
		return NULL;
	if(f2fs_inject_submit_page_bio(sbi, page, blk_addr, REQ_OP_READ)){
		//free page
		__free_page(page);
		return NULL;
	}
	return page;
}



static int check_f2fs_blkaddr(struct page *page, struct f2fs_sb_info *sbi, block_t blk, int op)
{
	if(!sbi)
		return 0;

	struct f2fs_super_block *super = F2FS_RAW_SUPER(sbi);
	u32 segment0_blkaddr = le32_to_cpu(super->segment0_blkaddr);
	u32 cp_blkaddr = le32_to_cpu(super->cp_blkaddr);
	u32 sit_blkaddr = le32_to_cpu(super->sit_blkaddr);
	u32 nat_blkaddr = le32_to_cpu(super->nat_blkaddr);
	u32 ssa_blkaddr = le32_to_cpu(super->ssa_blkaddr);
	u32 main_blkaddr = le32_to_cpu(super->main_blkaddr);
	u32 segment_count_ckpt = le32_to_cpu(super->segment_count_ckpt);
	u32 segment_count_sit = le32_to_cpu(super->segment_count_sit);
	u32 segment_count_nat = le32_to_cpu(super->segment_count_nat);
	u32 segment_count_ssa = le32_to_cpu(super->segment_count_ssa);
	u32 segment_count_main = le32_to_cpu(super->segment_count_main);
	u32 segment_count = le32_to_cpu(super->segment_count);
	u32 log_blocks_per_seg = le32_to_cpu(super->log_blocks_per_seg);
	/*u64 main_end_blkaddr = main_blkaddr +
				(segment_count_main << log_blocks_per_seg);
	u64 seg_end_blkaddr = segment0_blkaddr +
				(segment_count << log_blocks_per_seg);*/

	if(cp_blkaddr <= blk && 
		blk < cp_blkaddr + (segment_count_ckpt << log_blocks_per_seg)) {
		DMDEBUG("%s cp blk %d", __func__, blk);
		return 1;
	} else if(sit_blkaddr <= blk &&
		blk < sit_blkaddr + (segment_count_sit << log_blocks_per_seg)) {
		DMDEBUG("%s sit blk %d", __func__, blk);
		return 1;
	} else if (nat_blkaddr <= blk &&
		blk < nat_blkaddr + (segment_count_nat << log_blocks_per_seg)) {
		DMDEBUG("%s nat blk %d", __func__, blk);
		return 1;
	} else if (ssa_blkaddr <= blk &&
		blk < ssa_blkaddr + (segment_count_ssa << log_blocks_per_seg)) {
		DMDEBUG("%s ssa blk %d", __func__, blk);
		return 1;
	} else if (main_blkaddr <= blk &&
		blk < main_blkaddr + (segment_count_main << log_blocks_per_seg)) {

		//this is Relative (number within main area), not Logical
		//only to be used to index into SSA block
		unsigned int segno = GET_SEGNO(sbi, blk);
		unsigned int segoff = (blk - main_blkaddr) % ENTRIES_IN_SUM;
		struct seg_entry *seg = get_seg_entry(sbi, segno);

		DMDEBUG("%s blk %d seg %u %p", __func__, blk, segno, seg);
		//check segment is NODE or DATA type
		//maybe from summary info GET_SUM_TYPE
		//then use IS_INODE to check if is inode
		if(seg && op==REQ_OP_WRITE) {
			unsigned char type = seg->type;
								/*type = IS_DATASEG(seg->type) ?
								SUM_TYPE_DATA : SUM_TYPE_NODE;
								these types don't work with CURSEG*/
			struct curseg_info *curseg = CURSEG_I(sbi, type);
			struct f2fs_summary_block *sum = curseg->sum_blk;
			struct f2fs_summary *sum_entry = &sum->entries[segoff];
			DMDEBUG("%s sum nid %d type %d", __func__, sum_entry->nid, GET_SUM_TYPE(&sum->footer));
			/*struct page *sum_page = find_get_page(META_MAPPING(sbi),
									GET_SUM_BLOCK(sbi, segno));
			struct f2fs_summary_block *sum;
			struct f2fs_summary *sum_entry;
			f2fs_put_page(sum_page, 0);

			if(sum_page) {
				sum = page_address(sum_page);
				sum_entry = &sum->entries[segoff];
				DMDEBUG("%s sum nid %d type %d", __func__, sum_entry->nid, GET_SUM_TYPE(&sum->footer));
			}*/

			if (IS_NODESEG(type) && IS_INODE(page) && nid_of_node(page) >= F2FS_RESERVED_NODE_NUM) {
				DMDEBUG("%s me thinks it's an inode", __func__);
			}
			
			DMDEBUG("%s main %d blk %d sum %p", __func__, type, blk, sum);
			//might be able to lookup inode via ino_of_node or nid_of_node
		} else if(seg && op==REQ_OP_READ) {
			unsigned char type = seg->type;
			DMDEBUG("%s main %s blk %d", __func__, IS_NODESEG(type)?"NODE":"DATA", blk);
			if (IS_NODESEG(type) && IS_INODE(page) && nid_of_node(page) >= F2FS_RESERVED_NODE_NUM) {
				DMDEBUG("%s me thinks it's an inode nid %d ino %d", __func__, nid_of_node(page), ino_of_node(page));
			}
		} else {
			DMDEBUG("%s main blk %d", __func__, blk);
		}
		return 1;
	} else {
		DMDEBUG("%s unknown blk %d", __func__, blk);
	}
	/*DMDEBUG("%s cp %d sit %d nat %d", __func__, super->cp_blkaddr
		, super->sit_blkaddr, super->nat_blkaddr);*/
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

	DMDEBUG("%s bio op %d sector %d blk %d vcnt %d", __func__, bio_op(bio), bio->bi_iter.bi_sector, SECTOR_TO_BLOCK(bio->bi_iter.bi_sector), bio->bi_vcnt);

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

	//try to get superblock and fs-specific info
	if(ic->src_bdev) {
		sb = ic->src_bdev->bd_super;
	}
	if(sb && sb->s_type && sb->s_type->name) {
		//DMDEBUG("%s fs type %s", __func__, sb->s_type->name);
		if(check_fs_type(sb->s_type) == FS_F2FS) {
			sbi = F2FS_SB(sb);
			raw_super = F2FS_RAW_SUPER(sbi);
			//DMDEBUG("%s f2fs sbi %p raw_sb %p", __func__, sbi, raw_super);
			DMDEBUG("%s bio %p io_vec %p page %p len %d off %d", __func__,
				bio, bio->bi_io_vec, bio->bi_io_vec->bv_page, 
				bio->bi_io_vec->bv_len, bio->bi_io_vec->bv_offset);
			check_f2fs_blkaddr(bio->bi_io_vec->bv_page, sbi, SECTOR_TO_BLOCK(bio->bi_iter.bi_sector), bio_op(bio));
		}
	}


	if(ret==DM_MAPIO_SUBMITTED)
		bio_endio(bio);
	//dump_stack();
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
	/*if(ic->src_bdev) {
		sb = ic->src_bdev->bd_super;
		gd = ic->src_bdev->bd_disk;
	}*/
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
	if(ic->src_bdev) {
		sb = ic->src_bdev->bd_super;
	}

	if(bio_op(bio)==REQ_OP_READ && sb && sb->s_type && sb->s_type->name) {
		//DMDEBUG("%s fs type %s", __func__, sb->s_type->name);
		if(check_fs_type(sb->s_type) == FS_F2FS) {
			sbi = F2FS_SB(sb);
			//DMDEBUG("%s f2fs sbi %p raw_sb %p", __func__, sbi, raw_super);
			DMDEBUG("%s bio op %d sector %d blk %d vcnt %d", __func__, bio_op(bio), bio->bi_iter.bi_sector, SECTOR_TO_BLOCK(bio->bi_iter.bi_sector), bio->bi_vcnt);
			DMDEBUG("%s bio %p io_vec %p page %p len %d off %d", __func__,
				bio, bio->bi_io_vec, bio->bi_io_vec->bv_page, 
				bio->bi_io_vec->bv_len, bio->bi_io_vec->bv_offset);
			check_f2fs_blkaddr(bio->bi_io_vec->bv_page, sbi, SECTOR_TO_BLOCK(bio->bi_iter.bi_sector-8), bio_op(bio));
		}
	}
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
