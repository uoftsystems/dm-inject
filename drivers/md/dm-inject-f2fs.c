/*
 * dm-inject-f2fs device mapper target
 * for device mounted with F2FS
 */

#include "dm-inject.h"

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
static int f2fs_inject_submit_page_bio(struct f2fs_sb_info *sbi, struct page *page, block_t blk_addr, int op)
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

bool f2fs_corrupt_block(struct inject_c *ic, block_t blk)
{
	struct inject_rec *tmp;
	list_for_each_entry(tmp, &ic->inject_list, list) {
		if(tmp->type == INJECT_BLOCK && tmp->block_num == blk) {
			DMDEBUG("%s %d", __func__, blk);
			return true;
		}
	}
	return false;
}
// check inode against input to see if corruption should take place
// doesn't check for bio direction or anything
bool f2fs_corrupt_inode(struct inject_c *ic, nid_t ino)
{
	return false;
}

//associated with map function in DM injector module
bool f2fs_corrupt_block_to_dev(struct inject_c *ic, struct bio *bio)
{
	return false;
}

//associated with end_io function in DM injector module
bool f2fs_corrupt_block_from_dev(struct inject_c *ic, struct bio *bio)
{
	struct f2fs_sb_info *sbi = ic->f2fs_sbi;
	struct f2fs_super_block *super = F2FS_RAW_SUPER(sbi);
	struct page *page = bio->bi_io_vec->bv_page;
	//sector count was advanced since we're already at end_io
	block_t blk = SECTOR_TO_BLOCK((bio->bi_iter.bi_sector-8));

	if(f2fs_corrupt_block(ic, blk))
		return true;

	//from super.c: sanity_check_area_boundary
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
	u32 log_blocks_per_seg = le32_to_cpu(super->log_blocks_per_seg);

	if(cp_blkaddr <= blk && 
		blk < cp_blkaddr + (segment_count_ckpt << log_blocks_per_seg)) {
		DMDEBUG("%s cp blk %d", __func__, blk);
		return false;
	} else if(sit_blkaddr <= blk &&
		blk < sit_blkaddr + (segment_count_sit << log_blocks_per_seg)) {
		DMDEBUG("%s sit blk %d", __func__, blk);
		return false;
	} else if (nat_blkaddr <= blk &&
		blk < nat_blkaddr + (segment_count_nat << log_blocks_per_seg)) {
		DMDEBUG("%s nat blk %d", __func__, blk);
		return false;
	} else if (ssa_blkaddr <= blk &&
		blk < ssa_blkaddr + (segment_count_ssa << log_blocks_per_seg)) {
		DMDEBUG("%s ssa blk %d", __func__, blk);
		return false;
	} else if (main_blkaddr <= blk &&
		blk < main_blkaddr + (segment_count_main << log_blocks_per_seg)) {
		//this is relative segment number (number within main area)
		//not logical, only to be used to index into SSA block
		unsigned int segno = GET_SEGNO(sbi, blk);
		unsigned int segoff = (blk - main_blkaddr) % ENTRIES_IN_SUM;
		struct seg_entry *seg = get_seg_entry(sbi, segno);
		unsigned char type = seg->type;
		if(IS_NODESEG(type) && IS_INODE(page)
			&& nid_of_node(page) >= F2FS_RESERVED_NODE_NUM) {
			DMDEBUG("%s INODE %s blk %d seg %u", __func__, RW(bio_op(bio)), blk, segno);
		} else if (IS_NODESEG(type)) {
			DMDEBUG("%s NODE %s blk %d seg %u", __func__, RW(bio_op(bio)), blk, segno);
		} else {
			DMDEBUG("%s DATA %s blk %d seg %u", __func__, RW(bio_op(bio)), blk, segno);
		}
		return false;
	}
	return false;
}
