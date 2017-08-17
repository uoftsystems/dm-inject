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
static int f2fs_inject_submit_page_bio(struct inject_c *ic, struct page *page, block_t blk_addr, int op)
{
	struct bio *bio;
	//__bio_alloc / f2fs_bio_alloc
	bio = bio_alloc(GFP_NOIO | __GFP_NOFAIL, 1);
	//TODO: this is not the right bdev
	//f2fs is mounted on TOP of the mapper,
	//we need to go deeper to lower level
	//ic->dev->bdev
	//f2fs_target_device(sbi, blk_addr, bio);
	bio->bi_bdev = ic->dev->bdev;
	bio->bi_iter.bi_sector = SECTOR_FROM_BLOCK(blk_addr);
	bio->bi_end_io = NULL;
	bio->bi_private = NULL;
	
	//bio_add_page
	if (bio_add_page(bio, page, PAGE_SIZE, 0) < PAGE_SIZE) {
		bio_put(bio);
		return -EFAULT;
	}
	//bio->bi_opf = op;
	bio_set_op_attrs(bio, op, REQ_META|REQ_PRIO|REQ_SYNC);

	//submit_bio_wait
	DMDEBUG("%s blk_addr %d page %lx bdev %lx sec %d", __func__, blk_addr, page, bio->bi_bdev, bio->bi_iter.bi_sector);
	//submit_bio_wait(bio);
	submit_bio(bio);
	bio_put(bio);
	return 0;
}
//issue bios to read pages
static struct page *f2fs_inject_read_page(struct inject_c *ic, block_t blk_addr)
{
	struct page *page;
	page = alloc_page(GFP_NOIO | __GFP_NOFAIL);
	DMDEBUG("%s alloc_page %lx", __func__, page);
	if(!page)
		return NULL;
	if(f2fs_inject_submit_page_bio(ic, page, blk_addr, REQ_OP_READ)){
		//free page
		__free_page(page);
		return NULL;
	}
	return page;
}

bool f2fs_corrupt_block(struct inject_c *ic, block_t blk, int op)
{
	struct inject_rec *tmp;
	list_for_each_entry(tmp, &ic->inject_list, list) {
		if(tmp->type == INJECT_BLOCK && tmp->block_num == blk
			&& (tmp->op < 0 || tmp->op == op)) {
			DMDEBUG("%s %s %d", __func__, RW(op), blk);
			return true;
		}
	}
	return false;
}

bool f2fs_corrupt_sector(struct inject_c *ic, sector_t sec, int op)
{
	struct inject_rec *tmp;
	list_for_each_entry(tmp, &ic->inject_list, list) {
		if(tmp->type == INJECT_SECTOR && tmp->sector_num == sec
			&& (tmp->op < 0 || tmp->op == op)) {
			DMDEBUG("%s %s %d", __func__, RW(op), sec);
			return true;
		}
	}
	return false;
}

// check inode against input to see if corruption should take place
// doesn't check for bio direction or anything
bool f2fs_corrupt_inode(struct inject_c *ic, nid_t ino, int op)
{
	struct inject_rec *tmp;
	list_for_each_entry(tmp, &ic->inject_list, list) {
		if(tmp->type == INJECT_INODE && tmp->inode_num == ino
			&& (tmp->op < 0 || tmp->op == op)) {
			DMDEBUG("%s %s %d", __func__, RW(op), ino);
			return true;
		}
	}
	return false;
}

void f2fs_print_seg_entries(struct f2fs_sb_info *sbi)
{
	struct sit_info *sit_i = SIT_I(sbi);
	int i;
	for (i=0;i<MAIN_SEGS(sbi);i++) {
		struct seg_entry *s = &sit_i->sentries[i];
		DMDEBUG("%s seg %d type %d vblks %d", __func__, i, s->type, s->valid_blocks);
	}
}

// check inode against input to see if data corruption should take place
// doesn't check for bio direction or anything
bool f2fs_corrupt_data(struct inject_c *ic, nid_t ino, int op)
{
	struct inject_rec *tmp;
	list_for_each_entry(tmp, &ic->inject_list, list) {
		if(tmp->type == INJECT_DATA && tmp->inode_num == ino
			&& (tmp->op < 0 || tmp->op == op)) {
			DMDEBUG("%s %s %d", __func__, RW(op), ino);
			return true;
		}
	}
	return false;
}

//associated with end_io function in DM injector module
bool __f2fs_corrupt_block_dev(struct inject_c *ic, struct bio *bio, int op)
{
	struct f2fs_sb_info *sbi = ic->f2fs_sbi;
	struct f2fs_super_block *super = F2FS_RAW_SUPER(sbi);
	struct page *page = bio->bi_io_vec->bv_page;
	sector_t sec = bio->bi_iter.bi_sector;
	block_t	blk = SECTOR_TO_BLOCK((bio->bi_iter.bi_sector));
	//sector count was advanced if we're already at end_io
	//(read path)
	if(op == REQ_OP_READ) {
		sec -= 8;
		blk -= 1;
	}

	if(f2fs_corrupt_sector(ic, sec, op))
		return true;
	else if(f2fs_corrupt_block(ic, blk, op))
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
		DMDEBUG("%s CP blk %d compact %d", __func__, blk, is_set_ckpt_flags(sbi, CP_COMPACT_SUM_FLAG));
		return false;
	} else if(sit_blkaddr <= blk &&
		blk < sit_blkaddr + (segment_count_sit << log_blocks_per_seg)) {
		//DMDEBUG("%s SIT blk %d", __func__, blk);
		return false;
	} else if (nat_blkaddr <= blk &&
		blk < nat_blkaddr + (segment_count_nat << log_blocks_per_seg)) {
		//DMDEBUG("%s NAT blk %d", __func__, blk);
		return false;
	} else if (ssa_blkaddr <= blk &&
		blk < ssa_blkaddr + (segment_count_ssa << log_blocks_per_seg)) {
		//DMDEBUG("%s SSA blk %d", __func__, blk);
		return false;
	} else if (main_blkaddr <= blk &&
		blk < main_blkaddr + (segment_count_main << log_blocks_per_seg)) {
		//this is relative segment number (number within main area)
		//not logical, only to be used to index into SSA block
		unsigned int segno = GET_SEGNO(sbi, blk);
		unsigned int segoff = (blk - main_blkaddr) % ENTRIES_IN_SUM;
		struct seg_entry *seg = get_seg_entry(sbi, segno);
		unsigned char type = seg->type;
		nid_t num = nid_of_node(page);
		DMDEBUG("%s seg %d type %d vblks %d", __func__, segno, seg->type, seg->valid_blocks);
		if(is_set_ckpt_flags(sbi, CP_COMPACT_SUM_FLAG))
			DMDEBUG("%s ckpt is compact", __func__);
		if(IS_NODESEG(type) && IS_INODE(page)
			&& num >= F2FS_RESERVED_NODE_NUM) {
			struct f2fs_node *node = F2FS_NODE(page);
			DMDEBUG("%s INODE %s num %d blk %d seg %u%s", __func__, RW(bio_op(bio)), num, blk, segno, (node->i.i_inline&F2FS_INLINE_DATA)? " inline data":"");
			if(f2fs_corrupt_inode(ic, num, op))
				return true;
			if(node->i.i_inline & F2FS_INLINE_DATA //what about INLINE_DENTRY flag?
				&& f2fs_corrupt_data(ic, num, op)) {
				DMDEBUG("%s corrupting INODE with INLINE DATA", __func__);
				return true;
			}
		} else if (IS_NODESEG(type)) {
			DMDEBUG("%s NODE %s num %d  blk %d seg %u", __func__, RW(bio_op(bio)), num, blk, segno);
		} else {
			DMDEBUG("%s DATA %s num %d blk %d seg %u", __func__, RW(bio_op(bio)), num, blk, segno);
			//from f2fs-tools get_sum_block
			struct f2fs_checkpoint *cp = F2FS_CKPT(sbi);
			block_t ssa_blk = GET_SUM_BLOCK(sbi, segno);
			struct curseg_info *curseg;
			int seg_type;
			unsigned short blkoff;
			struct f2fs_summary *sum;
			DMDEBUG("%s looking for SSA block %d", __func__, ssa_blk);
			//look at open segments and see if we have SSA (in memory)
			for(seg_type = 0; seg_type < NR_CURSEG_DATA_TYPE; seg_type++) {
				//might change depending on number of concurrently open logs,
				//but pretty hard-coded in f2fs
				if(segno == le32_to_cpu(cp->cur_data_segno[seg_type])) {
					curseg = CURSEG_I(sbi, seg_type);
					DMDEBUG("%s found ckpt seg %d is open data curseg %p IS_CURSEG %d type %d", __func__, segno, curseg, IS_CURSEG(sbi, segno), GET_SUM_TYPE(&curseg->sum_blk->footer));
					blkoff = GET_BLKOFF_FROM_SEG0(sbi, blk);
					sum = &curseg->sum_blk->entries[blkoff];
					DMDEBUG("%s sum %p nid %d ofs %d", __func__, sum, sum->nid, sum->ofs_in_node);
					if(f2fs_corrupt_data(ic, sum->nid, op))
						return true;
					break;
				}
			}
			//from do_garbage_collect
			//struct page *sum_page;
			//struct f2fs_summary_block *sum;
			//see if it's already in cache
			////sum_page = find_get_page(META_MAPPING(sbi), GET_SUM_BLOCK(sbi, segno));
			//sum_page = f2fs_inject_read_page(ic, GET_SUM_BLOCK(sbi,segno));
			////sum_page = get_sum_page(sbi, segno);
			/*DMDEBUG("%s sum page %lx", __func__, sum_page);
			if(sum_page){
				sum = page_address(sum_page);
				DMDEBUG("%s found cached sum block %lx page %lx", __func__, sum, sum_page);
				f2fs_put_page(sum_page,0);
			}*/
		}
		return false;
	}
	return false;
}

//associated with map function in DM injector module
bool f2fs_corrupt_block_to_dev(struct inject_c *ic, struct bio *bio)
{
	return __f2fs_corrupt_block_dev(ic, bio, REQ_OP_WRITE);
}

//associated with end_io function in DM injector module
bool f2fs_corrupt_block_from_dev(struct inject_c *ic, struct bio *bio)
{
	return __f2fs_corrupt_block_dev(ic, bio, REQ_OP_READ);
}
