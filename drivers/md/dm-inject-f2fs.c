/*
 * dm-inject-f2fs device mapper target
 * for device mounted with F2FS
 */

#include "dm-inject.h"
#define DM_MSG_PREFIX "inject f2fs"

//(partially) init f2fs_sb_info
//from fs/f2fs/super.c
void init_sb_info(struct f2fs_sb_info *sbi)
{
	struct f2fs_super_block *raw_super = sbi->raw_super;
	int i, j;

	sbi->log_sectors_per_block =
		le32_to_cpu(raw_super->log_sectors_per_block);
	sbi->log_blocksize = le32_to_cpu(raw_super->log_blocksize);
	sbi->blocksize = 1 << sbi->log_blocksize;
	sbi->log_blocks_per_seg = le32_to_cpu(raw_super->log_blocks_per_seg);
	sbi->blocks_per_seg = 1 << sbi->log_blocks_per_seg;
	sbi->segs_per_sec = le32_to_cpu(raw_super->segs_per_sec);
	sbi->secs_per_zone = le32_to_cpu(raw_super->secs_per_zone);
	sbi->total_sections = le32_to_cpu(raw_super->section_count);
	sbi->total_node_count =
		(le32_to_cpu(raw_super->segment_count_nat) / 2)
			* sbi->blocks_per_seg * NAT_ENTRY_PER_BLOCK;
	sbi->root_ino_num = le32_to_cpu(raw_super->root_ino);
	sbi->node_ino_num = le32_to_cpu(raw_super->node_ino);
	sbi->meta_ino_num = le32_to_cpu(raw_super->meta_ino);
	sbi->cur_victim_sec = NULL_SECNO;
	//sbi->max_victim_search = DEF_MAX_VICTIM_SEARCH;

	sbi->dir_level = DEF_DIR_LEVEL;
	sbi->interval_time[CP_TIME] = DEF_CP_INTERVAL;
	sbi->interval_time[REQ_TIME] = DEF_IDLE_INTERVAL;
	clear_sbi_flag(sbi, SBI_NEED_FSCK);

	for (i = 0; i < NR_COUNT_TYPE; i++)
		atomic_set(&sbi->nr_pages[i], 0);

	INIT_LIST_HEAD(&sbi->s_list);
	mutex_init(&sbi->umount_mutex);
	for (i = 0; i < NR_PAGE_TYPE - 1; i++)
		for (j = HOT; j < NR_TEMP_TYPE; j++)
			mutex_init(&sbi->wio_mutex[i][j]);
	spin_lock_init(&sbi->cp_lock);
}
EXPORT_SYMBOL(init_sb_info);

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
		bio_set_dev(bio, bdev);
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
	bio_set_dev(bio, ic->dev->bdev);
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
	DMDEBUG("%s blk_addr %d page %lx disk %lx sec %d", __func__, blk_addr, page, bio->bi_disk, bio->bi_iter.bi_sector);
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
		if(tmp->type == DM_INJECT_BLOCK && tmp->block_num == blk
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
		if(tmp->type == DM_INJECT_SECTOR && tmp->sector_num == sec
			&& (tmp->op < 0 || tmp->op == op)) {
			DMDEBUG("%s %s %d", __func__, RW(op), sec);
			return true;
		}
	}
	return false;
}

bool f2fs_corrupt_checkpoint(struct inject_c *ic, int op)
{
	struct inject_rec *tmp;
	list_for_each_entry(tmp, &ic->inject_list, list) {
		if(tmp->type == DM_INJECT_F2FS_CP
			&& (tmp->op < 0 || tmp->op == op)) {
			DMDEBUG("%s %s cp", __func__, RW(op));
			return true;
		}
	}
	return false;
}

bool f2fs_corrupt_nat(struct inject_c *ic, int op)
{
	struct inject_rec *tmp;
	list_for_each_entry(tmp, &ic->inject_list, list) {
		if(tmp->type == DM_INJECT_F2FS_NAT
			&& (tmp->op < 0 || tmp->op == op)) {
			DMDEBUG("%s %s nat", __func__, RW(op));
			return true;
		}
	}
	return false;
}

bool f2fs_inject_rec_has_member(struct inject_rec *rec)
{
	return strlen(rec->inode_member) > 0;
}

bool f2fs_corrupt_inode_member(struct inject_c *ic, nid_t ino, int op, struct page *page)
{
	struct inject_rec *tmp;
	list_for_each_entry(tmp, &ic->inject_list, list) {
		if(tmp->type == DM_INJECT_F2FS_INODE && tmp->inode_num == ino
			&& (tmp->op < 0 || tmp->op == op)) {
			//DMDEBUG("%s %s %d", __func__, RW(op), ino);
			if(f2fs_inject_rec_has_member(tmp)) {
				if(strcmp(tmp->inode_member, "mode") == 0) {
					struct f2fs_node *node = F2FS_NODE(page);
					int old_val = node->i.i_mode;
					node->i.i_mode = 0;
					DMDEBUG("%s %s old %#x new %#x", __func__, tmp->inode_member, old_val, node->i.i_mode);
				}
				if(strcmp(tmp->inode_member, "atime") == 0) {
					struct f2fs_node *node = F2FS_NODE(page);
					int old_val = node->i.i_atime;
					node->i.i_atime = 0;
					DMDEBUG("%s %s old %#x new %#x", __func__, tmp->inode_member, old_val, node->i.i_mode);
				}
				if(strcmp(tmp->inode_member, "flags") == 0) {
					struct f2fs_node *node = F2FS_NODE(page);
					int old_val = node->i.i_flags;
					node->i.i_flags = 0;
					DMDEBUG("%s %s old %#x new %#x", __func__, tmp->inode_member, old_val, node->i.i_mode);
				}
				return true;
			}
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
		if(tmp->type == DM_INJECT_F2FS_INODE && tmp->inode_num == ino
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
bool f2fs_corrupt_data(struct inject_c *ic, nid_t ino, int off, int op)
{
	struct inject_rec *tmp;
	list_for_each_entry(tmp, &ic->inject_list, list) {
		if(tmp->type == DM_INJECT_F2FS_DATA && tmp->inode_num == ino
			&& (tmp->op < 0 || tmp->op == op)
			&& (tmp->offset < 0 || tmp->offset == off)) {
			DMDEBUG("%s %s %d %d", __func__, RW(op), ino, off);
			return true;
		}
	}
	return false;
}

int __f2fs_block_id(struct inject_c *ic, struct bio *bio, struct bio_vec *bvec, sector_t sec, int op)
{
	//sbi - even partial is okay since we just need boundaries
	struct f2fs_sb_info *sbi = ic->f2fs_sbi;
	struct f2fs_super_block *super = F2FS_RAW_SUPER(sbi);
	struct page *page = bvec->bv_page;
	block_t	blk = SECTOR_TO_BLOCK(sec);

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
		if(!ic->partial_sbi)
			DMDEBUG("%s CP %s blk %d compact %d", __func__, RW(bio_op(bio)), blk, is_set_ckpt_flags(sbi, CP_COMPACT_SUM_FLAG));
		else
			DMDEBUG("%s CP %s blk %d", __func__, RW(bio_op(bio)), blk);
		return DM_INJECT_F2FS_CP;
	} else if(sit_blkaddr <= blk &&
		blk < sit_blkaddr + (segment_count_sit << log_blocks_per_seg)) {
		DMDEBUG("%s SIT %s blk %d", __func__, RW(bio_op(bio)), blk);
		return DM_INJECT_F2FS_SIT;
	} else if (nat_blkaddr <= blk &&
		blk < nat_blkaddr + (segment_count_nat << log_blocks_per_seg)) {
		DMDEBUG("%s NAT %s blk %d", __func__, RW(bio_op(bio)), blk);
		return DM_INJECT_F2FS_NAT;
	} else if (ssa_blkaddr <= blk &&
		blk < ssa_blkaddr + (segment_count_ssa << log_blocks_per_seg)) {
		DMDEBUG("%s SSA %s blk %d", __func__, RW(bio_op(bio)), blk);
		return DM_INJECT_F2FS_SSA;
	} else if (ic->partial_sbi && main_blkaddr <= blk &&
		blk < main_blkaddr + (segment_count_main << log_blocks_per_seg)) {
		DMDEBUG("%s MAIN %s blk %d", __func__, RW(bio_op(bio)), blk);
		return DM_INJECT_F2FS_MAIN;
	} else if (!ic->partial_sbi && main_blkaddr <= blk &&
		blk < main_blkaddr + (segment_count_main << log_blocks_per_seg)) {
		//this is relative segment number (number within main area)
		//not logical, only to be used to index into SSA block
		unsigned int segno = GET_SEGNO(sbi, blk);
		//unsigned int segoff = (blk - main_blkaddr) % ENTRIES_IN_SUM;
		struct seg_entry *seg = get_seg_entry(sbi, segno);
		unsigned char type = seg->type;
		//DMDEBUG("%s MAIN %s blk %d", __func__, RW(bio_op(bio)), blk);
		if(IS_NODESEG(type)) {
			struct f2fs_node *node = F2FS_NODE(page);
			nid_t num = nid_of_node(page);
			if(IS_INODE(page) && num >= F2FS_RESERVED_NODE_NUM) {
				DMDEBUG("%s INODE %s num %d name '%.*s' blk %d seg %u%s", \
				__func__, RW(bio_op(bio)), num, \
				le32_to_cpu(node->i.i_namelen), \
				node->i.i_name, blk, segno, \
				(node->i.i_inline&F2FS_INLINE_DATA)? " inline data":"");
				return DM_INJECT_F2FS_INODE;
			} else {
				DMDEBUG("%s NODE %s num %d blk %d seg %u", __func__, RW(bio_op(bio)), num, blk, segno);
				return DM_INJECT_F2FS_DNODE | DM_INJECT_F2FS_INDNODE;
			}
		} else {
			//from f2fs-tools get_sum_block
			struct f2fs_checkpoint *cp = F2FS_CKPT(sbi);
			//ssa block # on disk, but we're looking in mem here
			//block_t ssa_blk = GET_SUM_BLOCK(sbi, segno);
			struct curseg_info *curseg;
			int seg_type;
			unsigned short blkoff;
			struct f2fs_summary *sum;
			struct nat_entry *ne;
			struct f2fs_node *inode_node;
			struct page *cache_page = NULL;
			bool found_inode = false;
			//look at open segments and see if we have SSA (in memory)
			for(seg_type = 0; seg_type < NR_CURSEG_DATA_TYPE; seg_type++) {
				//might change depending on number of concurrently open logs,
				//but pretty hard-coded in f2fs
				if(segno == le32_to_cpu(cp->cur_data_segno[seg_type])) {
					struct f2fs_nm_info *nm_i = NM_I(sbi);
					curseg = CURSEG_I(sbi, seg_type);
					//DMDEBUG("%s found ckpt seg %d is open data curseg %p IS_CURSEG %d type %d", __func__, segno, curseg, IS_CURSEG(sbi, segno), GET_SUM_TYPE(&curseg->sum_blk->footer));
					blkoff = GET_BLKOFF_FROM_SEG0(sbi, blk);
					sum = &curseg->sum_blk->entries[blkoff];
					//DMDEBUG("%s sum %p blk %d nid %d ofs %d", __func__, sum, blkoff, sum->nid, sum->ofs_in_node);
					//__lookup_nat_cache
					ne = radix_tree_lookup(&nm_i->nat_root, sum->nid);
					if(ne) {
						cache_page = pagecache_get_page(NODE_MAPPING(sbi), ne->ni.ino, 0, 0);
						if(cache_page) {
							found_inode = true;
							inode_node = F2FS_NODE(cache_page);
							//DMDEBUG("%s ne nid %d ino %d blk %d", __func__, ne->ni.nid, ne->ni.ino, ne->ni.blk_addr);
							//DMDEBUG("%s page %p dir %d name %.*s", __func__, cache_page, S_ISDIR(cpu_to_le16(inode_node->i.i_mode)), le32_to_cpu(inode_node->i.i_namelen), inode_node->i.i_name);
						}
					}
				}
			}
			if(found_inode) {
				DMDEBUG("%s DATA %s blk %d seg %u inode %d name '%.*s' dir %d", \
				__func__, RW(bio_op(bio)), blk, segno, ne->ni.ino, \
				le32_to_cpu(inode_node->i.i_namelen), \
				inode_node->i.i_name, \
				S_ISDIR(cpu_to_le16(inode_node->i.i_mode)));
				if(cache_page)
					put_page(cache_page);
			} else {
				DMDEBUG("%s DATA %s blk %d seg %u", __func__, RW(bio_op(bio)), blk, segno);
			}
			return DM_INJECT_F2FS_DATA;
		}
	}
	return DM_INJECT_NONE;
}

//associated with end_io function in DM injector module
bool __f2fs_corrupt_block_dev(struct inject_c *ic, struct bio *bio, struct bio_vec *bvec, sector_t sec, int op)
{
	struct f2fs_sb_info *sbi = ic->f2fs_sbi;
	struct f2fs_super_block *super = F2FS_RAW_SUPER(sbi);
	struct page *page = bvec->bv_page;
	block_t	blk = SECTOR_TO_BLOCK(sec);
	//sector count was advanced if we're already at end_io
	//(read path)
	/*if(op == REQ_OP_READ) {
		sec -= 8;
		blk -= 1;
	}*/

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
		if(!ic->partial_sbi)
			DMDEBUG("%s CP %s blk %d compact %d", __func__, RW(bio_op(bio)), blk, is_set_ckpt_flags(sbi, CP_COMPACT_SUM_FLAG));
		else
			DMDEBUG("%s CP %s blk %d", __func__, RW(bio_op(bio)), blk);
		return f2fs_corrupt_checkpoint(ic, op);
	} else if(sit_blkaddr <= blk &&
		blk < sit_blkaddr + (segment_count_sit << log_blocks_per_seg)) {
		DMDEBUG("%s SIT blk %d", __func__, blk);
		return false;
	} else if (nat_blkaddr <= blk &&
		blk < nat_blkaddr + (segment_count_nat << log_blocks_per_seg)) {
		DMDEBUG("%s NAT blk %d", __func__, blk);
		return f2fs_corrupt_nat(ic, op);
	} else if (ssa_blkaddr <= blk &&
		blk < ssa_blkaddr + (segment_count_ssa << log_blocks_per_seg)) {
		DMDEBUG("%s SSA blk %d", __func__, blk);
		return false;
	} else if (ic->partial_sbi && main_blkaddr <= blk &&
		blk < main_blkaddr + (segment_count_main << log_blocks_per_seg)) {
		DMDEBUG("%s MAIN blk %d", __func__, blk);
		return false;
	} else if (!ic->partial_sbi && main_blkaddr <= blk &&
		blk < main_blkaddr + (segment_count_main << log_blocks_per_seg)) {
		//this is relative segment number (number within main area)
		//not logical, only to be used to index into SSA block
		unsigned int segno = GET_SEGNO(sbi, blk);
		//unsigned int segoff = (blk - main_blkaddr) % ENTRIES_IN_SUM;
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
				&& f2fs_corrupt_data(ic, num, -1, op)) {
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
					DMDEBUG("%s sum %p blk %d nid %d ofs %d", __func__, sum, blkoff, sum->nid, sum->ofs_in_node);
					//if on write path, should be able to find in cache
					//if(op==REQ_OP_WRITE) {
					{
						struct f2fs_nm_info *nm_i = NM_I(sbi);
						struct nat_entry *ne;
						struct page *cache_page;
						struct f2fs_node *node;
						//__lookup_nat_cache
						ne = radix_tree_lookup(&nm_i->nat_root, sum->nid);
						if(ne) {
							cache_page = pagecache_get_page(NODE_MAPPING(sbi), ne->ni.ino, 0, 0);
							node = F2FS_NODE(cache_page);
							DMDEBUG("%s ne nid %d ino %d blk %d", __func__, ne->ni.nid, ne->ni.ino, ne->ni.blk_addr);
							DMDEBUG("%s page %p dir %d name %.*s", __func__, cache_page, S_ISDIR(cpu_to_le16(node->i.i_mode)), le32_to_cpu(node->i.i_namelen), node->i.i_name);
						}
					}
					if(f2fs_corrupt_data(ic, sum->nid, sum->ofs_in_node, op))
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
	unsigned int iter;
	struct bio_vec *bvec;
	/*if(bio_multiple_segments(bio)) {
		DMDEBUG("%s multiple seg sec %d size %d idx %d done %d", __func__, bio->bi_iter.bi_sector, bio->bi_iter.bi_size, bio->bi_iter.bi_idx, bio->bi_iter.bi_bvec_done);
	}*/
	//DMDEBUG("%s max %d", __func__, max((bio)->bi_iter.bi_size, (bio)->bi_iter.bi_idx*PAGE_SIZE));
	for_each_bvec_no_advance(iter, bvec, bio, 0) {
		//DMDEBUG("%s bvec %p len %d off %d", __func__, bvec->bv_page, bvec->bv_len, bvec->bv_offset);
		if(__f2fs_corrupt_block_dev(ic, bio, bvec, bio->bi_iter.bi_sector + (iter >> SECTOR_SHIFT), REQ_OP_WRITE))
			return true;
	}
	return false;
}
EXPORT_SYMBOL(f2fs_corrupt_block_to_dev);

//associated with end_io function in DM injector module
bool f2fs_corrupt_block_from_dev(struct inject_c *ic, struct bio *bio)
{
	unsigned int iter;
	struct bio_vec *bvec;
	//DMDEBUG("%s max %d", __func__, max((bio)->bi_iter.bi_size, (bio)->bi_iter.bi_idx*PAGE_SIZE));
	for_each_bvec_no_advance(iter, bvec, bio, 0) {
		//DMDEBUG("%s bvec %p len %d off %d", __func__, bvec->bv_page, bvec->bv_len, bvec->bv_offset);
		if(__f2fs_corrupt_block_dev(ic, bio, bvec, bio->bi_iter.bi_sector + (iter >> SECTOR_SHIFT) - 8, REQ_OP_READ))
			return true;
	}
	return false;
}
EXPORT_SYMBOL(f2fs_corrupt_block_from_dev);

int __f2fs_corrupt_data_dev(struct inject_c *ic, struct bio *bio, struct bio_vec *bvec, sector_t sec, int op)
{
	struct page *page = bvec->bv_page;
	block_t	blk = SECTOR_TO_BLOCK(sec);
	int block_type = __f2fs_block_id(ic, bio, bvec, sec, op);
	switch(block_type) {
		nid_t ino;
		case DM_INJECT_F2FS_INODE:
			ino = ino_of_node(page);
			if(f2fs_corrupt_inode_member(ic, ino, op, page))
				return DM_INJECT_F2FS_INODE;
			break;
		default:
			break;
	}
	return DM_INJECT_NONE;
}

int f2fs_corrupt_data_to_dev(struct inject_c *ic, struct bio *bio)
{
	unsigned int iter;
	struct bio_vec *bvec;
	int result = DM_INJECT_NONE;
	for_each_bvec_no_advance(iter, bvec, bio, 0) {
		//DMDEBUG("%s bvec %p len %d off %d", __func__, bvec->bv_page, bvec->bv_len, bvec->bv_offset);
		result = __f2fs_corrupt_data_dev(ic, bio, bvec, bio->bi_iter.bi_sector + iter/512, REQ_OP_WRITE);
		if(result != DM_INJECT_NONE)
			return result;
	}
	return DM_INJECT_NONE;
}
EXPORT_SYMBOL(f2fs_corrupt_data_to_dev);

int f2fs_corrupt_data_from_dev(struct inject_c *ic, struct bio *bio)
{
	unsigned int iter;
	struct bio_vec *bvec;
	int result = DM_INJECT_NONE;
	for_each_bvec_no_advance(iter, bvec, bio, 0) {
		//DMDEBUG("%s bvec %p len %d off %d", __func__, bvec->bv_page, bvec->bv_len, bvec->bv_offset);
		result = __f2fs_corrupt_data_dev(ic, bio, bvec, bio->bi_iter.bi_sector + iter/512 - 8, REQ_OP_READ);
		if(result != DM_INJECT_NONE)
			return result;
	}
	return DM_INJECT_NONE;
}
EXPORT_SYMBOL(f2fs_corrupt_data_from_dev);

static int __init dm_inject_f2fs_init(void)
{ 
	DMINFO("init");
	return 0;
}

static void __exit dm_inject_f2fs_exit(void)
{
	DMINFO("exit");
}

module_init(dm_inject_f2fs_init)
module_exit(dm_inject_f2fs_exit)

MODULE_AUTHOR("Andy Hwang <hwang@cs.toronto.edu>");
MODULE_DESCRIPTION(DM_NAME " f2fs error injection target");
MODULE_LICENSE("GPL");
