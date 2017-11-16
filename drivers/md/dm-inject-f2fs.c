/*
 * dm-inject-f2fs device mapper target
 * for device mounted with F2FS
 */

#include "dm-inject.h"

#include <linux/f2fs_fs.h>
#include "../../fs/f2fs/f2fs.h"
#include "../../fs/f2fs/segment.h"
#include "../../fs/f2fs/node.h"

#define DM_MSG_PREFIX "inject f2fs"

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

#define IS_F2FS(sb) ((sb) && ((sb)->s_magic == F2FS_SUPER_MAGIC))

struct f2fs_context {
	struct f2fs_sb_info *f2fs_sbi;
	//prior to full mount, we can already get partial sbi
	//via reading block 0 directly in inject_ctr
	//use it until the actual sb is setup properly
	struct f2fs_sb_info f2fs_sbi_copy;
	struct f2fs_super_block f2fs_sb_copy;
	bool partial_sbi;
};

//(partially) init f2fs_sb_info
//from fs/f2fs/super.c
void f2fs_init_sb_info(struct inject_c *ic, struct f2fs_sb_info *sbi)
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

void f2fs_get_partial_sb(struct inject_c *ic)
{
	struct f2fs_context *fsc = (struct f2fs_context *) ic->context;

	volatile struct bio *my_bio;
	volatile struct page *my_bio_page;
	int my_ret;

	//try submit_bio
	my_bio_page = alloc_page(GFP_NOIO | __GFP_NOFAIL);
	my_bio = bio_alloc(GFP_NOIO | __GFP_NOFAIL, 1);
	bio_set_dev(my_bio, ic->dev->bdev);
	my_bio->bi_iter.bi_sector = 0;
	my_bio->bi_end_io = NULL;
	my_bio->bi_private = NULL;

	if(bio_add_page(my_bio, my_bio_page, PAGE_SIZE, 0) < PAGE_SIZE) {
		DMDEBUG("%s failed bio_add_page");
	}

	bio_set_op_attrs(my_bio, REQ_OP_READ, REQ_META|REQ_PRIO|REQ_SYNC);

	my_ret = submit_bio_wait(my_bio);
	DMDEBUG("%s my bio %p page %p ret %d", __func__, my_bio, my_bio_page, my_ret);

	//got block 0, let's try and extract sb
	if(my_ret==0) {
		unsigned char *my_page_addr = (unsigned char*)page_address(my_bio->bi_io_vec->bv_page);
		memcpy(&fsc->f2fs_sb_copy, my_page_addr + F2FS_SUPER_OFFSET, sizeof(struct f2fs_super_block));
		fsc->f2fs_sbi_copy.raw_super = &fsc->f2fs_sb_copy;
		f2fs_init_sb_info(ic, &fsc->f2fs_sbi_copy);
		DMDEBUG("%s sb_copy %p sbi_copy %p sbi->raw %p", __func__,
			&fsc->f2fs_sb_copy, &fsc->f2fs_sbi_copy, (&fsc->f2fs_sbi_copy)->raw_super);
		fsc->partial_sbi = true;
		fsc->f2fs_sbi = &fsc->f2fs_sbi_copy;
		DMDEBUG("%s partial_sbi copied %p super %p", __func__, fsc->f2fs_sbi, F2FS_RAW_SUPER(fsc->f2fs_sbi));
	}
}

int f2fs_init(struct inject_c *ic)
{
	struct f2fs_context *fsc;
	fsc = kmalloc(sizeof(*fsc), GFP_KERNEL);
	if(!fsc) {
		DMERR("%s failed", __func__);
		return -ENOMEM;
	}
	fsc->partial_sbi = false;
	fsc->f2fs_sbi = NULL;
	ic->context = fsc;
	f2fs_get_partial_sb(ic);
	return 0;
}

void f2fs_exit(struct inject_c *ic)
{
	struct f2fs_context *fsc = (struct f2fs_context *) ic->context;
	if(fsc)
		kfree(fsc);
}

int f2fs_parse_args(struct inject_c *ic, struct dm_arg_set *as, char *error)
{
	unsigned long long tmp = 0;
	int tmp2 = -1;
	char tmp_str[64];
	const char *devname;
	char dummy;
	int i, ret = 0;

	// read sectors to corrupt
	if (as->argc > 0) {
		ic->num_corrupt = as->argc;
		//ic->corrupt_sector = (sector_t*) kmalloc(ic->num_corrupt * sizeof(sector_t), GFP_KERNEL);
		ic->corrupt_sector = NULL;
		ic->corrupt_block = (block_t*) kmalloc(ic->num_corrupt * sizeof(block_t), GFP_NOIO);
		DMDEBUG("%s num %d size %d", __func__, ic->num_corrupt, sizeof(block_t));
	} else {
		ic->num_corrupt = 0;
		ic->corrupt_sector = NULL;
		ic->corrupt_block = NULL;
	}

	for (i=0;i<ic->num_corrupt;i++) {
		char *cur_arg = dm_shift_arg(as);
		int new_type = DM_INJECT_BLOCK;
		int new_op = -1;
		// R or W denotes only corrupting on one type of access
		if (strchr(cur_arg,'R') == cur_arg) {
			cur_arg++;
			new_op = REQ_OP_READ;
		} else if (strchr(cur_arg,'W') == cur_arg) {
			cur_arg++;
			new_op = REQ_OP_WRITE;
		}
		// meta blocks
		if (strcmp(cur_arg, "cp") == 0) {
			cur_arg += 2;
			new_type = DM_INJECT_F2FS_CP;
		} else if (strcmp(cur_arg, "nat") == 0) {
			cur_arg += 3;
			new_type = DM_INJECT_F2FS_NAT;
		// sector/block/inode/data
		} else {
			if (strchr(cur_arg,'s') == cur_arg) {
				cur_arg++;
				new_type = DM_INJECT_SECTOR;
			} else if (strchr(cur_arg,'b') == cur_arg) {
				cur_arg++;
				new_type = DM_INJECT_BLOCK;
			} else if (strchr(cur_arg,'i') == cur_arg) {
				cur_arg++;
				new_type = DM_INJECT_F2FS_INODE;
			} else if (strchr(cur_arg,'d') == cur_arg) {
				cur_arg++;
				new_type = DM_INJECT_F2FS_DATA;
			}
			// the number
			// offset within data node
			if (new_type == DM_INJECT_F2FS_DATA && sscanf(cur_arg, "%llu[%d]%*c", &tmp, &tmp2) == 2) {
				//DMDEBUG("%s corrupt data %d offset %d", __func__, tmp, tmp2);
			// specific member inside inode
			} else if (new_type == DM_INJECT_F2FS_INODE && sscanf(cur_arg, "%llu[%s]%*c", &tmp, tmp_str) == 2) {
				if(tmp_str[strlen(tmp_str)-1]==']')
					tmp_str[strlen(tmp_str)-1]=0;
				//DMDEBUG("%s corrupt inode %d member %s", __func__, tmp, tmp_str);
			} else if (sscanf(cur_arg, "%llu%*c", &tmp) == 1) {
				//DMDEBUG("%s corrupt %d", __func__, tmp);
			} else {
				error = "Invalid corruption target";
				return -1;
			}
		}
		//ic->corrupt_sector[i] = tmp;
		//DMDEBUG("%s corrupt sector %d", __func__, ic->corrupt_sector[i]);
		//ic->corrupt_block[i] = tmp;
		//add to list
		struct inject_rec *new_block = kzalloc(sizeof(*new_block), GFP_NOIO);
		new_block->type = new_type;
		new_block->op = new_op;
		if (new_block->type == DM_INJECT_SECTOR) {
			new_block->sector_num = tmp;
			DMDEBUG("%s corrupt %s sector %d", __func__, RW(new_block->op), new_block->sector_num);
		} else if (new_block->type == DM_INJECT_BLOCK) {
			new_block->block_num = tmp;
			DMDEBUG("%s corrupt %s block %d", __func__, RW(new_block->op), new_block->block_num);
		} else if (new_block->type == DM_INJECT_F2FS_CP) {
			new_block->block_num = 0;
			DMDEBUG("%s corrupt %s checkpoint", __func__, RW(new_block->op));
		} else if (new_block->type == DM_INJECT_F2FS_NAT) {
			new_block->block_num = 0;
			DMDEBUG("%s corrupt %s NAT", __func__, RW(new_block->op));
		} else if (new_block->type == DM_INJECT_F2FS_INODE) {
			new_block->inode_num = tmp;
			if(strlen(tmp_str))
				strcpy(new_block->inode_member, tmp_str);
			else
				new_block->inode_member[0]=0;
			tmp_str[0]=0;
			DMDEBUG("%s corrupt %s inode %d %s", __func__, RW(new_block->op), new_block->inode_num, new_block->inode_member);
		} else if (new_block->type == DM_INJECT_F2FS_DATA) {
			new_block->inode_num = tmp;
			new_block->offset = tmp2/PAGE_SIZE;
			tmp2 = -1;
			DMDEBUG("%s corrupt %s data of inode %d off %d", __func__, RW(new_block->op), new_block->inode_num, new_block->offset);
		}
		list_add_tail(&new_block->list, &ic->inject_list);
	}

	return 0;
}

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
			DMDEBUG("%s CORRUPT %s block %d", __func__, RW(op), blk);
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
			DMDEBUG("%s CORRUPT %s sector %d", __func__, RW(op), sec);
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
			DMDEBUG("%s CORRUPT %s cp", __func__, RW(op));
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
			DMDEBUG("%s CORRUPT %s nat", __func__, RW(op));
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
					DMDEBUG("%s CORRUPT inode %s old %#x new %#x", __func__, tmp->inode_member, old_val, node->i.i_mode);
				}
				if(strcmp(tmp->inode_member, "atime") == 0) {
					struct f2fs_node *node = F2FS_NODE(page);
					int old_val = node->i.i_atime;
					node->i.i_atime = 0;
					DMDEBUG("%s CORRUPT inode %s old %#x new %#x", __func__, tmp->inode_member, old_val, node->i.i_mode);
				}
				if(strcmp(tmp->inode_member, "flags") == 0) {
					struct f2fs_node *node = F2FS_NODE(page);
					int old_val = node->i.i_flags;
					node->i.i_flags = 0;
					DMDEBUG("%s CORRUPT inode %s old %#x new %#x", __func__, tmp->inode_member, old_val, node->i.i_mode);
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
			DMDEBUG("%s CORRUPT %s inode %d", __func__, RW(op), ino);
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
			DMDEBUG("%s CORRUPT %s inode %d data off %d", __func__, RW(op), ino, off);
			return true;
		}
	}
	return false;
}

int __f2fs_block_id(struct inject_c *ic, struct bio *bio, struct bio_vec *bvec, sector_t sec, int op)
{
	struct f2fs_context *fsc = (struct f2fs_context *) ic->context;

	//sbi - even partial is okay since we just need boundaries
	struct f2fs_sb_info *sbi = fsc->f2fs_sbi;
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
		if(!fsc->partial_sbi)
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
	} else if (fsc->partial_sbi && main_blkaddr <= blk &&
		blk < main_blkaddr + (segment_count_main << log_blocks_per_seg)) {
		DMDEBUG("%s MAIN %s blk %d", __func__, RW(bio_op(bio)), blk);
		return DM_INJECT_F2FS_MAIN;
	} else if (!fsc->partial_sbi && main_blkaddr <= blk &&
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
	struct f2fs_context *fsc = (struct f2fs_context *) ic->context;

	struct f2fs_sb_info *sbi = fsc->f2fs_sbi;
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
		if(!fsc->partial_sbi)
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
	} else if (fsc->partial_sbi && main_blkaddr <= blk &&
		blk < main_blkaddr + (segment_count_main << log_blocks_per_seg)) {
		DMDEBUG("%s MAIN blk %d", __func__, blk);
		return false;
	} else if (!fsc->partial_sbi && main_blkaddr <= blk &&
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

int f2fs_get_full_sb(struct inject_c *ic)
{
	struct f2fs_context *fsc = (struct f2fs_context *) ic->context;

	struct super_block *sb = NULL;
	struct f2fs_sb_info *sbi = NULL;

	if(!fsc->partial_sbi)
		return 0;

	sb = get_bdev_sb(ic);

	if(sb && IS_F2FS(sb)) {
		sbi = F2FS_SB(sb);
		struct f2fs_super_block *from_bdev = F2FS_RAW_SUPER(F2FS_SB(sb));
		fsc->f2fs_sbi = sbi;
		fsc->partial_sbi = false;
		DMDEBUG("%s full_sbi %p sb %lx fsc->sb %lx memcmp %d", __func__,
		fsc->f2fs_sbi, *from_bdev, fsc->f2fs_sb_copy, memcmp(from_bdev, &fsc->f2fs_sb_copy, sizeof(struct f2fs_super_block)));
		return 1;
	}
	return 0;
}

bool f2fs_can_corrupt(struct inject_c *ic)
{
	struct f2fs_context *fsc = (struct f2fs_context *) ic->context;
	struct super_block *sb = NULL;
	bool ret = false;

	//already have full or partial sb
	if(fsc->f2fs_sbi != &fsc->f2fs_sbi_copy)
		return true;
	else if(fsc->partial_sbi)
		ret = true; //move on to try to get full sb

	//none or partial, but fs is mounted
	//so we can try to get full sb
	sb = get_bdev_sb(ic);
	if(sb && f2fs_get_full_sb(ic))
		ret = true;

	return ret;
}

//associated with map function in DM injector module
bool f2fs_corrupt_block_to_dev(struct inject_c *ic, struct bio *bio)
{
	unsigned int iter;
	struct bio_vec *bvec;

	if(!f2fs_can_corrupt(ic))
		return false;

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

//associated with end_io function in DM injector module
bool f2fs_corrupt_block_from_dev(struct inject_c *ic, struct bio *bio)
{
	unsigned int iter;
	struct bio_vec *bvec;

	if(!f2fs_can_corrupt(ic))
		return false;

	//DMDEBUG("%s max %d", __func__, max((bio)->bi_iter.bi_size, (bio)->bi_iter.bi_idx*PAGE_SIZE));
	for_each_bvec_no_advance(iter, bvec, bio, 0) {
		//DMDEBUG("%s bvec %p len %d off %d", __func__, bvec->bv_page, bvec->bv_len, bvec->bv_offset);
		if(__f2fs_corrupt_block_dev(ic, bio, bvec, bio->bi_iter.bi_sector + (iter >> SECTOR_SHIFT) - 8, REQ_OP_READ))
			return true;
	}
	return false;
}

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

	if(!f2fs_can_corrupt(ic))
		return DM_INJECT_NONE;

	for_each_bvec_no_advance(iter, bvec, bio, 0) {
		//DMDEBUG("%s bvec %p len %d off %d", __func__, bvec->bv_page, bvec->bv_len, bvec->bv_offset);
		result = __f2fs_corrupt_data_dev(ic, bio, bvec, bio->bi_iter.bi_sector + iter/512, REQ_OP_WRITE);
		if(result != DM_INJECT_NONE)
			return result;
	}
	return DM_INJECT_NONE;
}

int f2fs_corrupt_data_from_dev(struct inject_c *ic, struct bio *bio)
{
	unsigned int iter;
	struct bio_vec *bvec;
	int result = DM_INJECT_NONE;

	if(!f2fs_can_corrupt(ic))
		return DM_INJECT_NONE;

	for_each_bvec_no_advance(iter, bvec, bio, 0) {
		//DMDEBUG("%s bvec %p len %d off %d", __func__, bvec->bv_page, bvec->bv_len, bvec->bv_offset);
		result = __f2fs_corrupt_data_dev(ic, bio, bvec, bio->bi_iter.bi_sector + iter/512 - 8, REQ_OP_READ);
		if(result != DM_INJECT_NONE)
			return result;
	}
	return DM_INJECT_NONE;
}

static struct inject_fs_type f2fs_fs = {
	.name = "f2fs",
	.module = THIS_MODULE,
	.init = f2fs_init,
	.exit = f2fs_exit,
	.parse_args = f2fs_parse_args,
	.block_from_dev = f2fs_corrupt_block_from_dev,
	.block_to_dev = f2fs_corrupt_block_to_dev,
	.data_from_dev = f2fs_corrupt_data_from_dev,
	.data_to_dev = f2fs_corrupt_data_to_dev,
};

static int __init dm_inject_f2fs_init(void)
{
	dm_register_inject_fs(&f2fs_fs);
	DMINFO("init");
	return 0;
}

static void __exit dm_inject_f2fs_exit(void)
{
	dm_unregister_inject_fs(&f2fs_fs);
	DMINFO("exit");
}

module_init(dm_inject_f2fs_init)
module_exit(dm_inject_f2fs_exit)

MODULE_AUTHOR("Andy Hwang <hwang@cs.toronto.edu>");
MODULE_DESCRIPTION(DM_NAME " f2fs error injection target");
MODULE_LICENSE("GPL");
