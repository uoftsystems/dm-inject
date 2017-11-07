/*
 * dm-inject device mapper target
 * referencing dm-linear, dm-zero etc
 */

#include "dm-inject.h"

// Constructor
static int inject_ctr(struct dm_target *ti, unsigned int argc, char **argv)
{
	struct inject_c *ic;
	unsigned long long tmp = 0;
	int tmp2 = -1;
	char tmp_str[64];
	struct dm_arg_set as;
	const char *devname;
	char dummy;
	int i, ret = 0;

	//try submit_bio
	volatile struct bio *my_bio;
	volatile struct page *my_bio_page;
	int my_ret;

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

	INIT_LIST_HEAD(&ic->inject_list);

	// read sectors to corrupt
	if (argc > 2) {
		ic->num_corrupt = argc - 2;
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
		char *cur_arg = dm_shift_arg(&as);
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
				ti->error = "Invalid corruption target";
				goto bad;
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

	// do this last since it impacts ref count
	ret = dm_get_device(ti, devname, dm_table_get_mode(ti->table), &ic->dev);
	if (ret) {
		ti->error = "Device lookup failed";
		goto bad;
	}

	ic->src_bdev = NULL;
	ic->inject_enable = false; //injection disabled by default
	ti->num_discard_bios = 1;
	ti->private = ic;
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
		memcpy(&ic->f2fs_sb_copy, my_page_addr + F2FS_SUPER_OFFSET, sizeof(struct f2fs_super_block));
		ic->f2fs_sbi_copy.raw_super = &ic->f2fs_sb_copy;
		init_sb_info(&ic->f2fs_sbi_copy);
		DMDEBUG("%s sb_copy %p sbi_copy %p sbi->raw %p", __func__,
			&ic->f2fs_sb_copy, &ic->f2fs_sbi_copy, (&ic->f2fs_sbi_copy)->raw_super);
		ic->partial_sbi = true;
		ic->f2fs_sbi = &ic->f2fs_sbi_copy;
		DMDEBUG("%s partial_sbi copied %p super %p", __func__, ic->f2fs_sbi, F2FS_RAW_SUPER(ic->f2fs_sbi));
	}
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
	struct inject_rec *tmp, *tmp2;
	dm_put_device(ti, ic->dev);
	if(ic->corrupt_sector)
		kfree(ic->corrupt_sector);
	if(ic->corrupt_block)
		kfree(ic->corrupt_block);
	list_for_each_entry_safe(tmp, tmp2, &ic->inject_list, list) {
		list_del(&tmp->list);
		kfree(tmp);
	}
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

// Mapper
static int inject_map(struct dm_target *ti, struct bio *bio)
{
	struct inject_c *ic = (struct inject_c *) ti->private;
	struct super_block *sb;
	struct f2fs_sb_info *sbi;
	struct f2fs_super_block *raw_super;
	int ret = DM_MAPIO_SUBMITTED;

	//DMDEBUG("%s bio op %d sector %d blk %d vcnt %d", __func__, bio_op(bio), bio->bi_iter.bi_sector, SECTOR_TO_BLOCK(bio->bi_iter.bi_sector), bio->bi_vcnt);

	//drop read-ahead?
	if(bio->bi_opf & REQ_RAHEAD) {
		DMDEBUG("%s fail RAHEAD", __func__);
		return DM_MAPIO_KILL;
	}
	//assign src_bdev to grab superblock if fs is mounted
	//DMDEBUG("%s bio->bi_disk %p bd_super %p", __func__, bio->bi_disk, (bdget_disk(bio->bi_disk, 0))->bd_super);
	if(bio->bi_disk != NULL) {
		ic->src_bdev = bdget_disk(bio->bi_disk, 0);
	}

	//linear mapping
	bio_set_dev(bio, ic->dev->bdev);
	if(bio_sectors(bio)) {
		bio->bi_iter.bi_sector += ic->start;
		bio->bi_iter.bi_sector -= ti->begin;
	}
	//make the request SYNC to prevent merging?
	//bio->bi_opf |= REQ_SYNC;
	ret = DM_MAPIO_REMAPPED;

	//get sb and TODO:check if we need to corrupt
	if(!sb)
		sb = get_bdev_sb(ic);

	if(sb && IS_F2FS(sb)) {
		sbi = F2FS_SB(sb);
		struct f2fs_super_block *from_bdev = F2FS_RAW_SUPER(F2FS_SB(sb));
		ic->f2fs_sbi = sbi;
		ic->partial_sbi = false;
		/*DMDEBUG("%s full_sbi %p sb %lx ic->sb %lx memcmp %d", __func__,
		ic->f2fs_sbi, *from_bdev, ic->f2fs_sb_copy, memcmp(from_bdev, &ic->f2fs_sb_copy, sizeof(struct f2fs_super_block)));*/
	}
	//DMDEBUG("%s sb %p", __func__, sb);
	//intercept and inject F2FS write requests
	//data travelling from from memory to block device
	if(ic->inject_enable)
		if(bio_op(bio)==REQ_OP_WRITE && (IS_F2FS(sb)||ic->partial_sbi)) {
			//DMDEBUG("%s bio %s sector %d blk %d vcnt %d", __func__, RW(bio_op(bio)), bio->bi_iter.bi_sector, SECTOR_TO_BLOCK(bio->bi_iter.bi_sector), bio->bi_vcnt);
			//DMDEBUG("%s sector %d bi_size %d bi_bvec_done %d bi_idx %d", __func__, bio->bi_iter.bi_sector, bio->bi_iter.bi_size, bio->bi_iter.bi_bvec_done, bio->bi_iter.bi_idx);
			if(f2fs_corrupt_block_to_dev(ic, bio))
				return DM_MAPIO_KILL;
		}

	if(ret==DM_MAPIO_SUBMITTED)
		bio_endio(bio);
	//dump_stack();
	return ret;
}

static int inject_end_io(struct dm_target *ti, struct bio *bio, blk_status_t *error)
{
	struct inject_c *ic = (struct inject_c *) ti->private;
	struct super_block *sb;
	struct f2fs_sb_info *sbi;

	//DMDEBUG("%s bio op %d sector %d blk %d vcnt %d", __func__, bio_op(bio), bio->bi_iter.bi_sector, SECTOR_TO_BLOCK(bio->bi_iter.bi_sector), bio->bi_vcnt);
	//the sector count was advanced during the bio
	sector_t sec = bio->bi_iter.bi_sector-8;

	sb = get_bdev_sb(ic);
	//if we happen to read block 0, cat get sb
	if(!sb && sec==0) {
		//memcpy(&ic->sb, bio->bi_io_vec->bv_page + F2FS_SUPER_OFFSET, sizeof(
	}
	if(ic->partial_sbi && IS_F2FS(sb)) {
		sbi = F2FS_SB(sb);
		ic->f2fs_sbi = sbi;
		ic->partial_sbi = false;
		//DMDEBUG("%s full_sbi %p", __func__, ic->f2fs_sbi);
	}
	//intercept and inject F2FS read requests
	//data from block device travelling to memory
	if(ic->inject_enable)
		if(bio_op(bio)==REQ_OP_READ && (IS_F2FS(sb)||ic->partial_sbi)) {
			//DMDEBUG("%s bio %s sector %d blk %d vcnt %d", __func__, RW(bio_op(bio)), sec, SECTOR_TO_BLOCK(sec), bio->bi_vcnt);
			//DMDEBUG("%s sector %d bi_size %d bi_bvec_done %d bi_idx %d", __func__, bio->bi_iter.bi_sector, bio->bi_iter.bi_size, bio->bi_iter.bi_bvec_done, bio->bi_iter.bi_idx);
			/*DMDEBUG("%s bio %p io_vec %p page %p len %d off %d", __func__,
				bio, bio->bi_io_vec, bio->bi_io_vec->bv_page, 
				bio->bi_io_vec->bv_len, bio->bi_io_vec->bv_offset);*/
			if(f2fs_corrupt_data_from_dev(ic, bio)!=DM_INJECT_NONE) {
				//we corrupted some data, can do accounting here
				//but still pretend to be normal
				return DM_ENDIO_DONE;
			} else if(f2fs_corrupt_block_from_dev(ic, bio)) {
				*error = BLK_STS_IOERR;
				return DM_ENDIO_DONE;
			}
		}
	return DM_ENDIO_DONE;
}

static int inject_message(struct dm_target *ti, unsigned argc, char **argv)
{
	int r = -EINVAL;
	struct inject_c *ic = (struct inject_c *) ti->private;

	if(argc!=1) {
		return r;
	}
	if(strcasecmp(argv[0], "test")==0) {
		DMDEBUG("%s test message", __func__);
	} else if (strcasecmp(argv[0], "start")==0) {
		DMDEBUG("%s enable injection", __func__);
		ic->inject_enable = true;
	} else if (strcasecmp(argv[0], "stop")==0) {
		DMDEBUG("%s disable injection", __func__);
		ic->inject_enable = false;
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
	.message = inject_message,
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
