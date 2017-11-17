/*

 * dm-inject device mapper target
 * referencing dm-linear, dm-zero etc
 */

#include "dm-inject.h"
#define DM_MSG_PREFIX "inject"
static LIST_HEAD(_fs_list);
static DEFINE_SPINLOCK(_fs_list_lock);

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

	//see if fs type is specified.
	//if not default to f2fs (TODO:remove f2fs)
	if(as.argc > 0 && sscanf(*as.argv, "%s%*c", tmp_str) == 1
		&& request_module("dm-inject-%s", tmp_str) == 0) {
		dm_shift_arg(&as);
	} else if(request_module("dm-inject-f2fs") == 0) {
		strcpy(tmp_str, "f2fs");
	} else {
		DMDEBUG("unable to request module dm-inject-%s", tmp_str);
		goto bad;
	}

	DMDEBUG("request module dm-inject-%s", tmp_str);

	if(list_empty(&_fs_list)) {
		DMDEBUG("unable to register any inject filesystem");
		goto bad;
	}

	if(dm_find_inject_fs(tmp_str)!=NULL) {
		DMDEBUG("found inject_fs_type %s, assign to inject_c", tmp_str);
		ic->fs_t = dm_find_inject_fs(tmp_str);
	} else {
		DMDEBUG("no matching module for filesystem injection: %s", tmp_str);
		goto bad;
	}

	INIT_LIST_HEAD(&ic->inject_list);

	ret = ic->fs_t->parse_args(ic, &as, ti->error);

	if(ret)
		return ret;

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
	ic->fs_t->ctr(ic);
	return 0;
	bad:
		kfree(ic);
		return ret;
}

// Destructor
static void inject_dtr(struct dm_target *ti)
{
	struct inject_c *ic = (struct inject_c *) ti->private;
	struct inject_rec *tmp, *tmp2;
	dm_put_device(ti, ic->dev);
	ic->fs_t->dtr(ic);
	list_for_each_entry_safe(tmp, tmp2, &ic->inject_list, list) {
		list_del(&tmp->list);
		kfree(tmp);
	}
	kfree(ic);
}

/*
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

static int check_corrupt_block(struct inject_c *ic, u32 blk)
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
}*/

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

// Mapper
static int inject_map(struct dm_target *ti, struct bio *bio)
{
	struct inject_c *ic = (struct inject_c *) ti->private;
	struct super_block *sb;
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

	//DMDEBUG("%s sb %p", __func__, sb);
	//intercept and inject F2FS write requests
	//data travelling from from memory to block device
	if(ic->inject_enable)
		if(bio_op(bio)==REQ_OP_WRITE) {
			//DMDEBUG("%s bio %s sector %d blk %d vcnt %d", __func__, RW(bio_op(bio)), bio->bi_iter.bi_sector, SECTOR_TO_BLOCK(bio->bi_iter.bi_sector), bio->bi_vcnt);
			//DMDEBUG("%s sector %d bi_size %d bi_bvec_done %d bi_idx %d", __func__, bio->bi_iter.bi_sector, bio->bi_iter.bi_size, bio->bi_iter.bi_bvec_done, bio->bi_iter.bi_idx);
			if(ic->fs_t->data_to_dev(ic, bio)!=DM_INJECT_NONE)
				ret = DM_MAPIO_REMAPPED;
			else if(ic->fs_t->block_to_dev(ic, bio))
				ret = DM_MAPIO_KILL;
		}

	if(ret==DM_MAPIO_SUBMITTED)
		bio_endio(bio);
	//dump_stack();
	return ret;
}

static int inject_end_io(struct dm_target *ti, struct bio *bio, blk_status_t *error)
{
	struct inject_c *ic = (struct inject_c *) ti->private;

	//DMDEBUG("%s bio op %d sector %d blk %d vcnt %d", __func__, bio_op(bio), bio->bi_iter.bi_sector, SECTOR_TO_BLOCK(bio->bi_iter.bi_sector), bio->bi_vcnt);
	//the sector count was advanced during the bio
	sector_t sec = bio->bi_iter.bi_sector-8;

	//intercept and inject F2FS read requests
	//data from block device travelling to memory
	if(ic->inject_enable)
		if(bio_op(bio)==REQ_OP_READ) {
			//DMDEBUG("%s bio %s sector %d blk %d vcnt %d", __func__, RW(bio_op(bio)), sec, SECTOR_TO_BLOCK(sec), bio->bi_vcnt);
			//DMDEBUG("%s sector %d bi_size %d bi_bvec_done %d bi_idx %d", __func__, bio->bi_iter.bi_sector, bio->bi_iter.bi_size, bio->bi_iter.bi_bvec_done, bio->bi_iter.bi_idx);
			/*DMDEBUG("%s bio %p io_vec %p page %p len %d off %d", __func__,
				bio, bio->bi_io_vec, bio->bi_io_vec->bv_page, 
				bio->bi_io_vec->bv_len, bio->bi_io_vec->bv_offset);*/
			if(ic->fs_t->data_from_dev(ic, bio)!=DM_INJECT_NONE) {
				//we corrupted some data, can do accounting here
				//but still pretend to be normal
				return DM_ENDIO_DONE;
			} else if(ic->fs_t->block_from_dev(ic, bio)) {
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
	DMDEBUG("dm-inject target registered");
	return r;
}

static void __exit dm_inject_exit(void)
{
	dm_unregister_target(&inject_target);
	DMDEBUG("dm-inject target unregistered");
}

int dm_register_inject_fs(struct inject_fs_type *fs)
{
	spin_lock(&_fs_list_lock);
	list_add_tail(&fs->list, &_fs_list);
	spin_unlock(&_fs_list_lock);
	DMDEBUG("registered %s", fs->name);
	return 0;
}
EXPORT_SYMBOL(dm_register_inject_fs);

int dm_unregister_inject_fs(struct inject_fs_type *fs)
{
	spin_lock(&_fs_list_lock);
	list_del(&fs->list);
	spin_unlock(&_fs_list_lock);
	DMDEBUG("unregistered %s", fs->name);
	return 0;
}
EXPORT_SYMBOL(dm_unregister_inject_fs);

struct inject_fs_type *dm_find_inject_fs(const char* name)
{
	struct inject_fs_type *tmp, *ret = NULL;
	spin_lock(&_fs_list_lock);
	list_for_each_entry(tmp, &_fs_list, list) {
		if(strcmp(tmp->name, name)==0)
			ret = tmp;
	}
	spin_unlock(&_fs_list_lock);
	return ret;
}
EXPORT_SYMBOL(dm_find_inject_fs);

module_init(dm_inject_init)
module_exit(dm_inject_exit)

MODULE_AUTHOR("Andy Hwang <hwang@cs.toronto.edu>");
MODULE_DESCRIPTION(DM_NAME " error injection target");
MODULE_LICENSE("GPL");
