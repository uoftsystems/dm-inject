/*
 *
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
	int ret = 0;
	struct inject_c *ic;
	unsigned long long tmp = 0;
	char tmp_str[64], dummy;
	struct dm_arg_set as;
	const char *devname;
	struct inject_fs_type *inject_fs_type;

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
	if (as.argc > 0 && sscanf(*as.argv, "%s%c", tmp_str, &dummy) == 1
		&& !request_module("dm-inject-%s", tmp_str)) {
		dm_shift_arg(&as);
	} else if (!request_module("dm-inject-f2fs")) {
		strcpy(tmp_str, "f2fs");
	} else {
		DMDEBUG("Unable to request module dm-inject-%s", tmp_str);
		ti->error = "Unable to request module";
		goto bad;
	}
	DMDEBUG("Requested module: dm-inject-%s", tmp_str);

	if (list_empty(&_fs_list)) {
		DMDEBUG("Unable to register the filesystem");
		ti->error = "Unable to register the filesystem";
		goto bad;
	}

	inject_fs_type = dm_find_inject_fs(tmp_str);
	if (inject_fs_type && try_module_get(inject_fs_type->module)) {
		DMDEBUG("Found inject_fs_type %s, assign it to inject_c", tmp_str);
		ic->fs_t = inject_fs_type;
	} else {
		DMDEBUG("No matching module for filesystem injection: %s", tmp_str);
		ti->error = "No matching module for fileystem injection";
		goto bad;
	}

	INIT_LIST_HEAD(&ic->inject_list);
	ret = ic->fs_t->parse_args(ic, &as, ti->error);
	if (ret) {
		DMDEBUG("Could not parse the specified arguments!");
		ti->error = "Could not parse the specified arguments!";
		goto bad;
	}

	/* Do this last since it impacts ref count. */
	ret = dm_get_device(ti, devname, dm_table_get_mode(ti->table), &ic->dev);
	if (ret) {
		DMDEBUG("Device lookup failed");
		ti->error = "Device lookup failed";
		goto bad;
	}

	ic->src_bdev = NULL;
	ic->inject_enable = false; //injection disabled by default
	ic->global_corrupt_enable = false; //global corruption disabled by default
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
	struct inject_c *ic;
	struct inject_rec *tmp, *tmp2;

	if (!ti)
		return;

	ic = (struct inject_c *) ti->private;
	if (!ic)
		return;

	dm_put_device(ti, ic->dev);
	ic->fs_t->dtr(ic);
	module_put(ic->fs_t->module);
	list_for_each_entry_safe(tmp, tmp2, &ic->inject_list, list) {
		list_del(&tmp->list);
		kfree(tmp);
	}
	kfree(ic);

	/* Explicitly set the private member equal to NULL to avoid
	 * double free operations. */
	ti->private = NULL;
}

// Mapper
static int inject_map(struct dm_target *ti, struct bio *bio)
{
	struct inject_c *ic = (struct inject_c *) ti->private;
	int ret = DM_MAPIO_SUBMITTED, retval;
	struct bio_vec *bvec;
	unsigned int iter;

	DMDEBUG("%s bio op %d sector %lu", __func__, bio_op(bio),
		bio->bi_iter.bi_sector);

	// Drop read-ahead.
	if (bio->bi_opf & REQ_RAHEAD) {
		DMDEBUG("%s fail RAHEAD", __func__);
		return DM_MAPIO_KILL;
	}

	//assign src_bdev to grab superblock if fs is mounted
	//DMDEBUG("%s bio->bi_disk %p bd_super %p", __func__, bio->bi_disk, (bdget_disk(bio->bi_disk, 0))->bd_super);

	//make the request SYNC to prevent merging?
	//bio->bi_opf |= REQ_SYNC;
	//ret = DM_MAPIO_REMAPPED;

	//DMDEBUG("%s sb %p", __func__, sb);
	//intercept and inject F2FS write requests
	//data travelling from from memory to block device
	if (ic->inject_enable) {
		if (bio_op(bio) == REQ_OP_WRITE) {
			for_each_bvec_no_advance(iter, bvec, bio, 0) {
				//DMDEBUG("%s bio %s sector %d blk %d vcnt %d", __func__, RW(bio_op(bio)),
				//		bio->bi_iter.bi_sector, SECTOR_TO_BLOCK(bio->bi_iter.bi_sector), bio->bi_vcnt);
				//DMDEBUG("%s sector %d bi_size %d bi_bvec_done %d bi_idx %d", __func__,
				//		bio->bi_iter.bi_sector, bio->bi_iter.bi_size, bio->bi_iter.bi_bvec_done, bio->bi_iter.bi_idx);
				sector_t sec = bio->bi_iter.bi_sector + (iter >> SECTOR_SHIFT);

                                /*
                                 * In case of data corruption, re-map it as expected.
                                 * Otherwise, check if the operation must be dropped.
                                 */
				retval = ic->fs_t->data_to_dev(ic, bio, bvec, sec);
				//DMDEBUG("%s retval: %d sec: %lu", __func__, retval, sec);
				if (!retval) {
					if ((retval = ic->fs_t->block_to_dev(ic, bio, bvec, sec))) {
						DMDEBUG("%s retval: %d sec: %lu", __func__, retval, sec);
						bio_io_error(bio);

                                                //return DM_MAPIO_KILL;
						return DM_MAPIO_SUBMITTED;
					}
				}
			}
		}
        }

	if (bio->bi_disk)
		ic->src_bdev = bdget_disk(bio->bi_disk, 0);

	//linear mapping
	bio_set_dev(bio, ic->dev->bdev);
	if (bio_sectors(bio)) {
		bio->bi_iter.bi_sector += ic->start;
		bio->bi_iter.bi_sector -= ti->begin;
		ret = DM_MAPIO_REMAPPED;
	}

	return ret;
}

static int inject_end_io(struct dm_target *ti, struct bio *bio, blk_status_t *error)
{
	struct inject_c *ic = (struct inject_c *) ti->private;
	struct bio_vec *bvec;
	unsigned int iter;
	int ret;

	/* Initially, no error has occurred. */
	*error = BLK_STS_OK;

	//DMDEBUG("%s bio op %d sector %d blk %d vcnt %d", __func__, bio_op(bio), bio->bi_iter.bi_sector, SECTOR_TO_BLOCK(bio->bi_iter.bi_sector), bio->bi_vcnt);
	//intercept and inject F2FS read requests
	//data from block device travelling to memory
	if (ic->inject_enable) {
		if (bio_op(bio) == REQ_OP_READ) {
			//the sector count was advanced during the bio
			//sector_t sec = bio->bi_iter.bi_sector - 8;

			//DMDEBUG("%s bio %s sector %d blk %d vcnt %d", __func__, RW(bio_op(bio)), sec, SECTOR_TO_BLOCK(sec), bio->bi_vcnt);
			//DMDEBUG("%s sector %d bi_size %d bi_bvec_done %d bi_idx %d", __func__, bio->bi_iter.bi_sector, bio->bi_iter.bi_size,
			//		bio->bi_iter.bi_bvec_done, bio->bi_iter.bi_idx);
			/*DMDEBUG("%s bio %p io_vec %p page %p len %d off %d", __func__,
				bio, bio->bi_io_vec, bio->bi_io_vec->bv_page,
				bio->bi_io_vec->bv_len, bio->bi_io_vec->bv_offset);*/
			for_each_bvec_no_advance(iter, bvec, bio, 0) {
				sector_t sec = bio->bi_iter.bi_sector +
                                        (iter >> SECTOR_SHIFT) - 8;

				/*
				 * In case of data corruption, treat it as a regular I/O.
				 * Otherwise, check if the operation must be dropped.
				 */
				ret = ic->fs_t->data_from_dev(ic, bio, bvec, sec);
				//DMDEBUG("%s ret: %d sec: %lu", __func__, ret, sec);
				if (!ret && ic->fs_t->block_from_dev(ic, bio, bvec, sec)) {
					DMDEBUG("%s error sec: %lu", __func__, sec);
					*error = BLK_STS_IOERR;
					break;
				}
			}
		}
	}

	return DM_ENDIO_DONE;
}

static int inject_message(struct dm_target *ti, unsigned argc, char **argv)
{
	struct inject_c *ic = (struct inject_c *) ti->private;

	if (argc != 1) {
		DMDEBUG("%s: Additional arguments provided!", __func__);
		return -EINVAL;
	}

	if (!strcasecmp(argv[0], "test"))
		DMDEBUG("%s: test message", __func__);
	else if (!strcasecmp(argv[0], "start")) {
		DMDEBUG("%s: enable injection", __func__);
		ic->inject_enable = true;
	} else if (!strcasecmp(argv[0], "stop")) {
		DMDEBUG("%s: disable injection", __func__);
		ic->inject_enable = false;
	} else if (!strcasecmp(argv[0], "corruption_on")) {
		DMDEBUG("%s: enable global corruption", __func__);
		ic->global_corrupt_enable = true;
	} else if (!strcasecmp(argv[0], "corruption_off")) {
		DMDEBUG("%s: disable global corruption", __func__);
		ic->global_corrupt_enable = false;
	} else
		DMDEBUG("%s: unsupported operation: %s", __func__, argv[0]);

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
	if (r < 0)
		DMERR("dm-inject register failed %d", r);
	else
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
		if (!strcmp(tmp->name, name)) {
			ret = tmp;
			break;
		}
	}
	spin_unlock(&_fs_list_lock);

	return ret;
}
EXPORT_SYMBOL(dm_find_inject_fs);

module_init(dm_inject_init)
module_exit(dm_inject_exit)

MODULE_AUTHOR("Andy Hwang <hwang@cs.toronto.edu>");
MODULE_AUTHOR("Stathis Maneas <smaneas@cs.toronto.edu>");
MODULE_DESCRIPTION(DM_NAME " error injection target");
MODULE_LICENSE("GPL");
