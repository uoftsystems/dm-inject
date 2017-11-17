/*
 * dm-inject-ext4 device mapper target
 * template for ext4
 */


#include "dm-inject.h"

#define DM_MSG_PREFIX "inject ext4"

#define IS_EXT4(sb) ((sb) && ((sb)->s_magic == EXT4_SUPER_MAGIC))

struct ext4_context {
	int dummy;
};

int ext4_inject_ctr(struct inject_c *ic)
{
	struct ext4_context *fsc;
	fsc = kmalloc(sizeof(*fsc), GFP_KERNEL);
	if(!fsc) {
		DMERR("%s failed", __func__);
		return -ENOMEM;
	}
	ic->context = fsc;
	DMDEBUG("%s", __func__);
	return 0;
}

void ext4_inject_dtr(struct inject_c *ic)
{
	struct ext4_context *fsc = (struct ext4_context *) ic->context;
	if(fsc)
		kfree(fsc);
	DMDEBUG("%s", __func__);
	return;
}

int ext4_parse_args(struct inject_c *ic, struct dm_arg_set *as, char *error)
{
	while(as->argc > 0) {
		char *arg = dm_shift_arg(as);
		DMDEBUG("%s arg %s", __func__, arg);
	}
	return 0;
}

bool ext4_block_from_dev(struct inject_c *ic, struct bio *bio)
{
	DMDEBUG("%s", __func__);
	return false;
}
bool ext4_block_to_dev(struct inject_c *ic, struct bio *bio)
{
	DMDEBUG("%s", __func__);
	return false;
}

int ext4_data_from_dev(struct inject_c *ic, struct bio *bio)
{
	DMDEBUG("%s", __func__);
	return DM_INJECT_NONE;
}

int ext4_data_to_dev(struct inject_c *ic, struct bio *bio)
{
	DMDEBUG("%s", __func__);
	return DM_INJECT_NONE;
}

static struct inject_fs_type ext4_fs = {
	.name = "ext4",
	.module = THIS_MODULE,
	.ctr = ext4_inject_ctr,
	.dtr = ext4_inject_dtr,
	.parse_args = ext4_parse_args,
	.block_from_dev = ext4_block_from_dev,
	.block_to_dev = ext4_block_to_dev,
	.data_from_dev = ext4_data_from_dev,
	.data_to_dev = ext4_data_to_dev,
};

static int __init dm_inject_ext4_init(void)
{
	dm_register_inject_fs(&ext4_fs);
	DMINFO("init");
	return 0;
}
static void __exit dm_inject_ext4_exit(void)
{
	dm_unregister_inject_fs(&ext4_fs);
	DMINFO("exit");
}

module_init(dm_inject_ext4_init)
module_exit(dm_inject_ext4_exit)

MODULE_AUTHOR("Syslab <syslab.cs.toronto.edu>");
MODULE_DESCRIPTION(DM_NAME " ext4 error injection target");
MODULE_LICENSE("GPL");
