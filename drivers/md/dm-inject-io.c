#include<linux/fs.h>
#include<linux/module.h>
#include<linux/kernel.h>
#include<linux/init.h>
#include <linux/bio.h>
#include <linux/slab.h>
#include <linux/blkdev.h>
#include <linux/device-mapper.h>
#include <linux/blk_types.h>
// not required for ext4
#include "btrfs_helper.h"

#define EXT4 1
#define BTRFS 2

struct my_dm_target {
        struct dm_dev *dev;
        sector_t start;
	bool error;
	int fsType;
	char mode;	// R | W | A
	unsigned long errorBlockNumber;
};

static int io_map(struct dm_target *ti, struct bio *bio)
{
        struct my_dm_target *mdt = (struct my_dm_target *) ti->private;
	unsigned bsz;
	sector_t bno;

	bio_set_dev(bio, mdt->dev->bdev);
	bsz = bio->bi_iter.bi_size;
	
	switch (bio_data_dir(bio)) {
		case READ:
		break;

		case WRITE:
			printk(KERN_INFO "WRITE block = %lu\n",bio->bi_iter.bi_sector / 8);
			bno = bio->bi_iter.bi_sector / 8;
			if(mdt->fsType == BTRFS	) {
				print_header(bio);
			}
			if( mdt->error && bno == mdt->errorBlockNumber && mdt->mode != 'R') {
				printk("WRITE ERROR!!\n");
				printk(KERN_INFO "%s():ERROR ON block = %lu\n",__func__,(bio->bi_iter.bi_sector / 8) - 1);
				bio_io_error(bio);
				return DM_MAPIO_SUBMITTED;
			}
			break;
		default:
			printk(KERN_INFO "%s():NONE block = %lu\n",__func__,bio->bi_iter.bi_sector / 8);
	}

        submit_bio(bio);
        return DM_MAPIO_SUBMITTED;
}

int setup_errorblock(struct my_dm_target *mdt , struct dm_target *ti)
{
	mdt->error = false;
	mdt->mode = 'A';
	return 0;				
} 

static int 
io_ctr(struct dm_target *ti,unsigned int argc,char **argv)
{
        struct my_dm_target *mdt;
        unsigned long long start;
	int r;

        if (argc != 2) {
                ti->error = "Invalid argument count";
                return -EINVAL;
        }
        mdt = kmalloc(sizeof(struct my_dm_target), GFP_KERNEL);
        if(mdt==NULL)
        {
                ti->error = "dm-io: Cannot allocate linear context";
                return -ENOMEM;
        }       
        if(sscanf(argv[1], "%llu", &start)!=1)
        {
                ti->error = "dm-io: Invalid device sector";
                goto bad;
        }
        mdt->start=(sector_t)start;

	r = setup_errorblock(mdt, ti);
        if (dm_get_device(ti, argv[0], dm_table_get_mode(ti->table), &mdt->dev)) {
                ti->error = "dm-io: Device lookup failed";
                goto bad;
        }

	ti->num_flush_bios = 1;
	ti->num_discard_bios = 1;
	//ti->per_bio_data_size = sizeof(struct per_bio_data);
	ti->private = mdt;
        return 0;
  bad:
        kfree(mdt);
        return -EINVAL;
}

static void io_dtr(struct dm_target *ti)
{
        struct my_dm_target *mdt = (struct my_dm_target *) ti->private;
        dm_put_device(ti, mdt->dev);
        kfree(mdt);
}


static int io_end_io(struct dm_target *ti, struct bio *bio, blk_status_t *error)
{
	struct my_dm_target *mdt = ti->private;
	sector_t bno;
	
	// ERROR READ
	if(bio_data_dir(bio) == READ) {
		if(mdt->fsType == EXT4) {
			printk(KERN_INFO "READ block = %lu",(bio->bi_iter.bi_sector / 8) - 1);	
			bno = (bio->bi_iter.bi_sector / 8) - 1;
		}else if(mdt->fsType == BTRFS){
			printk(KERN_INFO "READ block = %lu",(bio->bi_iter.bi_sector / 8) - 4);	
			bno = (bio->bi_iter.bi_sector / 8) - 4;
			print_header_end(bio);
		}else {
			bno = (bio->bi_iter.bi_sector / 8) - 1;
		}
		if( mdt->error && bno == mdt->errorBlockNumber && mdt->mode != 'W') {
			printk("READ ERROR!!\n");
			printk(KERN_INFO "ERROR ON block = %lu\n", bno);	
			*error = BLK_STS_IOERR;
		//	mdt->error = false;
		}
	}
	return DM_ENDIO_DONE;
}

static const struct block_device_operations dm_blk_dops;

static int io_message(struct dm_target *ti,unsigned int argc, char **argv, char *unsusedarg1, unsigned unsusedarg2)
{
        struct my_dm_target *mdt = (struct my_dm_target *) ti->private;
	if(!strcmp(argv[0],"Y")){
		printk(KERN_INFO "CORRUPTION ENABLED\n");
		mdt->error = true;
	}else if(!strcmp(argv[0],"N")){
		printk(KERN_INFO "CORRUPTION DISABLED\n");
		mdt->error = false;
	}else{
		printk(KERN_INFO "MESSAGE NOT RECOGNIZED %s\n", argv[0]);
	}
	kstrtoul(argv[1], 10, &mdt->errorBlockNumber );
	printk(KERN_INFO "%s(): SETUP ERROR BLOCK SEQUENCE NUMBER TO %lu\n",__func__, mdt->errorBlockNumber);
	if(argc >= 3) {
		if(!strcmp(argv[2],"R")){
			printk(KERN_INFO "READ MODE ENABLED\n");
			mdt->mode = 'R';
		}else if(!strcmp(argv[2],"W")){
			printk(KERN_INFO "WRITE MODE ENABLED\n");
			mdt->mode = 'W';
		}else{
			printk(KERN_INFO "WILL CORRUPT BOTH READ AND WRITE MODE\n");
			mdt->mode = 'A';
		}
	}

	if(argc >= 4) {
		if(!strcmp(argv[3],"ext4")) {
			printk(KERN_INFO "setting up device mapper for ext4\n");
			mdt->fsType = EXT4;
		}
		else if(!strcmp(argv[3],"btrfs")) {
			printk(KERN_INFO "setting up device mapper for btrfs\n");
			mdt->fsType = BTRFS;
		}else {
			printk(KERN_INFO "could not recognize file type %s, setting default to ext4\n", argv[3]);
			mdt->fsType = EXT4;
		}
	}
	else {
		printk(KERN_INFO "could not detect fourth argument\n");
	}	
	return 0;
}

static struct target_type io = {
        .name = "io",
        .version = {1,0,0},
        .module = THIS_MODULE,
        .ctr = io_ctr,
        .dtr = io_dtr,
        .map = io_map,
	.end_io = io_end_io,
	.message = io_message,
};

/*---------Module Functions -----------------*/

static int init_io(void)
{
        int result;
        result = dm_register_target(&io);
        if(result < 0)
                printk(KERN_CRIT "\n Error in registering target \n");
        return 0;
}

static void cleanup_io(void)
{
        dm_unregister_target(&io);
}
module_init(init_io);
module_exit(cleanup_io);
MODULE_LICENSE("GPL");
