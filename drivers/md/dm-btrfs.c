#include<linux/fs.h>
#include<linux/module.h>
#include<linux/kernel.h>
#include<linux/init.h>
#include <linux/bio.h>
#include <linux/slab.h>
#include <linux/blkdev.h>
#include <linux/device-mapper.h>
#include <linux/blk_types.h>
#include "linux/btrfs.h"
#include "../fs/btrfs/ctree.h"

//#include <linux/error.h>
#define DM_MAPIO_KILL 4
#define START_SECTOR (64 * 1024)
#define END_SECTOR ((68 * 1024) -1)

/* This is a structure which will store  information about the underlying device 
*  Param:
* dev : underlying device
* start:  Starting sector number of the device
*/

/*
* every tree block (leaf or node) starts with this header.
*/

struct my_dm_target {
        struct dm_dev *dev;
        sector_t start;
//	unsigned down_interval;
//	unsigned up_interval;
	bool corrupt;
	unsigned long count;
	unsigned long corruptBlockSequenceNumber;
};

//copied from dm-zero

void corrupt_bio(struct bio *bio)
{
        unsigned long flags;
        struct bio_vec bv;
        struct bvec_iter iter;
	char *data;
	int i;

//	bio->bi_iter.bi_sector -= 32; // moving back 16384 bytes

	printk(KERN_INFO "%s() corrupting bio %lu \n",__func__, bio->bi_iter.bi_sector / 8 );

        bio_for_each_segment(bv, bio, iter) {
		printk(KERN_INFO "---corrupting segment--- len %d\n", bv.bv_len);
                data = bvec_kmap_irq(&bv, &flags);
		for (i = 0 ; i < bv.bv_len; i++) {
	        	memset(data + i, ~data[i],1);
		}
                flush_dcache_page(bv.bv_page);
                bvec_kunmap_irq(data, &flags);
        }
//	printk(KERN_INFO "bv len = %d\n", bv.bv_len);
}

void getOwner(long long unsigned treeId, char *ownerName){
	if (treeId > 20 || treeId < 0){
		strcpy(ownerName, "default");
		return;
	}else{
		switch(treeId)
		{
			case 1:
				strcpy(ownerName, "rootTree");
				break;
			case 2:
				strcpy(ownerName, "extentTree");
				break;
			case 3:
				strcpy(ownerName, "chunkTree");
				break;
			case 4:
				strcpy(ownerName, "devTree");
				break;
			case 5:
				strcpy(ownerName, "fsTree");
				break;
			case 6:
				strcpy(ownerName, "rootTreeDir");
				break;
			case 7:
				strcpy(ownerName, "csumTree");
				break;
			case 8:
				strcpy(ownerName, "quotaTree");
				break;
			case 9:
				strcpy(ownerName, "uuidTree");
				break;
		}		
	}
}

void print_header(struct bio *bio)
{
	struct btrfs_header *bh;

        unsigned long flags;
        struct bio_vec bv;
        struct bvec_iter iter;
	char ownerName[40];

	iter = bio->bi_iter;

//	printk(KERN_INFO "%s(): bi_sector = %lu bi_size = %u bi_idx = %u bi_bvec_done = %u\n",__func__ ,iter.bi_sector, iter.bi_size, iter.bi_idx, iter.bi_bvec_done);

	bio_for_each_segment(bv, bio, iter) {
                char *data = bvec_kmap_irq(&bv, &flags);
		bh = (struct btrfs_header *) data;
		getOwner(bh->owner, ownerName);
		printk( KERN_CRIT "XXX generation = %llu, owner = %llu:%s blkNo=%lu nritems= %u level %d bytenr=%llu\n", bh->generation, bh->owner , ownerName, bio->bi_iter.bi_sector / 8 ,bh->nritems, bh->level, bh->bytenr);
                bvec_kunmap_irq(data, &flags);
		break;
        }
}

void print_header_end(struct bio *bio)
{
	struct btrfs_header *bh;

        unsigned long flags;
        struct bio_vec bv;
        struct bvec_iter iter, start;
	char ownerName[40];
	unsigned int idx;

	// reset iter	
	start = bio->bi_iter;
	idx = start.bi_idx;
//	printk(KERN_INFO "%s(): PREV bi_sector = %lu bi_size = %u bi_idx = %u bi_bvec_done = %u\n",__func__ ,start.bi_sector, start.bi_size, start.bi_idx, start.bi_bvec_done);
	start.bi_sector = bio->bi_iter.bi_sector - (idx * 512 * 8);
	start.bi_size = idx * 512 * 8;
	start.bi_idx = 0;
	start.bi_bvec_done = 0;

//	printk(KERN_INFO "%s(): NEW bi_sector = %lu bi_size = %u bi_idx = %u bi_bvec_done = %u\n",__func__ ,start.bi_sector, start.bi_size, start.bi_idx, start.bi_bvec_done);

	__bio_for_each_segment(bv, bio, iter, start)
	{
	        char *data = bvec_kmap_irq(&bv, &flags);
		bh = (struct btrfs_header *) data;
		getOwner(bh->owner, ownerName);
		printk( KERN_CRIT "XXX generation = %llu, owner = %llu:%s blkNo=%lu nritems= %u level %d\n", bh->generation, bh->owner , ownerName, bio->bi_iter.bi_sector / 8 ,bh->nritems, bh->level);
                bvec_kunmap_irq(data, &flags);
		break;
	}
}

static int btrfs_map(struct dm_target *ti, struct bio *bio/*,union map_info *map_context*/)
{
        struct my_dm_target *mdt = (struct my_dm_target *) ti->private;
	unsigned bsz;
	long bno;	

	//struct per_bio_data *pb = dm_per_bio_data(bio, sizeof(struct per_bio_data));
	//pb->bio_submitted = false;

	//unsigned long blockNo;
//        bio->bi_bdev = mdt->dev->bdev;
	bio_set_dev(bio, mdt->dev->bdev);

	//sector_t start_sector = START_SECTOR;
	//sector_t end_sector = END_SECTOR;
	bsz = bio->bi_iter.bi_size;
	
	switch (bio_data_dir(bio)) {
		case READ:
			bno = bio->bi_iter.bi_sector / 8;
			if(mdt->corrupt && bno == mdt->corruptBlockSequenceNumber) {
				printk(KERN_INFO "%s():READ block = %lu\n",__func__,bio->bi_iter.bi_sector / 8);
				printk(KERN_INFO "%s():CORRUPTING BIO %lu\n",__func__,bio->bi_iter.bi_sector / 8);
				corrupt_bio(bio);
				// refer dm-zero
			//	printk(KERN_INFO "%s(): calling bio_endio\n",__func__);
				bio_endio(bio);
			//	printk(KERN_INFO "%s(): back from bio_endio\n",__func__);
				//mdt->corrupt = false;
				return DM_MAPIO_SUBMITTED;
			}
			mdt->count++;
		break;
		case WRITE:
			printk(KERN_INFO "%s():WRITE block = %lu\n",__func__,bio->bi_iter.bi_sector / 8);	
			print_header(bio);
			break;
		
		default:
			printk(KERN_INFO "%s():NONE block = %lu\n",__func__,bio->bi_iter.bi_sector / 8);
	}

        submit_bio(bio);
        return DM_MAPIO_SUBMITTED;
}

// copied from parse_features.

int setup_corruption(struct my_dm_target *mdt , struct dm_target *ti)
{
	mdt->corrupt = false;
	return 0;				
} 

static int 
btrfs_ctr(struct dm_target *ti,unsigned int argc,char **argv)
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
                ti->error = "dm-btrfs: Cannot allocate linear context";
                return -ENOMEM;
        }       
        if(sscanf(argv[1], "%llu", &start)!=1)
        {
                ti->error = "dm-btrfs: Invalid device sector";
                goto bad;
        }
        mdt->start=(sector_t)start;


	// set corruption property:
	
	r = setup_corruption(mdt, ti);
       /* dm_get_table_mode 
         * Gives out you the Permissions of device mapper table. 
         * This table is nothing but the table which gets created
         * when we execute dmsetup create. This is one of the
         * Data structure used by device mapper for keeping track of its devices.
         *
         * dm_get_device 
         * The function sets the mdt->dev field to underlying device dev structure.
         */
        if (dm_get_device(ti, argv[0], dm_table_get_mode(ti->table), &mdt->dev)) {
                ti->error = "dm-btrfs: Device lookup failed";
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

/*
 * This is destruction function
 * This gets called when we remove a device of type basic target. The function gets 
 * called per device. 
 */

static void btrfs_dtr(struct dm_target *ti)
{
        struct my_dm_target *mdt = (struct my_dm_target *) ti->private;
        dm_put_device(ti, mdt->dev);
        kfree(mdt);
}


static int btrfs_end_io(struct dm_target *ti, struct bio *bio, blk_status_t *error)
{
	long bno;
        struct my_dm_target *mdt = (struct my_dm_target *) ti->private;
	//unsigned long blockNo;
	if(bio_data_dir(bio) == READ) {
		printk(KERN_INFO "%s():READ block = %lu\n",__func__,bio->bi_iter.bi_sector / 8 - 4);	
		print_header_end(bio);
		bno = bio->bi_iter.bi_sector / 8 - 4;
		if(mdt->corrupt && bno == mdt->corruptBlockSequenceNumber) {
			printk(KERN_INFO "%s():READ block = %lu\n",__func__,bio->bi_iter.bi_sector / 8);
			printk(KERN_INFO "%s():CORRUPTING BIO %lu\n",__func__,bio->bi_iter.bi_sector / 8);
			corrupt_bio(bio);
//			mdt->corrupt = false;
		}	
		mdt->count++;
	}         
	return DM_ENDIO_DONE;
}

/*
 * This structure is fops for basic target.
 */

static const struct block_device_operations dm_blk_dops;

static int btrfs_message(struct dm_target *ti,unsigned int argc, char **argv,char *unused1, unsigned unused2 ){
//	printk(KERN_INFO "dm-btrfs message received: %s", argv[0]);

        struct my_dm_target *mdt = (struct my_dm_target *) ti->private;
	if(!strcmp(argv[0],"Y")){
		printk(KERN_INFO "CORRUPTION ENABLED\n");
		mdt->corrupt = true;
	}else if(!strcmp(argv[0],"N")){
		printk(KERN_INFO "CORRUPTION DISABLED\n");
		mdt->corrupt = false;
	}else{
		printk(KERN_INFO "MESSAGE NOT RECOGNIZED %s\n", argv[0]);
	}
	kstrtoul(argv[1], 10, &mdt->corruptBlockSequenceNumber );
	printk(KERN_INFO "%s(): SETUP CORRUPT BLOCK SEQUENCE NUMBER TO %lu\n",__func__, mdt->corruptBlockSequenceNumber);
	mdt->count =0;
	return 0;
}

static struct target_type btrfs = {
        .name = "btrfs",
        .version = {1,0,0},
        .module = THIS_MODULE,
        .ctr = btrfs_ctr,
        .dtr = btrfs_dtr,
        .map = btrfs_map,
	.end_io = btrfs_end_io,
	.message = btrfs_message,
};

/*---------Module Functions -----------------*/

static int init_btrfs(void)
{
        int result;
        result = dm_register_target(&btrfs);
        if(result < 0)
                printk(KERN_CRIT "\n Error in registering target \n");
        return 0;
}

static void cleanup_btrfs(void)
{
        dm_unregister_target(&btrfs);
}
module_init(init_btrfs);
module_exit(cleanup_btrfs);
MODULE_LICENSE("GPL");
