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

#define EXT4 1
#define BTRFS 2

#define for_each_bvec_no_advance(size, bvec, bio, start)	\
for(size = (start), bvec = (bio)->bi_io_vec;				\
	size < ((bio)->bi_iter.bi_size + (bio)->bi_iter.bi_bvec_done \
	+ (bio)->bi_iter.bi_idx*PAGE_SIZE);						\
	size += (bvec)->bv_len, (bvec)++)


//#include <linux/error.h>
#define DM_MAPIO_KILL 4
#define START_SECTOR (64 * 1024)
#define END_SECTOR ((68 * 1024) -1)

/* This is a structure which will store  information about the underlying device 
*  Param:
* dev : underlying device
* start:  Starting sector number of the device
*/

struct my_dm_target {
        struct dm_dev *dev;
        sector_t start;
	bool corrupt;
	char mode;	
	int fsType;	
	unsigned long count;
	unsigned long corruptBlockSequenceNumber;
};

//copied from dm-zero

void shorn_bio(struct bio *bio)
{

	unsigned long flags;
        struct bio_vec *bvec;
	unsigned int iter;
        char *data;
	int offset, size;
	int i;

        printk(KERN_INFO "%s() corrupting bio %lu",__func__, ((bio->bi_iter.bi_sector / 8) -1) );

	for_each_bvec_no_advance(iter, bvec, bio, 0) {
	//	printk(KERN_INFO "length (probably segment length) = %d", bvec->bv_len);
		
		offset = (int)((3 * bvec->bv_len) / 8);
		size = (int)((5 * bvec->bv_len) / 8);
		printk (KERN_INFO "zeroing offset = %d size = %d\n", offset, size);
		printk(KERN_INFO "sector = %lu",bio->bi_iter.bi_sector);
                data = bvec_kmap_irq(bvec, &flags);
		for(i = 0; i < size ; i++)
			memset(data + offset + i, 0, 1);
//                memset(data + offset, 0, size);
                flush_dcache_page(bvec->bv_page);
                bvec_kunmap_irq(data, &flags);
        }
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
		if((bio->bi_iter.bi_sector / 8 == 16)||(bio->bi_iter.bi_sector / 8 == 16384)) {
			strcpy(ownerName, "superBlock");
		}
		printk( KERN_CRIT "XXX generation = %llu, owner = %llu:%s blkNo=%lu nritems= %u level %d\n", bh->generation, bh->owner , ownerName, bio->bi_iter.bi_sector / 8 ,bh->nritems, bh->level);
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
		if((bio->bi_iter.bi_sector / 8 == 16)||(bio->bi_iter.bi_sector / 8 == 16384)) {
			strcpy(ownerName, "superBlock");
		}
		printk( KERN_CRIT "XXX generation = %llu, owner = %llu:%s blkNo=%lu nritems= %u level %d\n", bh->generation, bh->owner , ownerName, bio->bi_iter.bi_sector / 8 ,bh->nritems, bh->level);
                bvec_kunmap_irq(data, &flags);
		break;
	}
}

static int shorn_map(struct dm_target *ti, struct bio *bio/*,union map_info *map_context*/)
{
        struct my_dm_target *mdt = (struct my_dm_target *) ti->private;
	unsigned bsz;

	size_t bno;
	bio_set_dev(bio, mdt->dev->bdev);

	bsz = bio->bi_iter.bi_size;
	bno = bio->bi_iter.bi_sector / 8;

	switch (bio_data_dir(bio)) {
		case READ:
			break;
		case WRITE:

			printk(KERN_INFO "WRITE block = %lu\n",bio->bi_iter.bi_sector / 8);
			if(mdt->corrupt && bno == mdt->corruptBlockSequenceNumber) {
				printk(KERN_INFO "%s():CORRUPTING BIO %lu\n",__func__,bio->bi_iter.bi_sector / 8);
				shorn_bio(bio);
				printk(KERN_INFO "%s(): calling bio_endio\n",__func__);
				printk(KERN_INFO "%s(): back from bio_endio\n",__func__);
				// shorn writes are transient, i.e. a subsiquent write of a previously
				// shorn block (eg. during a fsck repair) would not cause a shear.
				mdt->corrupt = false;
				return DM_MAPIO_REMAPPED;
			}
			mdt->count++;
			if(mdt->fsType == BTRFS) {		
				print_header(bio);
			}
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
shorn_ctr(struct dm_target *ti,unsigned int argc,char **argv)
{
        struct my_dm_target *mdt;
        unsigned long long start;
	int r;

	printk("%s():\n",__func__);	
        if (argc != 2) {
                ti->error = "Invalid argument count";
                return -EINVAL;
        }
        mdt = kmalloc(sizeof(struct my_dm_target), GFP_KERNEL);
        if(mdt==NULL)
        {
                ti->error = "dm-shorn: Cannot allocate linear context";
                return -ENOMEM;
        }       
        if(sscanf(argv[1], "%llu", &start)!=1)
        {
                ti->error = "dm-shorn: Invalid device sector";
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
                ti->error = "dm-shorn: Device lookup failed";
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

static void shorn_dtr(struct dm_target *ti)
{
        struct my_dm_target *mdt = (struct my_dm_target *) ti->private;
	printk("%s():\n",__func__);	
        dm_put_device(ti, mdt->dev);
        kfree(mdt);
}


static int shorn_end_io(struct dm_target *ti, struct bio *bio, blk_status_t *error)
{

	//unsigned long blockNo;
	struct my_dm_target *mdt = ti->private;
	if(bio_data_dir(bio) == READ) {
		size_t bno = bio->bi_iter.bi_sector / 8;
		printk(KERN_INFO "READ block = %lu\n",bio->bi_iter.bi_sector / 8);
		if(mdt->fsType == BTRFS) {		
			print_header_end(bio);
		}
		if(mdt->corrupt && mdt->corruptBlockSequenceNumber == bno) {
			printk(KERN_INFO "%s():CORRUPTING BIO %lu\n",__func__,bio->bi_iter.bi_sector / 8);
			shorn_bio(bio);
			// a shorn read is sticky - i.e. re-reads of a previously shorn
			// written block will continue to be shorn.
			//mdt->corrupt = false;
			mdt->count++;
			return DM_ENDIO_DONE;
		}
		mdt->count++;
	} else if(bio_data_dir(bio) == WRITE) { ;
//		printk(KERN_INFO "%s():WRITE block = %lu\n",__func__,bio->bi_iter.bi_sector / 8);	
	} else {
		printk(KERN_INFO "%s():NONE block = %lu\n",__func__,bio->bi_iter.bi_sector / 8);
//		print_header_end(bio);
	}
        return DM_ENDIO_DONE;
}

/*
 * This structure is fops for basic target.
 */

static const struct block_device_operations dm_blk_dops;

//static int shorn_message(struct dm_target *ti,unsigned int argc, char **argv){
static int shorn_message(struct dm_target *ti,unsigned int argc, char **argv, char *unused1, unsigned unused2){
//	printk(KERN_INFO "dm-shorn message received: %s", argv[0]);

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
        kstrtoul(argv[1], 10, &mdt->corruptBlockSequenceNumber);
        printk(KERN_INFO "%s(): SETUP ERROR BLOCK SEQUENCE NUMBER TO %lu\n",__func__, mdt->corruptBlockSequenceNumber);
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

	mdt->count =1;
	return 0;
}

static struct target_type shorn = {
        .name = "shorn",
        .version = {1,0,0},
        .module = THIS_MODULE,
        .ctr = shorn_ctr,
        .dtr = shorn_dtr,
        .map = shorn_map,
	.end_io = shorn_end_io,
	.message = shorn_message,
};

/*---------Module Functions -----------------*/

static int init_shorn(void)
{
        int result;
        result = dm_register_target(&shorn);
        if(result < 0)
                printk(KERN_CRIT "\n Error in registering target \n");
        return 0;
}

static void cleanup_shorn(void)
{
        dm_unregister_target(&shorn);
}
module_init(init_shorn);
module_exit(cleanup_shorn);
MODULE_LICENSE("GPL");
