
#include "linux/btrfs.h"
#include "../fs/btrfs/ctree.h"

void getOwner(long long unsigned treeId, char *ownerName, int blkNo){
	if(blkNo == 16 || blkNo == 16384) {
		strcpy(ownerName, "superBlock");
		return;
	}
	else if (treeId > 20 || treeId < 0){
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
		getOwner(bh->owner, ownerName, bio->bi_iter.bi_sector/ 8);
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
		getOwner(bh->owner, ownerName,bio->bi_iter.bi_sector/ 8);
		printk( KERN_CRIT "XXX generation = %llu, owner = %llu:%s blkNo=%lu nritems= %u level %d\n", bh->generation, bh->owner , ownerName, bio->bi_iter.bi_sector / 8 ,bh->nritems, bh->level);
                bvec_kunmap_irq(data, &flags);
		break;
	}
}



