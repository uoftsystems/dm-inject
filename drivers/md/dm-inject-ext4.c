/*
 * dm-inject-ext4 device mapper target
 * template for ext4
 */

#include "dm-inject.h"
#include "../../fs/ext4/ext4.h"

#define DM_MSG_PREFIX ""
#define IS_EXT4(sb) ((sb) && ((sb)->s_magic == EXT4_SUPER_MAGIC))

//EXT4 Specific Structures
#define DM_INJECT_EXT4_SB			0x0001
#define DM_INJECT_EXT4_BG			0x0002
#define DM_INJECT_EXT4_DATA_BITMAP		0x0004
#define DM_INJECT_EXT4_INODE_BITMAP		0x0008
#define DM_INJECT_EXT4_INDIRECT_DATA_MAP	0x0020
#define	DM_INJECT_EXT4_EXTENT_TABLE		0x0040
#define	DM_INJECT_EXT4_JOURNAL			0x0080
#define	DM_INJECT_EXT4_EXTENDED_ATTRIBUTES	0x0100
//Common FileSystem Structures
#define DM_INJECT_EXT4_INODE			0x0200
#define DM_INJECT_EXT4_DIRECTORY		0x0400
#define DM_INJECT_EXT4_BLOCK			0x0800

struct my_ext4_bg_info {
	int bg_block_bitmap_bno;
	int bg_inode_bitmap_bno;
	int bg_inode_table_bno; 	
};

struct ext4_context {
	struct ext4_super_block sb;	
	struct my_ext4_bg_info *gdt;
	bool initSuper;
	int dummy;
	int numGroups;
};

void initSB(struct inject_c *ic)
{
	struct bio *bgt_bio;
	struct page *bgt_bio_page;
	int sz = 0;
	int len=PAGE_SIZE;
	struct ext4_context *fsc;
	int blocks_count, blocks_per_group;
	unsigned char *bg_page_addr;

	if(ic->context == NULL) {
		DMDEBUG("%s():uninitialized injector context",__func__);
		return;
	}

	fsc = ic->context;

	bgt_bio_page = alloc_page(GFP_NOIO | __GFP_NOFAIL);
	if(bgt_bio_page == NULL){
		DMDEBUG("bgt_bio_page is null");
	}

	bgt_bio = bio_alloc(GFP_KERNEL, 1);

	if(bgt_bio == NULL) {
		DMDEBUG("bgt_bio is null");
	}

	if(ic->dev == NULL){
		DMDEBUG("ic device not set, returning");
		return;
	}

	if(ic->dev->bdev == NULL) {
		DMDEBUG("ic device bdev not set, returning");
	}

	bio_set_dev(bgt_bio, ic->dev->bdev);
	bgt_bio->bi_iter.bi_sector = 0;
	bgt_bio->bi_end_io = NULL;
	bgt_bio->bi_private = NULL;

	sz = bio_add_page(bgt_bio, bgt_bio_page, PAGE_SIZE, 0);


	if(sz < PAGE_SIZE) {
		DMDEBUG("%s failed bio_add_page size allocated = %d",__func__, len);
		bio_put(bgt_bio);
		return;
	}

	bio_set_op_attrs(bgt_bio, REQ_OP_READ, REQ_META|REQ_PRIO|REQ_SYNC);
	submit_bio_wait(bgt_bio);
	DMDEBUG("%s():submit_bio done!",__func__);

	bg_page_addr = (unsigned char*)page_address(bgt_bio->bi_io_vec->bv_page);
	if(bg_page_addr == NULL){
		DMDEBUG("bg_page_addr assigned null");
		return;
	}else {
		DMDEBUG("bg_page_addr is not null");
		memcpy(&(fsc->sb),bg_page_addr + 1024,sizeof(struct ext4_super_block));
		blocks_count = fsc->sb.s_blocks_count_lo; // total number of blocks
		blocks_per_group = fsc->sb.s_blocks_per_group;	// total number of blocks per group
		fsc->numGroups = (blocks_count + blocks_per_group - 1) / blocks_per_group;
		fsc->initSuper = true;
		DMDEBUG("num of groups = %d fsc->s_log_groups_per_flex = %d fsc->s_desc_size = %d", fsc->numGroups, fsc->sb.s_log_groups_per_flex, fsc->sb.s_desc_size);
	}
	return;
}

void initGDT(struct inject_c *ic, sector_t off)
{
	//try submit_bio
	
	struct bio *bgt_bio;
	struct page *bgt_bio_page;
	struct ext4_group_desc tmp_blk_group;
	int sz = 0;
	int len=PAGE_SIZE, i;
	struct ext4_context *fsc;
	int number_of_groups;
	unsigned char *bg_page_addr;
	
	fsc = ic->context;
	if(fsc == NULL){
		DMDEBUG("no ic-> context");
		return;
	}
	number_of_groups = fsc->numGroups;
	if(number_of_groups == 0){
		DMDEBUG("no of groups detected in file system %d, EXITING GDT initialization", number_of_groups);
	}
	fsc->gdt = kmalloc(sizeof(struct my_ext4_bg_info) * number_of_groups , GFP_KERNEL | GFP_NOIO);

	if(ic->context == NULL) {
		DMDEBUG("%s():uninitialized injector context",__func__);
		return;
	}

	bgt_bio_page = alloc_page(GFP_NOIO | __GFP_NOFAIL);
	if(bgt_bio_page == NULL){
		DMDEBUG("bgt_bio_page is null");
	}

	bgt_bio = bio_alloc(GFP_KERNEL, 1);

	if(bgt_bio == NULL) {
		DMDEBUG("bgt_bio is null");
	}

	bio_set_dev(bgt_bio, ic->dev->bdev);
	bgt_bio->bi_iter.bi_sector = off;
	bgt_bio->bi_end_io = NULL;
	bgt_bio->bi_private = NULL;

	sz = bio_add_page(bgt_bio, bgt_bio_page, PAGE_SIZE, 0);

	if(sz < PAGE_SIZE) {
		DMDEBUG("%s failed bio_add_page size allocated = %d",__func__, len);
		bio_put(bgt_bio);
		return;
	}

	bio_set_op_attrs(bgt_bio, REQ_OP_READ, REQ_META|REQ_PRIO|REQ_SYNC);
	submit_bio_wait(bgt_bio);
	DMDEBUG("submit_bio done!");

	bg_page_addr = (unsigned char*)page_address(bgt_bio->bi_io_vec->bv_page);
	
	for(i = 0 ; i < number_of_groups ; i++){
		memcpy(&tmp_blk_group, bg_page_addr + (i * sizeof(struct ext4_group_desc)), sizeof(struct ext4_group_desc));
		fsc->gdt[i*2].bg_block_bitmap_bno = tmp_blk_group.bg_block_bitmap_lo;
		fsc->gdt[i*2].bg_inode_bitmap_bno = tmp_blk_group.bg_inode_bitmap_lo;
		fsc->gdt[i*2].bg_inode_table_bno = tmp_blk_group.bg_inode_table_lo;
				
		fsc->gdt[i*2+1].bg_block_bitmap_bno = tmp_blk_group.bg_block_bitmap_hi;
		fsc->gdt[i*2+1].bg_inode_bitmap_bno = tmp_blk_group.bg_inode_bitmap_hi;
		fsc->gdt[i*2+1].bg_inode_table_bno = tmp_blk_group.bg_inode_table_hi;
	}
	return;
}

int ext4_inject_ctr(struct inject_c *ic)
{
	struct ext4_context *fsc;
	int blocks_per_group;

	fsc = kmalloc(sizeof(*fsc), GFP_KERNEL);
	if(!fsc) {
		DMERR("%s failed", __func__);
		return -ENOMEM;
	}

	ic->context = fsc;
	fsc->initSuper = false;
	initSB(ic);

	if(fsc->initSuper == false){
		DMDEBUG("unable to initialize Super, returning");
		return 0;
	}

	if(fsc->numGroups == 0){
		DMDEBUG("unable to read number of Groups, returning");
		return 0;
	}

	blocks_per_group = fsc->sb.s_blocks_per_group;
	initGDT(ic, (blocks_per_group + 1) * 8);

	DMDEBUG("%s", __func__);
	return 0;
}

void ext4_inject_dtr(struct inject_c *ic)
{
	struct ext4_context *fsc;
	if(ic == NULL)
		return;
	fsc = (struct ext4_context *) ic->context;
	if(fsc != NULL) {
		if(fsc->gdt != NULL) {
			kfree(fsc->gdt);
		}
		kfree(fsc);
	}
	DMDEBUG("%s", __func__);
	return;
}

/* wrapper functions for argument comparision for character and string parameters
 functions increment pointer by the number of characters read. */

bool isTypeChar(char **cur_arg, char testType){
        if (strchr(*cur_arg,testType) == *cur_arg){
                (*cur_arg)++;
                return true;
        }
        return false;
}

bool isTypeStr(char **cur_arg, char* testType){
        if(strncmp(*cur_arg,testType, strlen(testType)) == 0) {
                (*cur_arg)+=strlen(testType);
                return true;
        }
        return false;
}

long long unsigned getNumber(char *cur_arg)
{
	long long unsigned number = 0;
	if(sscanf(cur_arg, "%llu%*c",&number) == 1){
		DMDEBUG("only number extracted %llu", number);
	}else {
		DMINFO("Incorrect parameters specified\n");
	}
	return number;
}

// return true if new_type is of any of the following types

bool hasOffset(int new_type)
{
	return (new_type & ( DM_INJECT_EXT4_BLOCK |  DM_INJECT_EXT4_DATA_BITMAP |
		 DM_INJECT_EXT4_INODE_BITMAP | DM_INJECT_EXT4_INDIRECT_DATA_MAP |
		DM_INJECT_EXT4_EXTENT_TABLE) );
}

// return true if new_type is of any of the following types

bool hasField(int new_type)
{
	return (new_type & ( DM_INJECT_EXT4_SB | DM_INJECT_EXT4_INODE 
	| DM_INJECT_EXT4_JOURNAL | DM_INJECT_EXT4_DIRECTORY |
	DM_INJECT_EXT4_EXTENDED_ATTRIBUTES) );
}

bool hasOffsetAndField(int new_type)
{
	return (new_type & DM_INJECT_EXT4_BG);	
}

/*
	EXT4 parameters could be one of the following:
		
         - sbX[field]		superblock
	 - iX[field]		inode
	 - bX[offset]		block
	 - gdX[field]		group descriptor
	 - dbmapX[offset]	data bitmap
	 - ibmapX[offset]	inode bitmap
	 - itableX[offset]	inode table
	 - idmap[offset]	indirect block (probably not required for EXT4, writing here for backward compatibility)
	 - etbX[offset] 	extent table block
	 - **jX[field]		journal block
	 - dirX[field]		directory block 
	 - exattrX[field]	extended attributes

	where offset is a numerical value
	and field is a string sub-field of the structure.
*/

int ext4_parse_args(struct inject_c *ic, struct dm_arg_set *as, char *error)
{
	int i, offset, new_op = 0, new_type = DM_INJECT_EXT4_BLOCK;
	char *cur_arg;
	long long unsigned number;
	char field[256] = {'\0'};
	struct inject_rec *new_block;
	int size = 0;

	if (as->argc > 0) {
		ic->num_corrupt = as->argc;
		ic->corrupt_sector = NULL;
	} else {
		ic->num_corrupt = 0;
		ic->corrupt_sector = NULL;
		ic->corrupt_block = NULL;
	}
	for (i=0;i<ic->num_corrupt;i++) {
		cur_arg = dm_shift_arg(as);
		// R or W denotes only corrupting only READ or WRITE access
		if(isTypeChar(&cur_arg,'R'))
			new_op = REQ_OP_READ;
		else if (isTypeChar(&cur_arg,'W')) 
			new_op = REQ_OP_WRITE;

		// Extract Parameter
		if(isTypeStr(&cur_arg,"sb"))	{
			new_type =  DM_INJECT_EXT4_SB;
		} else if (isTypeStr(&cur_arg,"gd")) {
			new_type = DM_INJECT_EXT4_BG;
		} else if( isTypeStr(&cur_arg,"dbmap")) { 
			new_type = DM_INJECT_EXT4_DATA_BITMAP;
		} else if( isTypeStr(&cur_arg,"ibmap")) {
			new_type = DM_INJECT_EXT4_INODE_BITMAP;
		} /*else if( isTypeStr(&cur_arg,"itable")) { 
			new_type = DM_INJECT_EXT4_INODE_TABLE;
		}*/ else if(isTypeStr(&cur_arg,"idmap")) {
			new_type = DM_INJECT_EXT4_INDIRECT_DATA_MAP;
		} else if(isTypeStr(&cur_arg,"etb")) { 
			new_type = DM_INJECT_EXT4_EXTENT_TABLE;
		} else if(isTypeStr(&cur_arg,"dir")) {
			new_type = DM_INJECT_EXT4_DIRECTORY; 
		} else if(isTypeStr(&cur_arg,"exattr")) {
		 	new_type = DM_INJECT_EXT4_EXTENDED_ATTRIBUTES;
		} else if(isTypeChar(&cur_arg,'j')) { 	
			new_type = DM_INJECT_EXT4_JOURNAL; 
		} else if (isTypeChar(&cur_arg,'b')) { 
			DMINFO("setting up block type corruption\n");
			new_type = DM_INJECT_EXT4_BLOCK;
		} else if(isTypeChar(&cur_arg,'i')) {
			DMINFO("setting up inode type corruption\n");
			new_type =  DM_INJECT_EXT4_INODE;
		} else {
			DMDEBUG("%s():unidentified parameter %s\n",__func__, cur_arg);
		}
	
		// extract number, and either offset or field.
		if(hasOffset(new_type)){
			sscanf(cur_arg, "%llu[%d][%d]%*c",&number, &offset,&size);
		} else if(hasField(new_type)) {
			sscanf(cur_arg, "%llu[%s]%*c",&number, field);
			field[strlen(field) - 1] = '\0';
		} else if(hasOffsetAndField(new_type)) {
			sscanf(cur_arg, "%llu[%d][%s]%*c",&number, &offset,field);
		}

		// parameter extraction complete. initialize injector 

		new_block = kzalloc(sizeof(*new_block), GFP_NOIO);
		new_block->type = new_type;
		new_block->op = new_op;
		new_block->number = number;
		new_block->size = size;
		if(strlen(field) > 2){
			field[strlen(field)]='\0';
			strcpy(new_block->field, field);
		}else{
			new_block->field[0] = '\0';
		}
		new_block->offset = offset;

		DMDEBUG("%s(): parameters => op = %d type = %d number = %llu field = %s offset = %d size = %d\n", __func__, new_op, new_type, number,  field, offset, size);

		list_add_tail(&new_block->list, &ic->inject_list);
	}
	return 0;
}

void corrupt_bio(struct bio *bio, int offset, int size)
{
        unsigned long flags;
        struct bio_vec *bvec;
	unsigned int iter;
        char *data;

        DMINFO("%s() corrupting bio %lu",__func__, (bio->bi_iter.bi_sector / 8) -1 );
	DMINFO("%s(): offset = %d size = %d",__func__, offset, size);

        for_each_bvec_no_advance(iter, bvec, bio, 0) {
		DMDEBUG("length (probably segment length) = %d", bvec->bv_len);
		DMDEBUG("sector = %lu",bio->bi_iter.bi_sector);
                data = bvec_kmap_irq(bvec, &flags);
                memset(data + offset, 66, size);
                flush_dcache_page(bvec->bv_page);
                bvec_kunmap_irq(data, &flags);
        }
}

void corrupt_byte(struct bio *bio, int offset, char c)
{
	unsigned long flags;
        struct bio_vec *bvec;
	unsigned int iter;
        char *data;

        DMDEBUG("%s() corrupting bio %lu",__func__, (bio->bi_iter.bi_sector / 8) -1 );
	DMDEBUG("%s(): offset = %d",__func__, offset);

        for_each_bvec_no_advance(iter, bvec, bio, 0) {
		DMDEBUG("length (probably segment length) = %d", bvec->bv_len);
		DMDEBUG("sector = %lu",bio->bi_iter.bi_sector);
                data = bvec_kmap_irq(bvec, &flags);
                memset(data + offset, c, 1);
                flush_dcache_page(bvec->bv_page);
                bvec_kunmap_irq(data, &flags);
        }
}

bool isFactor(int y, int x)
{
        if(x > y)
                return false;
        if(x == y)
                return true;
        return isFactor(x * x , y);
}

/* 	
	checks if blockNumber contains superBlock.
	blockNumber is a super block if it is block 0, 1,
	first block of all Block Groups if isSparse = False
	first block of all Block Groups that are multiples 
	of 3,5 or 7 if isSparse is set.
*/

bool isSuper( struct ext4_context *fsc, bool isSparse, sector_t blockNumber)
{
	uint32_t blocks_per_group = fsc->sb.s_blocks_per_group;
	sector_t blockGroupNumber;

	if(blockNumber % blocks_per_group)
		return false;
	if(!isSparse){
		return true;
	}else{
		blockGroupNumber = blockNumber / blocks_per_group;
		if(blockGroupNumber  == 1 || isFactor(blockGroupNumber,3) 
		|| isFactor(blockGroupNumber, 5) || isFactor(blockGroupNumber, 7)) {
			DMDEBUG("%s():detected super at block number %lu\n",__func__,blockNumber);
			return true;
		}
	}
	return false;
}

int lookup_offset_ext4_inode(char *field)
{
	if(!strcmp(field,"i_mode")) {
		return offsetof(struct ext4_inode,i_mode);	
	} else if(!strcmp(field,"i_size_lo")) {
		return offsetof(struct ext4_inode,i_size_lo);
	} else if(!strcmp(field,"i_blocks_lo")) {
		return offsetof(struct ext4_inode,i_blocks_lo);
	} else if(!strcmp(field,"i_flags")) {
		return offsetof(struct ext4_inode,i_flags);
	}
	return 0;
}

int lookup_size_ext4_inode(char *field)
{
	if(!strcmp(field,"i_mode")) {
		return sizeof(((struct ext4_inode *)0)->i_mode);
	} else if(!strcmp(field, "i_size_lo")) {
		return sizeof(((struct ext4_inode *)0)->i_size_lo);
	} else if(!strcmp(field, "i_blocks_lo")) {
		return sizeof(((struct ext4_inode *)0)->i_blocks_lo);
	} else if(!strcmp(field, "i_flags")) {
		return sizeof(((struct ext4_inode *)0)->i_flags);
	}
	return 0;
}

int lookup_offset_ext4_group_desc(char *field)
{
	if(!strcmp(field,"bg_block_bitmap_lo")) {
		return offsetof(struct ext4_group_desc,bg_block_bitmap_lo);	
	} else if(!strcmp(field,"bg_inode_bitmap_lo")) {
		return offsetof(struct ext4_group_desc,bg_inode_bitmap_lo);
	} else if(!strcmp(field,"bg_inode_table_lo")) {
		return offsetof(struct ext4_group_desc,bg_inode_table_lo);
	} else if(!strcmp(field,"bg_free_blocks_count_lo")) {
		return offsetof(struct ext4_group_desc,bg_free_blocks_count_lo);
	}
	return 0;
}

int lookup_size_ext4_group_desc(char *field)
{
	if(!strcmp(field,"bg_block_bitmap_lo")) {
		return sizeof(((struct ext4_group_desc *)0)->bg_block_bitmap_lo);
	} else if(!strcmp(field, "bg_inode_table_lo")) {
		return sizeof(((struct ext4_group_desc *)0)->bg_inode_table_lo);
	} else if(!strcmp(field, "bg_inode_table_lo")) {
		return sizeof(((struct ext4_group_desc *)0)->bg_inode_table_lo);
	} else if(!strcmp(field, "bg_free_blocks_count_lo")) {
		return sizeof(((struct ext4_group_desc *)0)->bg_free_blocks_count_lo);
	}
	return 0;
}

// TODO add more fields here

int lookup_offset_ext4_super_block(char *field){
	if(!strcmp(field,"s_inodes_count")) {
		return offsetof(struct ext4_super_block,s_inodes_count);
	} else if(!strcmp(field, "s_blocks_count")) {
		return offsetof(struct ext4_super_block,s_blocks_count_lo);
	} else if(!strcmp(field, "s_free_inodes_count")) {
		return offsetof(struct ext4_super_block,s_free_inodes_count);
	}
	return 0;
}

int lookup_size_ext4_super_block(char *field){
	if(!strcmp(field,"s_inodes_count")) {
		return sizeof(((struct ext4_super_block *)0)->s_inodes_count);
	} else if(!strcmp(field, "s_blocks_count")) {
		return sizeof(((struct ext4_super_block *)0)->s_blocks_count_lo);
	} else if(!strcmp(field, "s_free_inodes_count")) {
		return sizeof(((struct ext4_super_block *)0)->s_free_inodes_count);
	}
	return 0;
}


// both super block number and block group are indexed from 1.

long long unsigned getSBNumber(long long unsigned blockNumber, bool isSparse, uint32_t blocks_per_group )
{
	int sbNo, bg, i;
	if(blockNumber % blocks_per_group != 0)
		return 0;
	if(isSparse == false){
		return (blockNumber / blocks_per_group) + 1;
	}else{
		bg = (blockNumber / blocks_per_group) + 1;	
		sbNo = 0;
		for (i = 0 ; i < bg; i++){
			if(i == 0 || i == 1 || isFactor(i,3) || isFactor(i,5) || isFactor(i,7))
				sbNo++;
		}
		return sbNo;
	}
}

void processSB(struct ext4_context *fsc, sector_t bnum, char *page_addr, struct inject_rec *ext4_ic, struct bio *bio)
{
	bool isSparse;
	long unsigned int number = 0;
	int offset,size;
	char field[256] = {'\0'};

	isSparse = fsc->sb.s_feature_ro_compat & EXT4_FEATURE_RO_COMPAT_SPARSE_SUPER;
	DMDEBUG("%s(): corrupting SB", __func__);
	
	if(fsc->initSuper == false){ // wait to fill sb with block 0
		DMINFO("superblock uninitialized in context. please start dminject");
		DMINFO("sudo dmsetup message dmtarget 0 start");
		return;
	}

	// extract arguments
	if(ext4_ic == NULL)
		return;

	number = ext4_ic->number;
	if(ext4_ic->field != NULL){
		DMDEBUG("ext4_ic->field is not null");
		memcpy(field, ext4_ic->field, strlen(ext4_ic->field));
	}else{
		DMDEBUG("ext4_ic->field is null");
	}
	offset = lookup_offset_ext4_super_block(field);
	size = lookup_size_ext4_super_block(field);

	// sb1[s_free_inodes_count]
	DMDEBUG("%s():XXXX number = %lu, field = %s, offset = %d\n",__func__, number, field, offset);	
	if(number == getSBNumber(bnum,isSparse, fsc->sb.s_blocks_per_group)) {
		DMINFO("%s():corrupting block %lu which is the %lu'th block\n",__func__,bnum, number);
		// use offset + 1024 only for block 0.
		if(number == 1)
			corrupt_bio(bio, offset + 1024, size);
		else
			corrupt_bio(bio, offset , size);
	}
}

/* this is the only function which is called only when the bnum matches the inject->num
 value. for all other functions, the derivation of actual block to corrupt from the 
 inject-> num value takes place within the process* function.
*/
void processBlock(struct ext4_context *fsc, sector_t bnum, char *page_addr, struct inject_rec *ext4_ic, struct bio *bio) {

	long long unsigned number;
	int offset, size,block_size;
	if(ext4_ic == NULL)
		return;
	// no need to copy ext4_ic->field.

	number = ext4_ic->number;
	offset = ext4_ic->offset;
	size = ext4_ic->size;
	block_size = fsc->sb.s_log_block_size;
	// if size is not specified for block corruption, corrupt everything in block after the offset.
	size = (size == 0) ? block_size - offset : size ;
	DMDEBUG("%s():size for corruption %d",  __func__,size);
	if(size > 4096){
		DMDEBUG("%s():block corruption size %d greater than block size",__func__,size);
		DMDEBUG("%s():readjusting corruption size to %d",__func__,block_size - offset);
		size = block_size - offset;
	}
	corrupt_bio(bio, offset, size);
}

void processGD(struct ext4_context *fsc, sector_t bnum, char *page_addr, struct inject_rec *ext4_ic, struct bio *bio) {
	sector_t number;
	int group_descriptor_offset, number_of_groups;
	int size_of_field, offset_of_field, ibnum;
	char field[256];
	bool isSparse;
	// store values in integer as mod on long unsigned is not supported in Kernel
	int ibcount;
	
	if(ext4_ic == NULL || fsc == NULL){
		return;
	}
	
	// sanity check for large long unsigned bnum values from block_inject-base code
	// also, bnum = 0 is superblock
	if( bnum ==0 || bnum > fsc->sb.s_blocks_count_lo)
		return;
	
	ibcount = fsc->sb.s_blocks_count_lo;
	ibnum = bnum;

	if(((ibnum - 1) % ibcount) != 0)
		return;


	number_of_groups = fsc->numGroups;
	number = ext4_ic->number;
	group_descriptor_offset = ext4_ic->offset;
	strcpy(field,ext4_ic->field);

 	isSparse = fsc->sb.s_feature_ro_compat & EXT4_FEATURE_RO_COMPAT_SPARSE_SUPER;

	if(number == getSBNumber(ibnum - 1,isSparse, fsc->sb.s_blocks_per_group)){
		// got the right Group Descriptor Table.
		DMDEBUG("%s(): SB Number = %lu bnum = %d number of groups = %d, field = %s, num of blocks = %d",__func__, number, ibnum, number_of_groups, field, fsc->sb.s_blocks_count_lo);
		if(group_descriptor_offset == 0){ // corrupt all group descriptors
			DMDEBUG("group descriptor 0 given, corrupting entire group descriptor");
			corrupt_bio(bio,0, number_of_groups * sizeof(struct ext4_group_desc));
		} else {
			DMDEBUG("corrupting gd at offset %d", group_descriptor_offset);
			if(strlen(field) == 0){	// corrupt entire field
				DMDEBUG("no field specified, corrupting entire group descriptor no %d",group_descriptor_offset);
				corrupt_bio(bio,(group_descriptor_offset -1) * sizeof(struct ext4_group_desc), sizeof(struct ext4_group_desc));
			}else{	// corrupt only specific field
				DMDEBUG("field %s specified. corrupting number %lu offset %d field %s", field, number, group_descriptor_offset, field);
				size_of_field = lookup_size_ext4_group_desc(field);
				offset_of_field = lookup_offset_ext4_group_desc(field);
				corrupt_bio(bio,((group_descriptor_offset -1) * sizeof(struct ext4_group_desc)) + offset_of_field,size_of_field);
			}
		}
	}
}

void processDBMap(struct ext4_context *fsc, sector_t bnum, char *page_addr, struct inject_rec *ext4_ic, struct bio *bio, struct inject_c *ic) {
	sector_t number;
	int number_of_groups, offset;
	char c;
	int i;
	
	if(fsc == NULL || ext4_ic == NULL)
		return;

	number = ext4_ic->number;
	number_of_groups = fsc->numGroups;
	
	offset = ext4_ic->offset;
	if(offset > 255) {
		DMDEBUG("%s():only offsets smaller than 255 supported. given offset %d",__func__, offset);
	}
		

	if(fsc->initSuper && (number > number_of_groups)){
		DMWARN("cannot fuzz DBMap %lu, please specify number less than %d, the total no of groups in the file system", number, number_of_groups);
	}

	// if bno == fsc-> gdt[number].bg_block_bitmap_bno
	if(bnum == fsc->gdt[number-1].bg_block_bitmap_bno) {
		DMDEBUG("Block Number of %lu data bitmap = %d bno = %lu", number, fsc->gdt[number-1].bg_block_bitmap_bno, bnum);
		DMDEBUG("block match. get offset");
		// copy from bio into a char value
		for(i = 0 ; i < 256 ; i++){
			memcpy(	&c, bio + i , 1);
			if ( i == offset) {
				DMDEBUG("found offset char %hhu", c);
				c = ~c;
				DMDEBUG("reset bitmap value to %hhu", c);
				corrupt_byte(bio, offset, c);		
			}
		}
	}
}

void processIBMap(struct ext4_context *fsc, sector_t bnum, char *page_addr, struct inject_rec *ext4_ic, struct bio *bio, struct inject_c *ic) {
	sector_t number;
	int number_of_groups, offset;
	char c;
	int i;
	
	if(fsc == NULL || ext4_ic == NULL)
		return;

	number = ext4_ic->number;
	number_of_groups = fsc->numGroups;
	
	offset = ext4_ic->offset;
	if(offset > 255) {
		DMDEBUG("%s():only offsets smaller than 255 supported. given offset %d",__func__, offset);
	}
		

	if(fsc->initSuper && (number > number_of_groups)){
		DMWARN("cannot fuzz DBMap %lu, please specify number less than %d, the total no of groups in the file system", number, number_of_groups);
	}

	if(bnum == fsc->gdt[number-1].bg_inode_bitmap_bno) {
		DMDEBUG("Block Number of %lu inode bitmap = %d bno = %lu", number, fsc->gdt[number-1].bg_inode_bitmap_bno, bnum);
		DMDEBUG("block match. get offset");
		// copy from bio into a char value
		for(i = 0 ; i < 256 ; i++){
			memcpy(	&c, bio + i , 1);
			if ( i == offset) {
				DMDEBUG("found offset char %hhu", c);
				c = ~c;
				DMDEBUG("reset bitmap value to %hhu", c);
				corrupt_byte(bio, offset, c);		
			}
		}
	}
}

int get_block_size(int log_size)
{
	int i;
	int res = 1024;

	for(i = 0 ; i < log_size; i++)
		res *=2;
	return res;
}

void processInode(struct ext4_context *fsc, sector_t bnum, char *page_addr, 
	struct inject_rec *ext4_ic, struct bio *bio) {

	int number_of_groups, number_of_inodes, inode_table_start;
	int  inodes_per_group, inumber, block_group, offset;
	sector_t boffset, number;
	int block_size;
	char field[256];
	struct ext4_inode my_inode;
	int size_of_field, offset_of_field;

	DMINFO("%s(): here\n",__func__);

	if(fsc->gdt == NULL) {
		DMDEBUG("%s(): Group Descriptor Fields Uninitialized,\
			returning", __func__);
		return;
	}

	number = ext4_ic->number;
	strcpy(field,ext4_ic->field);
	if(number == 0) {
		DMDEBUG("%s(): Inode number with 0 does not exist,\
			returning", __func__);
		return;
	}

	inumber = number;
	inodes_per_group = fsc->sb.s_inodes_per_group;
	block_group = number / inodes_per_group;
	number_of_groups = fsc->numGroups;
	number_of_inodes = fsc->sb.s_inodes_count;

	if(block_group > number_of_groups) {
		DMDEBUG("%s(): trying to fuzz inode number %lu,\
		  greater than max inodes %d",__func__, number, 
				inodes_per_group * number_of_groups);
		return;
	}
	inode_table_start = fsc->gdt[block_group].bg_inode_table_bno;
	block_size = get_block_size(fsc->sb.s_log_block_size);
	
	boffset = (inode_table_start * block_size) + 
		(((inumber -1) % inodes_per_group) * sizeof(struct ext4_inode));

	if(boffset / block_size == bnum) {
		offset = boffset % block_size;
		memcpy(&my_inode, bio + offset, sizeof(struct ext4_inode));
		if(strlen(field) == 0) {
			corrupt_bio(bio, offset, sizeof(struct ext4_inode));	
		} else {
			size_of_field = lookup_size_ext4_inode(field);
			offset_of_field = lookup_offset_ext4_inode(field);
			corrupt_bio(bio, offset + offset_of_field, size_of_field);
		}
	}
	return;
}

bool ext4_block_from_dev(struct inject_c *ic, struct bio *bio, struct bio_vec *bvec, sector_t sec)
{	
	sector_t bnum = sec / 8;
	struct ext4_context *fsc;
	unsigned char *my_page_addr;
	struct inject_rec *tmp;

	fsc = ic->context;
	my_page_addr = (unsigned char*)page_address(bio->bi_io_vec->bv_page);

	list_for_each_entry(tmp, &ic->inject_list, list) {
		//if(tmp->op == REQ_OP_WRITE)
		//	continue;
		if(tmp->type == DM_INJECT_EXT4_SB) {
			processSB(fsc,bnum,my_page_addr,tmp, bio);
		}
		if(tmp->type == DM_INJECT_EXT4_BLOCK && (tmp->number == bnum)) {
			processBlock(fsc,bnum,my_page_addr,tmp,bio);
		}
		if(tmp->type == DM_INJECT_EXT4_BG) {
			processGD(fsc,bnum,my_page_addr,tmp,bio);
		}
		if(tmp->type == DM_INJECT_EXT4_INODE) {
			processInode(fsc,bnum,my_page_addr,tmp,bio);
		}
		if(tmp->type == DM_INJECT_EXT4_DATA_BITMAP) {
			processDBMap(fsc,bnum,my_page_addr,tmp,bio,ic);
		}
		if(tmp->type == DM_INJECT_EXT4_INODE_BITMAP) {
			processIBMap(fsc,bnum,my_page_addr,tmp,bio,ic);	
		}
	}
	return false;
}

bool ext4_block_to_dev(struct inject_c *ic, struct bio *bio, struct bio_vec *bvec, sector_t sec)
{
//	DMDEBUG("%s %s sec %lu", __func__, RW(bio_op(bio)), sec);
	return false;
}

int ext4_data_from_dev(struct inject_c *ic, struct bio *bio, struct bio_vec *bvec, sector_t sec)
{
//	DMDEBUG("%s %s sec %lu", __func__, RW(bio_op(bio)), sec);
	return DM_INJECT_NONE;
}

int ext4_data_to_dev(struct inject_c *ic, struct bio *bio, struct bio_vec *bvec, sector_t sec)
{
//	DMDEBUG("%s %s sec %lu", __func__, RW(bio_op(bio)), sec);
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
