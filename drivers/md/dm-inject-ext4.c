/*
 * dm-inject-ext4 device mapper target
 * template for ext4
 */

#include "dm-inject.h"
#include "../../fs/ext4/ext4.h"
#include "../../fs/ext4/ext4_extents.h"
#include "../../include/linux/completion.h"

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
	struct ext4_inode root_inode;
	bool initSuper;
	int dummy;
	int numGroups;
};

bool getExtentBlocks(struct ext4_inode *inode, struct ext4_context *fsc, struct inject_c *ic, struct inject_rec *ext4_rec, char *path, char *field);

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
		bio_put(bgt_bio);
		return;
	}else {
		DMDEBUG("bg_page_addr is not null");
		memcpy(&(fsc->sb),bg_page_addr + 1024,sizeof(struct ext4_super_block));
		blocks_count = fsc->sb.s_blocks_count_lo; // total number of blocks
		blocks_per_group = fsc->sb.s_blocks_per_group;	// total number of blocks per group
		fsc->numGroups = (blocks_count + blocks_per_group - 1) / blocks_per_group;
		fsc->initSuper = true;
		DMDEBUG("num of groups = %d fsc->s_log_groups_per_flex = %d fsc->s_desc_size = %d", fsc->numGroups, fsc->sb.s_log_groups_per_flex, fsc->sb.s_desc_size);
		DMDEBUG("%s():s_inode_size = %d",__func__, fsc->sb.s_inode_size);
	}
	bio_put(bgt_bio);
	return;
}

int initRootInode(struct inject_c *ic)
{
	struct bio *rinode_bio;
	struct page *rinode_bio_page;
	int sz = 0;
	int len=PAGE_SIZE;
	struct ext4_context *fsc;
	unsigned char *rinode_page_addr;

	if(ic->context == NULL) {
		DMDEBUG("%s():uninitialized injector context",__func__);
		return -1;
	}

	fsc = ic->context;

	if(fsc->gdt == NULL) {
		DMDEBUG("%s():group descriptor table uninitialized, cannot read root block", __func__);
		return -1;
	}

	rinode_bio_page = alloc_page(GFP_NOIO | __GFP_NOFAIL);
	if(rinode_bio_page == NULL){
		DMDEBUG("bgt_bio_page is null");
		return -ENOMEM;
	}

	rinode_bio = bio_alloc(GFP_KERNEL, 1);

	if(rinode_bio == NULL) {
		DMDEBUG("bgt_bio is null");
		return -ENOMEM;
	}

	if(ic->dev == NULL){
		DMDEBUG("ic device not set, returning");
		return -1;
	}

	if(ic->dev->bdev == NULL) {
		DMDEBUG("ic device bdev not set, returning");
		return -1;
	}

	bio_set_dev(rinode_bio, ic->dev->bdev);

	DMDEBUG("reading from bg_inode_table_bno = %d", fsc->gdt[0].bg_inode_table_bno);
	rinode_bio->bi_iter.bi_sector = fsc->gdt[0].bg_inode_table_bno * 8;
	rinode_bio->bi_end_io = NULL;
	rinode_bio->bi_private = NULL;

	sz = bio_add_page(rinode_bio, rinode_bio_page, PAGE_SIZE, 0);

	if(sz < PAGE_SIZE) {
		DMDEBUG("%s failed bio_add_page size allocated = %d",__func__, len);
		return -ENOMEM;
	}

	bio_set_op_attrs(rinode_bio, REQ_OP_READ, REQ_META|REQ_PRIO|REQ_SYNC);
	submit_bio_wait(rinode_bio);
	DMDEBUG("%s():submit_rinode_bio done!",__func__);

	rinode_page_addr = (unsigned char*)page_address(rinode_bio->bi_io_vec->bv_page);
	if(rinode_page_addr == NULL){
		DMDEBUG("bg_page_addr assigned null");
		//bio_put(bgt_bio);
		return -ENOMEM;
	}else {
		DMDEBUG("bg_page_addr is not null");
		    	if(rinode_page_addr == NULL) {
	    			DMDEBUG("%s(): rinode_page_addr is null",__func__);
	    			return -1;
	    		}
		    	if(rinode_page_addr + fsc->sb.s_inode_size == NULL) {
	    			DMDEBUG("%s(): rinode page addr + size is null",__func__);
	    			return -1;
		    	} 
			// copy 2nd Inode which is the root inode into filesystem
			// context
	    		memcpy(&fsc->root_inode, rinode_page_addr + fsc->sb.s_inode_size, sizeof(struct ext4_inode));
	}
	bio_put(rinode_bio);
	return 0;
}

int initGDT(struct inject_c *ic, sector_t off)
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
		return -ENOMEM;
	}
	number_of_groups = fsc->numGroups;
	if(number_of_groups == 0){
		DMDEBUG("no of groups detected in file system %d, EXITING GDT initialization", number_of_groups);
		return -1;
	}
	fsc->gdt = kmalloc(sizeof(struct my_ext4_bg_info) * number_of_groups , GFP_KERNEL | GFP_NOIO);

	if(ic->context == NULL) {
		DMDEBUG("%s():uninitialized injector context",__func__);
		return -1;
	}

	bgt_bio_page = alloc_page(GFP_NOIO | __GFP_NOFAIL);
	if(bgt_bio_page == NULL){
		DMDEBUG("bgt_bio_page is null");
		return -ENOMEM;
	}

	bgt_bio = bio_alloc(GFP_KERNEL, 1);

	if(bgt_bio == NULL) {
		DMDEBUG("bgt_bio is null");
		return -ENOMEM;
	}

	bio_set_dev(bgt_bio, ic->dev->bdev);
	bgt_bio->bi_iter.bi_sector = off;
	bgt_bio->bi_end_io = NULL;
	bgt_bio->bi_private = NULL;

	sz = bio_add_page(bgt_bio, bgt_bio_page, PAGE_SIZE, 0);

	if(sz < PAGE_SIZE) {
		DMDEBUG("%s failed bio_add_page size allocated = %d",__func__, len);
		bio_put(bgt_bio);
		return -1;
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
	bio_put(bgt_bio);
	return 0;
}

int lookup_offset_ext4_inode(char *field)
{
	if(!strcmp(field,"i_mode")) {
		return offsetof(struct ext4_inode,i_mode);
	} else if(!strcmp(field,"i_block")) {
		return offsetof(struct ext4_inode,i_block);
	} else if(!strcmp(field,"i_size_lo")) {
		return offsetof(struct ext4_inode,i_size_lo);
	} else if(!strcmp(field,"i_blocks_lo")) {
		return offsetof(struct ext4_inode,i_blocks_lo);
	} else if(!strcmp(field,"i_flags")) {
		return offsetof(struct ext4_inode,i_flags);
	}
	return 0;
}

int lookup_offset_ext4_dir_entry_2(char *field)
{
	if(!strcmp(field,"inode")) {
		return offsetof(struct ext4_dir_entry_2,inode);	
	} else if(!strcmp(field,"rec_len")) {
		return offsetof(struct ext4_dir_entry_2,rec_len);
	} else if(!strcmp(field,"name_len")) {
		return offsetof(struct ext4_dir_entry_2,name_len);
	} else if(!strcmp(field,"file_type")) {
		return offsetof(struct ext4_dir_entry_2,file_type);
	}  else if(!strcmp(field,"name")) {
		return offsetof(struct ext4_dir_entry_2,name);
	}
	DMDEBUG("COULD NOT FIND ENTRY FOR %s", field); 
	return 0;
}

int lookup_size_ext4_inode(char *field)
{
	if(!strcmp(field,"i_mode")) {
		return sizeof(((struct ext4_inode *)0)->i_mode);
	} else if(!strcmp(field, "i_size_lo")) {
		return sizeof(((struct ext4_inode *)0)->i_size_lo);
	} else if(!strcmp(field, "i_block")) {
		return sizeof(((struct ext4_inode *)0)->i_block);
	} else if(!strcmp(field, "i_blocks_lo")) {
		return sizeof(((struct ext4_inode *)0)->i_blocks_lo);
	} else if(!strcmp(field, "i_flags")) {
		return sizeof(((struct ext4_inode *)0)->i_flags);
	}
	return 0;
}

/* returns pointer to an inode structure
*/

struct ext4_inode *read_inode(sector_t inode_number, struct inject_c *ic, struct ext4_context *fsc)
{
	struct bio *inode_bio;
	struct page *inode_bio_page;
	int sz = 0;
	int len=PAGE_SIZE;
	unsigned char *inode_page_addr;
	struct ext4_inode *inode = NULL;

	int number_of_inodes_per_group;
	int groupNo;
	int number_of_inodes_per_block;
	int size_of_inode;

	int inode_number_in_group;
	int inode_number_in_block;
	int block_number_in_group;
	int absolute_block_number;
	

	// freed by the caller function that receives inode address
	inode = kmalloc(sizeof(struct ext4_inode) , GFP_KERNEL);
	if(inode == NULL) {
		DMDEBUG("%s():could not allocate memory", __func__);
		return NULL;
	}

	if(ic->context == NULL) {
		DMDEBUG("%s():uninitialized injector context",__func__);
		return NULL;
	}

	if(fsc->gdt == NULL) {
		DMDEBUG("%s():group descriptor table uninitialized, cannot read root block", __func__);
		return NULL;
	}

	inode_bio_page = alloc_page(GFP_NOIO | __GFP_NOFAIL);
	if(inode_bio_page == NULL){
		DMDEBUG("bgt_bio_page is null");
		return NULL;
	}

	if(ic->dev == NULL){
		DMDEBUG("ic device not set, returning");
		return NULL;
	}

	if(ic->dev->bdev == NULL) {
		DMDEBUG("ic device bdev not set, returning");
		return NULL;
	}

	inode_bio = bio_alloc(GFP_KERNEL, 1);

	if(inode_bio == NULL) {
		DMDEBUG("bgt_bio is null");
		return NULL;
	}

	bio_set_dev(inode_bio, ic->dev->bdev);

	// groupNo = inodeNo / number_of_inodes_per_group
	number_of_inodes_per_group = fsc->sb.s_inodes_per_group;
	if(number_of_inodes_per_group == 0){
		DMDEBUG("%s():number_of_inodes_per_group = 0, returning", __func__);
		return NULL;
	}
		
	groupNo = inode_number / number_of_inodes_per_group;
	if(fsc->sb.s_inode_size == 0){
		DMDEBUG("%s():fsc->sb.s_inode_size = 0, returning", __func__);
		return NULL;
	}
	number_of_inodes_per_block = 4096 / fsc->sb.s_inode_size;
	size_of_inode = fsc->sb.s_inode_size;

	if(number_of_inodes_per_block == 0 || number_of_inodes_per_group == 0) {
		DMDEBUG("number_of_inodes_per_group = %d, number_of_inodes_per_block = %d, returning", number_of_inodes_per_group, number_of_inodes_per_block);
		return NULL;
	}
	inode_number_in_group = (inode_number -1) % number_of_inodes_per_group;
	inode_number_in_block = (inode_number -1) % number_of_inodes_per_block;
	block_number_in_group = inode_number_in_group / number_of_inodes_per_block;
	absolute_block_number = fsc->gdt[groupNo].bg_inode_table_bno + block_number_in_group;

	DMDEBUG("%s():bno of bg_inode_table %d inode_number %lu",__func__, fsc->gdt[groupNo].bg_inode_table_bno, inode_number);

	inode_bio->bi_iter.bi_sector = absolute_block_number * 8;
	inode_bio->bi_end_io = NULL;
	inode_bio->bi_private = NULL;

	DMDEBUG("%s():inode_number_in_group %d inode_number_in_block %d block_number_in_group %d absolute_block_number %d",__func__, inode_number_in_group, inode_number_in_block, block_number_in_group, absolute_block_number);
	
	sz = bio_add_page(inode_bio, inode_bio_page, PAGE_SIZE, 0);

	if(sz < PAGE_SIZE) {
		DMDEBUG("%s failed bio_add_page size allocated = %d",__func__, len);
		return NULL;
	}

	bio_set_op_attrs(inode_bio, REQ_OP_READ, REQ_META|REQ_PRIO|REQ_SYNC);
	submit_bio_wait(inode_bio);
	DMDEBUG("%s():submit_inode_bio done!",__func__);

	inode_page_addr = (unsigned char*)page_address(inode_bio->bi_io_vec->bv_page);
	if(inode_page_addr == NULL){
		DMDEBUG("bg_page_addr assigned null");
		//bio_put(bgt_bio);
		return NULL;
	}else {
		DMDEBUG("bg_page_addr is not null");
		memcpy(inode,inode_page_addr + (inode_number_in_block * size_of_inode), sizeof(*inode));
		DMDEBUG("%s():received inode with size = %d, link count = %d\n",__func__,inode->i_size_lo, inode->i_links_count);
	}
	bio_put(inode_bio);
	return inode;
}

bool processDirectoryBlocks(struct ext4_extent *ext_external, 
	struct ext4_context *fsc, struct inject_c *ic,
	struct inject_rec *ext4_rec, char *dirPath, char *field)
{
	// for all blocks starting from ext_external.ee_start_lo 
	// to ext_external.ee_len,read different directoryBlocks

	struct bio *dir_bio;
	struct page *dir_bio_page;
	int sz;
	unsigned char *dir_page_addr;
	struct ext4_dir_entry_2 dir;
	struct ext4_inode *next_inode_ptr = NULL;
	int rec_len;
	char *currentDirectory = NULL; // to be processed
	char delem[2] = "/";
	bool over = false;
	int field_offset = 0;
	int field_size = 0;

	sector_t dir_bstart;
	sector_t dir_bcount;

	dir_bcount = ext_external->ee_len;
	dir_bstart = ext_external->ee_start_lo;

	if(ic->dev == NULL){
		DMDEBUG("ic device not set, returning");
		return true;
	}

	if(ic->dev->bdev == NULL) {
		DMDEBUG("ic device bdev not set, returning");
	}

	dir_bio_page = alloc_page(GFP_NOIO | __GFP_NOFAIL);
	if(dir_bio_page == NULL){
		DMDEBUG("bgt_bio_page is null");
	}

	dir_bio = bio_alloc(GFP_KERNEL, dir_bcount);

	if(dir_bio == NULL) {
		DMDEBUG("bgt_bio is null");
	}
	
	bio_set_dev(dir_bio, ic->dev->bdev);
	dir_bio->bi_iter.bi_sector = dir_bstart * 8;
	dir_bio->bi_end_io = NULL;
	dir_bio->bi_private = NULL;

	sz = bio_add_page(dir_bio, dir_bio_page, PAGE_SIZE * dir_bcount, 0);

	if(sz < PAGE_SIZE) {
		DMDEBUG("%s failed bio_add_page size allocated = %d",__func__, sz);
		bio_put(dir_bio);
		return true;
	}

	bio_set_op_attrs(dir_bio, REQ_OP_READ, REQ_META|REQ_PRIO|REQ_SYNC);
	submit_bio_wait(dir_bio);
	DMDEBUG("%s():submit_bio done!",__func__);

	if(field != NULL || strlen(field) > 0){
		field_offset = lookup_offset_ext4_dir_entry_2(field);
		field_size = lookup_offset_ext4_dir_entry_2(field);
		DMDEBUG("%s(): field = %s field offset = %d field size = %d",__func__, field, field_offset, field_size);
	//	field_offset = 0;
	//	field_size = 0;
	}

	dir_page_addr = (unsigned char*)page_address(dir_bio->bi_io_vec->bv_page);
	if(dir_page_addr == NULL){
		DMDEBUG("bg_page_addr assigned null");
		bio_put(dir_bio);
		return true;
	}else {
		DMDEBUG("bg_page_addr is not null");
		rec_len = 0;
		while (rec_len != dir_bcount * 4096) {
			memcpy(&dir,dir_page_addr + rec_len ,sizeof(dir));
//			DMDEBUG("inode = %d rec_len = %d name_len = %d file_type = %d name = %s", dir.inode, dir.rec_len, dir.name_len, dir.file_type, dir.name);

			// check if dirPath has more path to traverse.
			// if yes, get inode structure corresponding to dir.inode
			// and call getExtentBlocks with that inode number.

			dir.name[dir.name_len] = '\0';
			if(!strncmp(dirPath, dir.name, strlen(dir.name)))
			{
				// FIXME shouldnt be comparing with 2.
				if(dir.file_type == 2) {
					currentDirectory = strsep(&dirPath,delem);
					DMDEBUG("%s():Directory Found %s", __func__, currentDirectory);
					if(dirPath == NULL) {
						DMDEBUG("%s(): found directory dir_bstart = %lu, rec_len = %d, dir_len = %d", __func__, dir_bstart, rec_len, dir.rec_len);
						// get the block number.
						// get the offset.
						// get the size.
						// store the 3 values inside ext4_rec.
						ext4_rec->block_num = dir_bstart + (rec_len / 4096);
						ext4_rec->offset = ((rec_len + field_offset) % 4096);
						if(field_size == 0) {
							ext4_rec->size = dir.rec_len;
						} else {
							ext4_rec->size = field_size;
						}
						over = true;
						DMDEBUG("%s():SEARCH COMPLETE %s", __func__, currentDirectory);
						DMDEBUG("%s():set block_num %d offset %d size %d\n", __func__,ext4_rec->block_num, ext4_rec->offset, ext4_rec->size);
				
					}else {
						DMDEBUG("%s():remaining path = %s",__func__, dirPath);
						// fetch the inode structure using the inode number.
						DMDEBUG("%s():reading inode number = %d",__func__, dir.inode);
						next_inode_ptr = read_inode(dir.inode,ic, fsc);

						if(next_inode_ptr == NULL) {
							DMDEBUG("Detected Inode 0, returning");
							return true;
						}else {
						// fsc, ic remain as is. 
							// call recursively
							DMDEBUG("%s():received inode with size = %d, link count = %d\n",__func__,next_inode_ptr->i_size_lo, next_inode_ptr->i_links_count);
							DMDEBUG("%s(): calling getExtentBlocks()",__func__);

							over = getExtentBlocks(next_inode_ptr, fsc, ic, ext4_rec, dirPath, field);
							if(next_inode_ptr == NULL){
								kfree(next_inode_ptr);
								next_inode_ptr = NULL;
							}
							return over;
						}
					}
				}else {
					DMDEBUG("%s()File Match but not Directory %s", __func__, currentDirectory);
				}
				bio_put(dir_bio);
				return over;
			}else {
				DMDEBUG("%s",dir.name);
			}
			rec_len += dir.rec_len;
		}
		DMDEBUG("%s():could not find a match for %s",__func__, currentDirectory);
		// if no path was found, return False.
		bio_put(dir_bio);
		return over;
	}
}

/* given an inode, find all blocks pointed to by the extent
*/
bool getExtentBlocks(struct ext4_inode *inode, struct ext4_context *fsc, struct inject_c *ic, struct inject_rec *ext4_rec, char *path, char *field)
{
	struct ext4_inode root_inode;
	struct ext4_extent_header eh;
	struct ext4_extent *ext_external;
	int i;
	// over determines weather the search should terminate or not
	// true = search should terminate.
	// false = continue search.
	bool over = false;

	if(inode == NULL){
		DMDEBUG("%s():inode is null, returning\n",__func__);
		return true;
	}

	// copy inode
	memcpy(&root_inode, inode, sizeof(root_inode));

	// copy extent header
	memcpy(&eh, &root_inode.i_block, sizeof(eh));

	ext_external = kmalloc(sizeof(struct ext4_extent), GFP_KERNEL);
	
	//DMDEBUG("%s(): eh magic %d, eh_entries %d, eh_max %d eh_depth %d eh_generation %d\n",__func__, eh.eh_magic, eh.eh_entries, eh.eh_max, eh.eh_depth, eh.eh_generation);

	// every extent node (60 bytes) starts with ex_header.
	// if eh_depth = 0, subsiquent extents point to leaf nodes.
	// in this case, read all other entries (1 to eh_max) 
	// in the leaf extent struct i.e. ext4_extent

//	DMDEBUG("PRINTING POINTERS");
//	DMDEBUG("%p , %p , %p , %p", &root_inode.i_block[3], &root_inode.i_block[6], &root_inode.i_block[9], &root_inode.i_block[12]);

	if(eh.eh_depth == 0) {
		for(i = 1 ; i <= eh.eh_entries ; i++) {
//			DMDEBUG("%s():PTR %p",__func__, (void *)&root_inode.i_block[i*3]);
			memcpy(ext_external, &root_inode.i_block[i * 3] ,sizeof(struct ext4_extent));
		//	DMDEBUG("external first logical block %d length %d physical block hi %d physical block low %d", ext_external.ee_block, ext_external.ee_len, ext_external.ee_start_hi, ext_external.ee_start_lo);
			DMDEBUG("%s(): calling processDirectoryBlocks ",__func__);
			over = processDirectoryBlocks(ext_external, fsc, ic, ext4_rec , path, field);
			if(over) {
				if(ext_external != NULL) {
					kfree(ext_external);
					ext_external = NULL;
				}
				return over;
			}
		}
	}
	if(ext_external != NULL) {
		kfree(ext_external);
		ext_external = NULL;
	}
	return over;

	// TODO
	// if eh_depth > 0, subsiquent extents point to non leaf nodes.
	// read all entries in ext4_extent_idx
}

/* given a directory path and a field, the function stores the on-disk block
number and offset that should be corrupted for a given directory path and stores
it in the inject_rec structure fields (bnum, offset)*/

void getDirectoryLocation(struct inject_c *ic, struct inject_rec *ext4_rec, struct ext4_context *fsc)
{
	char *field = NULL, *path = NULL;
	bool over;

	if(fsc == NULL) {
		DMDEBUG("fsc null, returning");
		return;
	}
	if(ext4_rec == NULL) {
		DMDEBUG("ext4_rec null, returning");
	}
//	DMDEBUG("path to fuzz = %s, field to fuzz %s",ext4_rec->path, ext4_rec->field);
//	return;

	if(ext4_rec->path != NULL) {
		path = kmalloc( sizeof(char) * (strlen(ext4_rec->path) + 1), GFP_KERNEL);
		// ignore first "/" 
		memcpy(path, ext4_rec->path + 1, strlen(ext4_rec->path)); 
	}
	if(ext4_rec->field != NULL) {
		field = kmalloc( sizeof(char) * (strlen(ext4_rec->field) + 1), GFP_KERNEL);
		memcpy(field, ext4_rec->field, strlen(ext4_rec->field) + 1);
//		field[strlen(ext4_rec->field)] = '\0';
	}

	over = getExtentBlocks(&fsc->root_inode, fsc, ic, ext4_rec , path, field);
	if(over) {
		DMDEBUG("over");
	}else {
		DMDEBUG("not over");
	}
	if(field != NULL) {
		kfree(field);
		field = NULL;
	}
	if(path != NULL) {
		kfree(path);
		path = NULL;
	}
}

/* User requested corrupting a data structure that does not have a fixed
on disk location. Hence, we derive the on disk location on our own.
*/
void getOnDiskLocation(struct inject_c *ic, struct inject_rec *ext4_rec, struct ext4_context *fsc)
{
	DMDEBUG("%s():begin",__func__);
	if(ext4_rec->type == DM_INJECT_EXT4_DIRECTORY) {
		getDirectoryLocation(ic, ext4_rec, fsc);
	}
	DMDEBUG("%s():end",__func__);
}

/* for some file system data structures such as directory block, the user
 may provide the path of directory and the field. The On-Disk location of
 the directory is then computed by us.*/

bool deriveOnDiskLocation(int new_type)
{
	return (new_type & DM_INJECT_EXT4_DIRECTORY);
}

int ext4_inject_ctr(struct inject_c *ic)
{
	struct ext4_context *fsc;
	int blocks_per_group;
	struct inject_rec *tmp;
	int ret;

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
	ret = initGDT(ic, (blocks_per_group + 1) * 8);
	if(ret)
		return ret;
	ret = initRootInode(ic);
	if(ret)
		return ret;

	list_for_each_entry(tmp, &ic->inject_list, list) {
		if(deriveOnDiskLocation(tmp->type)){
			DMDEBUG("%s():deriveOnDiskLocation enabled", __func__);
			getOnDiskLocation(ic,tmp,fsc);				
		}else {
			DMDEBUG("%s():deriveOnDiskLocation returned false", __func__);
		}
	}
	DMDEBUG("%s END", __func__);
	return 0;
}

void ext4_inject_dtr(struct inject_c *ic)
{
	struct ext4_context *fsc;
	if(ic == NULL)
		return;
	if(ic->context != NULL){
		fsc = (struct ext4_context *) ic->context;
		if(fsc->gdt != NULL) {
			kfree(fsc->gdt);
			fsc->gdt = NULL;
		}
		kfree(fsc);
		fsc = NULL;
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
	| DM_INJECT_EXT4_JOURNAL | DM_INJECT_EXT4_EXTENDED_ATTRIBUTES) );
}

bool has2Fields(int new_type)
{
	return (new_type & ( DM_INJECT_EXT4_DIRECTORY ));
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
	 - dir[field][field]	directory block 
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
	char path[256] = {'\0'};
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
		} else if(isTypeStr(&cur_arg,"idmap")) {
			new_type = DM_INJECT_EXT4_INDIRECT_DATA_MAP;
		} else if(isTypeStr(&cur_arg,"etb")) { 
			new_type = DM_INJECT_EXT4_EXTENT_TABLE;
		} else if(isTypeStr(&cur_arg,"dir")) {
			DMDEBUG("dir detected");
			new_type = DM_INJECT_EXT4_DIRECTORY;
		} else if(isTypeStr(&cur_arg,"exattr")) {
		 	new_type = DM_INJECT_EXT4_EXTENDED_ATTRIBUTES;
		} else if(isTypeChar(&cur_arg,'j')) { 	
			new_type = DM_INJECT_EXT4_JOURNAL; 
		} else if (isTypeChar(&cur_arg,'b')) { 
			new_type = DM_INJECT_EXT4_BLOCK;
		} else if(isTypeChar(&cur_arg,'i')) {
			new_type =  DM_INJECT_EXT4_INODE;
		} else {
			DMDEBUG("%s():unidentified parameter %s\n",__func__, cur_arg);
		}
	
		// extract number, and either offset or field.
		if(hasOffset(new_type)){
			DMDEBUG("%s():new_type = %d hasOffset",__func__, new_type);
			sscanf(cur_arg, "%llu[%d][%d]%*c",&number, &offset,&size);
		} else if(hasField(new_type)) {
			DMDEBUG("%s():new_type = %d hasField",__func__, new_type);
			sscanf(cur_arg, "%llu[%s]%*c",&number, field);
			field[strlen(field) - 1] = '\0';
		} else if(hasOffsetAndField(new_type)) {
			DMDEBUG("%s():new_type = %d hasOffsetAndField",__func__, new_type);
			sscanf(cur_arg, "%llu[%d][%s]%*c",&number, &offset,field);
		} else if(has2Fields(new_type)) {
			DMDEBUG("%s():new_type = %d has2Fields",__func__, new_type);
			cur_arg++;
			sscanf(cur_arg, "%s",path);
			DMDEBUG("got path = %s", path);
			cur_arg+=(strlen(path) + 1);
			sscanf(cur_arg,"%s", field);
			DMDEBUG("got field = %s", field);
			cur_arg+=(strlen(field) + 1);
			i+=2;
		} else {
			DMDEBUG("%s():no match found for new_type = %d", __func__, new_type);
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

		if(strlen(path) > 2){
			path[strlen(path)] = '\0';
			if(path[0] != '/'){
				DMDEBUG("****ABSOLUTE PATH WAS NOT PROVIDED!!!***");
				DMDEBUG("please provide path relative to root, starting with a '/'");
			}
			strcpy(new_block->path, path);
		}else{
			new_block->path[0] = '\0';
		}

		new_block->offset = offset;

		DMDEBUG("%s(): parameters => op = %d type = %d number = %llu field = %s path = %s offset = %d size = %d\n", __func__, new_op, new_type, number,  field, path , offset, size);

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

        DMDEBUG("%s() corrupting bio %lu",__func__, (bio->bi_iter.bi_sector / 8) -1 );
	DMDEBUG("%s(): offset = %d size = %d",__func__, offset, size);

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

int lookup_size_ext4_dir_entry_2(char *field)
{
	if(!strcmp(field,"inode")) {
		return sizeof(((struct ext4_dir_entry_2 *)0)->inode);
	} else if(!strcmp(field, "rec_len")) {
		return sizeof(((struct ext4_dir_entry_2 *)0)->rec_len);
	} else if(!strcmp(field, "name_len")) {
		return sizeof(((struct ext4_dir_entry_2 *)0)->name_len);
	} else if(!strcmp(field, "file_type")) {
		return sizeof(((struct ext4_dir_entry_2 *)0)->file_type);
	} else if(!strcmp(field, "name")) {
		return sizeof(((struct ext4_dir_entry_2 *)0)->name);
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
		DMDEBUG("%s():corrupting block %lu which is the %lu'th block\n",__func__,bnum, number);
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

struct bio* read_block(sector_t bno, struct bio *bio, struct page *bio_page, struct inject_c *ic)
{
	int sz;

	DMDEBUG("%s(): reading sector %lu",__func__,bno * 8);
	bio_page = alloc_page(GFP_NOIO);
	if(bio_page == NULL){
		DMDEBUG("bgt_bio_page is null");
	}

	bio = bio_alloc(GFP_NOIO, 1);

	if(bio == NULL) {
		DMDEBUG("bgt_bio is null");
	}

	bio_set_dev(bio, ic->dev->bdev);
	bio->bi_iter.bi_sector = (bno * 8);
	bio->bi_end_io = NULL;//end_bio_bh_io_sync;
	bio->bi_private = NULL;

	sz = bio_add_page(bio, bio_page, PAGE_SIZE, 0);

	if(sz < PAGE_SIZE) {
		DMDEBUG("%s failed bio_add_page size allocated = %d",__func__, sz);
		bio_put(bio);
		return NULL;
	}

	bio_set_op_attrs(bio, REQ_OP_READ, REQ_META | REQ_PRIO | REQ_NOWAIT);

	submit_bio(bio);
	return bio;
}

static void inode_end_return(struct bio *bio)
{
	struct ext4_inode root_inode;
	char * bg_page_addr;

	if(bio == NULL) {
		DMDEBUG("bio null");
		return;
	}else {
		DMDEBUG("%s():page read done! status = %d",__func__, (bio->bi_status) );
		if(bio->bi_io_vec == NULL) {
			DMDEBUG("%s():bio->bi_io_vec is NULL",__func__);
			return;
		}
		if(bio->bi_io_vec->bv_page == NULL) {
			DMDEBUG("%s():bio->bi_io_vec->bv_page is NULL", __func__);
			return;
		}
		bg_page_addr = (unsigned char*)page_address(bio->bi_io_vec->bv_page);
	}
	DMDEBUG("%s():HERE",__func__);

//	
	// copy second inode, which is the root directory of ext4 file system
		if(bg_page_addr == NULL) {
			DMDEBUG("%s(): bg_page_addr is null",__func__);
			return;
		}
		if(bg_page_addr + 256 == NULL) {
			DMDEBUG("%s(): bg page addr + size is null",__func__);
			return;
		} 
		memcpy(&root_inode, bg_page_addr + 256, sizeof(struct ext4_inode));
		DMDEBUG("i_links_count = %d i_blocks_lo = %d\n",
				root_inode.i_links_count, root_inode.i_blocks_lo);

	return;
}

void processDirectory(struct ext4_context *fsc, sector_t bnum, char *page_addr, 
	struct inject_rec *ext4_ic, struct bio *bio, struct inject_c *ic) {

	if(ext4_ic == NULL){
		DMDEBUG("%s(): injection context not initialized, returning", __func__);
		return;
	}

	if(bnum != ext4_ic->block_num){
		DMDEBUG("%s(): read block %lu, NEED BLOCK %d DO NOT FUZZ", __func__, bnum, ext4_ic->block_num);
		return;
	}else{
		DMDEBUG("bnumber match, corrupting bnumber %lu", bnum);
	}

	DMDEBUG("%s(): corrupting block number %lu offset %d size %d", __func__, bnum, ext4_ic->offset, ext4_ic->size);
	corrupt_bio(bio, ext4_ic->offset, ext4_ic->size);
}

bool ext4_block_from_dev(struct inject_c *ic, struct bio *bio, struct bio_vec *bvec, sector_t sec)
{	
	sector_t bnum;
	struct ext4_context *fsc;
	unsigned char *my_page_addr;
	struct inject_rec *tmp;

	DMDEBUG("%s():INIT",__func__);
	fsc = ic->context;
	if(sec > 8) {
		bnum = (sec / 8);
	}else {
		bnum = 0;
	}
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
		if(tmp->type == DM_INJECT_EXT4_DIRECTORY) {
			DMDEBUG("%s(): PROCESS DIR",__func__);
			processDirectory(fsc,bnum,my_page_addr,tmp,bio,ic);
		}
	}
	DMDEBUG("%s():EXIT",__func__);
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
