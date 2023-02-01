#include "inode_manager.h"

// disk layer -----------------------------------------

disk::disk()
{
  bzero(blocks, sizeof(blocks));//clear the sizeof(blocks) bits in blocks to 0.
}

void
disk::read_block(blockid_t id, char *buf)
{
    //(unsigned char*)buf = blocks[id];
    memcpy(buf, blocks[id], BLOCK_SIZE);//use the MEMCPY function to avoid the error of type changing.
}

void
disk::write_block(blockid_t id, const char *buf)
{
   // blocks[id] = (unsigned char*)buf;
   memcpy(blocks[id], buf, BLOCK_SIZE);
}

void
disk::write_block_left(blockid_t id, const char* buf, int size) {
   memcpy(blocks[id], buf, size);//used to write the content that is less than a block size to the disk layer.
}

// block layer -----------------------------------------

// Allocate a free disk block.
blockid_t
block_manager::alloc_block()
{
  /*
   * your code goes here.
   * note: you should mark the corresponding bit in block bitmap when alloc.
   * you need to think about which block you can start to be allocated.
   */
  uint32_t start = IBLOCK(INODE_NUM, sb.nblocks) + 1;//the start of the blocks that can be allocated should be the next of the end of the inode table.  
  for(uint32_t i = start; i < BLOCK_NUM; ++i) {
  	if(using_blocks[i] == 0) {
	    using_blocks[i] = 1;//make the block to the using mode.
	    return i;
	}
  }
  return 0;
}

void
block_manager::free_block(uint32_t id)
{
  /* 
   * your code goes here.
   * note: you should unmark the corresponding bit in the block bitmap when free.
   */
  if(using_blocks[id] == 0) return ;//unallocated, return directly.
  using_blocks[id] = 0;//free the allocated block int the bitmap.
  return;
}

// The layout of disk should be like this:
// |<-sb->|<-free block bitmap->|<-inode table->|<-data->|
block_manager::block_manager()
{
  d = new disk();
  
  // format the disk
  sb.size = BLOCK_SIZE * BLOCK_NUM;
  sb.nblocks = BLOCK_NUM;//super block --> remember the max block number.
  sb.ninodes = INODE_NUM;//super block --> remember the max inode number.

}

void
block_manager::read_block(uint32_t id, char *buf)
{
  d->read_block(id, buf);//the effect of the layering structure.
}

void
block_manager::write_block(uint32_t id, const char *buf)
{
  d->write_block(id, buf);
}

void
block_manager::write_block_left(uint32_t id, const char* buf, int size) {
    d->write_block_left(id, buf, size);
}

// inode layer -----------------------------------------

inode_manager::inode_manager()
{
  bm = new block_manager();
  uint32_t root_dir = alloc_inode(extent_protocol::T_DIR);//in out implementation, allocate the inode 1 to the root file,while is a dir with the name "".
  if (root_dir != 1) {
    printf("\tim: error! alloc first inode %d, should be 1\n", root_dir);
    exit(0);
  }
}

/* Create a new file.
 * Return its inum. */
uint32_t
inode_manager::alloc_inode(uint32_t type)
{
  /* 
   * your code goes here.
   * note: the normal inode block should begin from the 2nd inode block.
   * the 1st is used for root_dir, see inode_manager::inode_manager().
   */
  uint32_t inum = 0;
  struct inode* ino;
  //printf("\tim: alloc_inode %d\n", type);
  for(int i = 0; i < INODE_NUM - 1; ++i) {//NOTE:can't traverse the i = INODE_NUM - 1,beacause the block 0 is used default (Boot Block).
      inum = i + 1;
      //if(inum == 1) continue;
      ino = get_inode(inum);//get the corresponding inode .
      if(ino == nullptr) {//find a unused inode, put the new file into it.
	ino = (struct inode*)malloc(sizeof(struct inode));
	bzero(ino, sizeof(struct inode));
	ino->type = type;
	/*unsigned int cur = (unsigned int)time(NULL);
	ino->atime = cur;
	ino->mtime = cur;
	ino->ctime = cur;//set the three times at the end of the allocation.*/
	time_t t;
	time(&t);
	unsigned int cur = (unsigned int)t;
	ino->atime = cur;
	ino->mtime = cur;
	ino->ctime = cur;
	put_inode(inum, ino);//put the newly allocated inode into the disk.
	free(ino);//avoid the memory leak.
	return inum;
      }
  } 
  free(ino);
  /*if(bm->using_blocks.get(0) != 1) {
  	unsigned int cur = (unsigned int)time(NULL);
	ino->type = type;
	ino->atime = cur;
	ino->mtime = cur;
	ino->ctime = cur;
	inum = 0;
	put_inode(inum, ino);
	return inum;
  }*/
  return 1;
}

void
inode_manager::free_inode(uint32_t inum)
{
  /* 
   * your code goes here.
   * note: you need to check if the inode is already a freed one;
   * if not, clear it, and remember to write back to disk.
   */
  struct inode* ino = get_inode(inum);
  if(ino == nullptr) return ;//the inode is not used.
  if(ino->type == 0) return ;//the inode type is unknown.
  ino->type = 0;//change the file type to the UNKNOWN.
  ino->atime = 0;
  ino->mtime = 0;
  ino->ctime = 0;
  ino->size = 0;
  put_inode(inum, ino);//don't forget to put the cleared inode back to the disk.
  free(ino);
  return;
}


/* Return an inode structure by inum, NULL otherwise.
 * Caller should release the memory. */
struct inode* 
inode_manager::get_inode(uint32_t inum)
{
  struct inode *ino;
  /* 
   * your code goes here.
   */
  if(inum < 0 || inum >= INODE_NUM) return nullptr;
  struct inode* ino_disk;
  //printf("\tim: get_inode %d\n", inum);//similar to the implementation og the put_inode function.
  char buf[BLOCK_SIZE];
  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);//read the corresponding data into the buf,the IBLOCK marco is used to get the corresponding line of the needed block.
  //struct inode* ino;
  /*ino_disk = (struct inode*)buf + inum % IPB;//similar to the put_inode
  ino = (struct inode*)malloc(sizeof(struct inode));//remeber to malloc the new memory for the new inode QAQ
  *ino = *ino_disk;//similar to the put_inode function.*/
  ino = (struct inode*)malloc(sizeof(struct inode));
  *ino = *((struct inode*)(buf + (inum % IPB) * sizeof(struct inode)));
  if(ino->type == 0) {
  	return nullptr;//inode has NO type, meaning that it's unused.
  }
  //free(ino_disk);
  return ino;
}

void
inode_manager::put_inode(uint32_t inum, struct inode *ino)
{
  char buf[BLOCK_SIZE];
  struct inode *ino_disk;

  //printf("\tim: put_inode %d\n", inum);
  if (ino == NULL)
    return;

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  ino_disk = (struct inode*)buf + inum%IPB;
  *ino_disk = *ino;
  bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
  //printf("\tim: put_inode end");
}

#define MIN(a,b) ((a)<(b) ? (a) : (b))

void putDataIntoBuf(char**& buf_out, char buf[BLOCK_SIZE], int i) {
	memcpy(*buf_out + i * BLOCK_SIZE, buf, BLOCK_SIZE);
}

/* Get all the data of a file by inum. 
 * Return alloced data, should be freed by caller. */
void
inode_manager::read_file(uint32_t inum, char **buf_out, int *size)
{
  /*
   * your code goes here.
   * note: read blocks related to inode number inum,
   * and copy them to buf_out
   */
 //the *size arg should be applied to the ino->size! QAQ
  //printf("\tim: read_file %d", inum);
  struct inode* ino = get_inode(inum);//get the coressponding inode .
  if(ino == nullptr) return ;//try to read an unallocated inode info.
  //*buf_out = (char*)malloc(*size);
  *size = ino->size;//according extent_server.cc file, the size is used to store the total size of the read file.
  if(*size > MAXFILE * BLOCK_SIZE) return ;
  *buf_out = (char*)malloc(*size);
  int read_size = *size;
  //int total_size = (int)ino->size;//the total size of the file.
  ino->atime = (unsigned int)time(NULL);//set the new access time of the read node.
  int filled = read_size / BLOCK_SIZE;//count the number of the filled block.
  int left = read_size % BLOCK_SIZE;//count the left size in the unfilled block .(can be 0).
  for(int i = 0; i < filled; ++i) {
  	char buf[BLOCK_SIZE];//C language!
	blockid_t id = get_blockid_i(i, ino);//get the current reading block id.
	if(id == 0) return ;
	bm->read_block(id, buf);//read the info in the filled block.
	putDataIntoBuf(buf_out, buf, i);
	//memcpy(*buf_out + i * BLOCK_SIZE, buf, BLOCK_SIZE);//NOTE:be careful of the operations to the pointer of char!
  }
  if(left != 0) {
  	char buf[BLOCK_SIZE];
	blockid_t id = get_blockid_i(filled, ino);
	if(id == 0) return ;
	bm->read_block(id, buf);
	memcpy(*buf_out + filled * BLOCK_SIZE, buf, left);//read the left part of the file info.
  }
  free(ino);
  return;
}

/* alloc/free blocks if needed */
void
inode_manager::write_file(uint32_t inum, const char *buf, int size)
{
  /*
   * your code goes here.
   * note: write buf to blocks of inode inum.
   * you need to consider the situation when the size of buf 
   * is larger or smaller than the size of original inode
   */
  //printf("\tim: write_file %d size: %d", inum, size);
  struct inode* ino = get_inode(inum);
  if(ino == nullptr) return ;
  int old = (int)ino->size;
  int old_filled = old / BLOCK_SIZE;
  int old_left = old % BLOCK_SIZE;
  int old_num = (old_left == 0) ? old_filled : (old_filled + 1);//count the old block number needed.
  int new_filled = size / BLOCK_SIZE;
  int new_left = size % BLOCK_SIZE;
  int new_num = (new_left == 0) ? new_filled : (new_filled + 1);//count the new block number needed.
  //printf("\tim: old_num = %d, new_num = %d\n", old_num, new_num);
  if(new_num < old_num) {//the case that some blocks are needed to be freed.
	//printf("\tim: get into the new < old case!");
  	for(int i = 0; i < new_filled; ++i) {
	    //blockid_t id = ino->blocks[i];
	    blockid_t id = get_blockid_i(i, ino);
	    if(id == 0) return ;
	    bm->write_block(id, buf + i * BLOCK_SIZE);
	}
	if(new_left != 0) {
	    //blockid_t id = ino->blocks[new_filled];
	    blockid_t id = get_blockid_i(new_filled, ino);
	    if(id == 0) return ;
	    bm->write_block_left(id, buf + new_filled * BLOCK_SIZE, new_left); 
	}	
	for(uint32_t i = (uint32_t)new_num; i < old_num; ++i) {
	    //bm->free_block(ino->blocks[i]);
	    bm->free_block(get_blockid_i(i, ino));
	}
  } 
  else {
  	if(new_num > old_num) {//the case that some blocks are needed to be allocated to the inode.
	    //printf("\tim: get into the new > old case!");
	    for(int i = old_num; i < new_num; ++i) {
	    	alloc_blockid_i(i, ino);
		//blockid_t id = get_blockid_i(i, ino);
		//if(id == 0) return ;
	    }
	    for(int i = 0; i < new_filled; ++i) {
	    	blockid_t id = get_blockid_i(i, ino);
		bm->write_block(id, buf + i * BLOCK_SIZE);
	    }
	    if(new_left != 0) {
	    	//blockid_t id = bm->alloc_block();
		//alloc_blockid_i(new_filled, ino);
		//if(id == 0) return ;
		//ino->blocks[end] = id;
		blockid_t id = get_blockid_i(new_filled, ino);
		if(id == 0) return ;
		bm->write_block_left(id, buf + new_filled * BLOCK_SIZE, new_left);
		//char c[BLOCK_SIZE];
		//memcpy(c, buf + new_filled * BLOCK_SIZE, new_left);
		//bm->write_block(id, c);
	    }
	}
	else { //new_num == old_num
	    //printf("\tim: get into the new = old case!");
	    int end = (new_left == 0) ? new_num : new_num - 1;
	    for(int i = 0; i < end; ++i) {
	    	//blockid_t id = ino->blocks[i];
		blockid_t id = get_blockid_i(i, ino);
		if(id == 0) return ;
		bm->write_block(id, buf + i * BLOCK_SIZE);
	    }
	    if(new_left != 0) {
	    	//blockid_t id = ino->blocks[end];
		blockid_t id = get_blockid_i(end, ino);
		bm->write_block_left(id, buf + end * BLOCK_SIZE, new_left);
	    }
	}
  }
  unsigned int cur = (unsigned int)time(NULL);
  ino->atime = cur;
  ino->ctime = cur;
  ino->mtime = cur;//update the inode's times
  ino->size = size;//update the file size to the new one.
  put_inode(inum, ino);//put the updated inode back to the disk.
  free(ino);
  return;
}

void
inode_manager::get_attr(uint32_t inum, extent_protocol::attr &a)
{//the basic function of the FUSE_getattr().
  /*
   * your code goes here.
   * note: get the attributes of inode inum.
   * you can refer to "struct attr" in extent_protocol.h
   */
  struct inode* ino = get_inode(inum);//get the corresponding inode of inum.
  if(ino == nullptr) return ;
  a.atime = ino->atime;
  a.mtime = ino->mtime;
  a.ctime = ino->ctime;
  a.type = ino->type;
  a.size = ino->size;
  free(ino); 
  return;
}

void
inode_manager::remove_file(uint32_t inum)
{
  /*
   * your code goes here
   * note: you need to consider about both the data block and inode of the file
   */
  struct inode* ino = get_inode(inum);
  if(ino == nullptr || ino->type == 0) return ;//the inode is not used.
  int size = (int)ino->size;
  int filled = size / BLOCK_SIZE;
  int left = size % BLOCK_SIZE;
  for(int i = 0; i < filled; ++i) {
      clockid_t id = get_blockid_i(i, ino);
      bm->free_block(id);
  }  
  if(left != 0) {
      clockid_t id = get_blockid_i(filled, ino);
      bm->free_block(id);
  }//free the used blocks.
  int total = (left == 0) ? filled : filled + 1;
  if(total > NDIRECT) bm->free_block(ino->blocks[NDIRECT]);//if use the undirect block, remember to clear the indirect block ITSELF.
  bzero(ino, sizeof(inode_t));
  free_inode(inum);//free the inode at the end of the free function.
  return;
}

blockid_t
inode_manager::get_blockid_i(int i, struct inode* ino) {
    blockid_t ans = 0;
    //blockid_t *indirectBlocks;
    if(i < NDIRECT) ans = ino->blocks[i];//i < NDIRECT,can use the direct block(s)
    else {
    	if(i >= MAXFILE) exit(0);//if the i exceed the MAXFILE limitation, abort directly.
	char buf[BLOCK_SIZE];
	bm->read_block(ino->blocks[NDIRECT], buf);
	//indirectBlocks = (blockid_t*)buf;
	//ans = indirectBlocks[i - NDIRECT];
	ans = ((blockid_t*)buf)[i - NDIRECT];
    }
    return ans;
}

blockid_t
inode_manager::alloc_blockid_i(int i, struct inode* ino) {
    blockid_t ans = 0;
    if(i < NDIRECT) {
    	ino->blocks[i] = bm->alloc_block();
	return ino->blocks[i];
    }
    if(i >= MAXFILE) exit(0);
    char buf[BLOCK_SIZE];
    if(ino->blocks[NDIRECT] == 0) {//if the inirect block is empty, allocate a new block to it.
    	ino->blocks[NDIRECT] = bm->alloc_block();
	//if(ino->blocks[NDIRECT] == 0) return 0;//fail to allocate 
    }
    bm->read_block(ino->blocks[NDIRECT], buf);
    ((blockid_t*)buf)[i - NDIRECT] = bm->alloc_block();
    ans = ((blockid_t*)buf)[i - NDIRECT];
    bm->write_block(ino->blocks[NDIRECT], buf);
    return ans;
}
