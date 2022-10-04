#include "inode_manager.h"
#include <iostream>


// some helpful function
bool check_bit(char op_char,int pos){
  // check the bit located at pos of op_char is set as one or not
  // pos range : [0, 7]
  uint8_t judge = (op_char & (1 << pos)) >> pos;
  if(judge == 1){
    return true;
  }else{
    return false;
  }
}

void set_bit(char &op_char,int pos){
  // do check before setting
  bool check = check_bit(op_char,pos);
  if(check){
    std::cout << "wrong in set bit" << std::endl;
  }

  // do setting
  op_char = op_char | (1 << pos);
}

void unset_bit(char &op_char,int pos){
  bool check = check_bit(op_char,pos);
  if(!check){
    std::cout << "wrong in unsetbit" << std::endl;
  }

  //do unsetting
  op_char = op_char & (~(1 << pos));
}

void update_inode(inode* ino,uint32_t type){
  // the logic of updating a inode struct
  ino->type = type;
}

// return a pointer to core block pointed by indirect block
blockid_t* get_indirect(inode* ino,block_manager* bm){
  char *buf = new char[BLOCK_SIZE];
  bm->read_block(ino->blocks[NDIRECT],buf);
  blockid_t* block_index = (blockid_t *)buf;
  return block_index;
}

// read 
void read_file_direct(inode_manager* ino_manager){

}

// disk layer -----------------------------------------

disk::disk()
{
  bzero(blocks, sizeof(blocks));
}

void
disk::read_block(blockid_t id, char *buf)
{
  //that is a version that buf is not allocated in function
  for(int i = 0;i < BLOCK_SIZE;++i){
    buf[i] = this->blocks[id][i];
  }
}

void
disk::write_block(blockid_t id, const char *buf)
{
  for(int i = 0;i < BLOCK_SIZE;++i){
    this->blocks[id][i] = buf[i];
  }
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
  bool flag = false;

  // printf("new start\n");
  // the index of checking bit
  for(int index = 0;;)
  {
    /* code */
    // check one by one
    // printf("index is %d\n",index);
    flag = true;

    int b_num = BBLOCK(index);
    // get the left num minus the b_num block of bits
    int left_bit = index - (b_num-BBLOCK(0)) * BPB;
    if(left_bit < 0){
      std::cout << "wrong in alloc" << std::endl;
    }

    // try to check the bit
    int block_col = left_bit / 8;
    int left_col_bit = left_bit - block_col * 8;

    // get the core bytes;
    char *buf = new char[BLOCK_SIZE];
    this->d->read_block(b_num,buf);
    char op_char = buf[block_col];
    bool check = check_bit(op_char,left_col_bit);
    if(!check){
      set_bit(op_char,left_col_bit);
      if(!check_bit(op_char,left_col_bit)){
        printf("wrong in set bit\n");
      }
      buf[block_col] = op_char;
      this->d->write_block(b_num,buf);
      return index;
    }else{
      index++;
      for(;flag;index++){
        int bb_num = BBLOCK(index);
        if(b_num < bb_num){
          delete [] buf;
          flag = false;
          //index--;
          continue;
        }

        // use same block
        left_bit = index - (b_num-BBLOCK(0)) * BPB;
        block_col = left_bit / 8;
        left_col_bit = left_bit - block_col * 8;

        // read core char
        char op_char = buf[block_col];
        check = check_bit(op_char,left_col_bit);
        if(!check){
          set_bit(op_char,left_col_bit);
          buf[block_col] = op_char;
          this->d->write_block(b_num,buf);
          return index;
        }

        //printf("index is %d\n",index);


        // do some check
        if(index > BLOCK_NUM){
          std::cout << "range wrong " << std::endl;
          return 0;
        }
      }
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

  // find the core block_row and block_col and core bit pos
  int block_row = BBLOCK(id);
  int left_bit = id - (block_row-BBLOCK(0)) * BPB;
  int block_col = left_bit / 8;
  int left_col_bit = left_bit - block_col * 8;
  char *buf = new char[BLOCK_SIZE];
  this->d->read_block(block_row,buf);
  char op_char = buf[block_col];
  unset_bit(op_char,left_col_bit);
  buf[block_col] = op_char;
  this->d->write_block(block_row,buf);
  delete [] buf;
  
  return;
}

// The layout of disk should be like this:
// |<-sb->|<-free block bitmap->|<-inode table->|<-data->|
block_manager::block_manager()
{
  d = new disk();

  // format the disk
  sb.size = BLOCK_SIZE * BLOCK_NUM;
  sb.nblocks = BLOCK_NUM;
  sb.ninodes = INODE_NUM;

  // alloc bit for boot and super block and bitmap and inode table
  for(int i = 0;i < 2 + BLOCK_NUM/BPB + 1 + INODE_NUM/IPB;++i){
    int check_num = alloc_block();
    if(i != check_num){
      printf("wrong,check is %d, i is %d\n",check_num,i);
    }
  }

}

void
block_manager::read_block(uint32_t id, char *buf)
{
  d->read_block(id, buf);
}

void
block_manager::write_block(uint32_t id, const char *buf)
{
  d->write_block(id, buf);
}

// inode layer -----------------------------------------

inode_manager::inode_manager()
{
  bm = new block_manager();
  uint32_t root_dir = alloc_inode(extent_protocol::T_DIR);
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

  // returned value
  int inum = 1;
  // first , the case of dir
  if(type == extent_protocol::T_DIR){
    // lookup all inode and find which type is 0 (it stand for free block)
    for(;inum < INODE_NUM;++inum){
      inode* ino = get_inode(inum);
      if(ino->type == 0){
        // find the free block
        update_inode(ino,extent_protocol::T_DIR);
        put_inode(inum,ino);
        //printf("returned type is %d\n",inum);
        return inum;
      }
      delete ino;
    }
    std::cout << "no enough block" << std::endl;
  }else if(type == extent_protocol::T_FILE){
    // lookup all inode and find which type is 0 (it stand for free block)
    for(;inum < INODE_NUM;++inum){
      inode* ino = get_inode(inum);
      if(ino->type == 0){
        // find the free block
        update_inode(ino,extent_protocol::T_FILE);
        put_inode(inum,ino);
        return inum;
      }
      delete ino;
    }
    std::cout << "no enough block" << std::endl;
  }else{
    std::cout << "type error" << std::endl;
  }
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

  // first , find the inode
  inode* ino = get_inode(inum);

  // check is free or not
  if(ino->type == 0){
    return;
  }else{
    // it is a code that may cause some bugs
    memset(ino,0,sizeof(struct inode));

    this->put_inode(inum,ino);
  }

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
  char *buf = new char[BLOCK_SIZE];

  // read message from disk
  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  ino = (struct inode*)buf + inum%IPB;
  return ino;
}

void
inode_manager::put_inode(uint32_t inum, struct inode *ino)
{
  char buf[BLOCK_SIZE];
  struct inode *ino_disk;

  printf("\tim: put_inode %d\n", inum);
  if (ino == NULL)
    return;

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  ino_disk = (struct inode*)buf + inum%IPB;
  *ino_disk = *ino;
  bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
}

#define MIN(a,b) ((a)<(b) ? (a) : (b))

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
  
  // may need to fix the atime in the future

  // get the inode first
  inode* ino = get_inode(inum);
  *size = ino->size;
  *buf_out = new char[*size];

  // the bytes that has not been into the string
  int left_num = *size;

  int offset = 0;

  // read data from block and copy to buf_out
  // first. read direct block
  for(int i = 0;i < NDIRECT;++i){
    char *buf = new char[BLOCK_SIZE];
    this->bm->read_block(ino->blocks[i],buf);
    int min_size = MIN(left_num,BLOCK_SIZE);
    memcpy((*buf_out + offset),buf,min_size);
    offset += min_size;
    delete buf;
    left_num -= min_size;
    if(left_num == 0){
      return;
    }
  }

  // second, read indirect block
  char *buf = new char[BLOCK_SIZE];
  this->bm->read_block(ino->blocks[NDIRECT],buf);
  blockid_t* block_index = (blockid_t *)buf;

  int left_file_num = ((left_num + BLOCK_SIZE - 1) / BLOCK_SIZE);

  for(int i = 0;i < left_file_num;++i){
    // read core block;
    char* ind_buf = new char[BLOCK_SIZE];
    this->bm->read_block(block_index[i],ind_buf);
    int min_size = MIN(left_num,BLOCK_SIZE);
    memcpy((*buf_out + offset),ind_buf,min_size);
    offset += min_size;
    delete ind_buf;
    left_num -= min_size;
    if(left_num == 0){
      break;
    }
  }
  if(left_num != 0){
    printf("some wrong in read file\n");
  }
  delete [] buf;
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
  
  // write to cover the previous thing
  inode* ino = get_inode(inum);
  if(size > ino->size){
    // need to alloc new block

    // first , compute the num of blocks need to be alloced
    int ino_block = ((ino->size + BLOCK_SIZE - 1) / BLOCK_SIZE);
    int need_block = ((size + BLOCK_SIZE - 1) / BLOCK_SIZE);

    if(ino_block == need_block){
      // don't need to alloc block

      int writed_bytes = 0;
      int offset = 0;

      if(need_block <= NDIRECT){
        // write block by block
        for(int i = 0;i < need_block;++i){
          // think about the situlation that the writing bytes is smaller than BLOCK_SIZE
          if(writed_bytes + BLOCK_SIZE > size){
            char* new_buf = new char[BLOCK_SIZE];
            memcpy(new_buf,buf+offset,size - writed_bytes);
            memset(new_buf+size-writed_bytes,0,writed_bytes + BLOCK_SIZE - size);
            this->bm->write_block(ino->blocks[i],new_buf);
            delete [] new_buf;
            break;
          }

          this->bm->write_block(ino->blocks[i],buf + offset);
          offset += BLOCK_SIZE;
          writed_bytes += BLOCK_SIZE;
        }

        // update the inode
        ino->size = size;
        this->put_inode(inum,ino);
        delete ino;
        return;
      }
      else{
        // need to write indirect block
        // write block by block
        for(int i = 0;i < NDIRECT;++i){
          // think about the situlation that the writing bytes is smaller than BLOCK_SIZE
          if(writed_bytes + BLOCK_SIZE > size){
            char* new_buf = new char[BLOCK_SIZE];
            memcpy(new_buf,buf+offset,size - writed_bytes);
            memset(new_buf+size-writed_bytes,0,writed_bytes + BLOCK_SIZE - size);
            this->bm->write_block(ino->blocks[i],new_buf);
            delete [] new_buf;
            break;
          }

          this->bm->write_block(ino->blocks[i],buf + offset);
          offset += BLOCK_SIZE;
          writed_bytes += BLOCK_SIZE;
        }

        // get indirect block
        char *bl_buf = new char[BLOCK_SIZE];
        this->bm->read_block(ino->blocks[NDIRECT],bl_buf);
        blockid_t* block_index = (blockid_t *)bl_buf;

        // write indirect block
        for(int i = NDIRECT;i < need_block;++i){
          // think about the situlation that the writing bytes is smaller than BLOCK_SIZE
          if(writed_bytes + BLOCK_SIZE > size){
            char* new_buf = new char[BLOCK_SIZE];
            memcpy(new_buf,buf+offset,size - writed_bytes);
            memset(new_buf+size-writed_bytes,0,writed_bytes + BLOCK_SIZE - size);
            this->bm->write_block(block_index[i - NDIRECT],new_buf);
            delete [] new_buf;
            break;
          }

          this->bm->write_block(block_index[i - NDIRECT],buf + offset);
          offset += BLOCK_SIZE;
          writed_bytes += BLOCK_SIZE;
        }

        // update the inode
        ino->size = size;
        this->put_inode(inum,ino);
        delete ino;
        delete [] bl_buf;
        return;
      }
    }else if(ino_block < need_block){
      int writed_bytes = 0;
      int offset = 0;

      if(need_block <= NDIRECT){
        // need to alloc new block
        for(int i = ino_block;i < need_block;++i){
          ino->blocks[i] =this->bm->alloc_block();
        }

        // write block by block
        for(int i = 0;i < need_block;++i){
          // think about the situlation that the writing bytes is smaller than BLOCK_SIZE
          if(writed_bytes + BLOCK_SIZE > size){
            char* new_buf = new char[BLOCK_SIZE];
            memcpy(new_buf,buf+offset,size - writed_bytes);
            memset(new_buf+size-writed_bytes,0,writed_bytes + BLOCK_SIZE - size);
            this->bm->write_block(ino->blocks[i],new_buf);
            delete [] new_buf;
            break;
          }

          this->bm->write_block(ino->blocks[i],buf + offset);
          offset += BLOCK_SIZE;
          writed_bytes += BLOCK_SIZE;
        }

        // update the inode
        ino->size = size;
        this->put_inode(inum,ino);
        delete ino;
        return;
      }else if(ino_block > NDIRECT){
        // first, read the indirect block and write to it
        // get indirect block
        char *bl_buf = new char[BLOCK_SIZE];
        this->bm->read_block(ino->blocks[NDIRECT],bl_buf);
        blockid_t* block_index = (blockid_t *)bl_buf;
        //blockid_t* block_index = get_indirect(ino,this->bm);

        int be_index = ino_block - NDIRECT;
        int e_index = need_block - NDIRECT;
        for(int i = be_index;i < e_index;++i){
          block_index[i] = this->bm->alloc_block();
        }

        this->bm->write_block(ino->blocks[NDIRECT],bl_buf);
        delete [] bl_buf;

        // write to disk
        // need to write indirect block
        // write block by block
        for(int i = 0;i < NDIRECT;++i){
          // think about the situlation that the writing bytes is smaller than BLOCK_SIZE
          if(writed_bytes + BLOCK_SIZE > size){
            char* new_buf = new char[BLOCK_SIZE];
            memcpy(new_buf,buf+offset,size - writed_bytes);
            memset(new_buf+size-writed_bytes,0,writed_bytes + BLOCK_SIZE - size);
            this->bm->write_block(ino->blocks[i],new_buf);
            delete [] new_buf;
            break;
          }

          this->bm->write_block(ino->blocks[i],buf + offset);
          offset += BLOCK_SIZE;
          writed_bytes += BLOCK_SIZE;
        }

        // get indirect block
        bl_buf = new char[BLOCK_SIZE];
        this->bm->read_block(ino->blocks[NDIRECT],bl_buf);
        block_index = (blockid_t *)bl_buf;

        // write indirect block
        for(int i = NDIRECT;i < need_block;++i){
          // think about the situlation that the writing bytes is smaller than BLOCK_SIZE
          if(writed_bytes + BLOCK_SIZE > size){
            char* new_buf = new char[BLOCK_SIZE];
            memcpy(new_buf,buf+offset,size - writed_bytes);
            memset(new_buf+size-writed_bytes,0,writed_bytes + BLOCK_SIZE - size);
            this->bm->write_block(block_index[i - NDIRECT],new_buf);
            delete [] new_buf;
            break;
          }

          this->bm->write_block(block_index[i - NDIRECT],buf + offset);
          offset += BLOCK_SIZE;
          writed_bytes += BLOCK_SIZE;
        }

        // update the inode
        ino->size = size;
        this->put_inode(inum,ino);
        delete ino;
        return;
        
      }else{
        // first , alloc from ino_block to NDIBLOCK
        for(int i = ino_block;i < NDIRECT + 1;++i){
          int midval = this->bm->alloc_block();
          ino->blocks[i] = midval;
          if(midval == 4096){
            int a = 2;
            int b = 1+a;
          }
        }

        // second , alloc indirect block
        // get indirect block
        char *bl_buf = new char[BLOCK_SIZE];
        this->bm->read_block(ino->blocks[NDIRECT],bl_buf);
        blockid_t* block_index = (blockid_t *)bl_buf;
        //blockid_t* block_index = get_indirect(ino,this->bm);

        for(int i = NDIRECT;i < need_block;++i){
          block_index[i - NDIRECT] = this->bm->alloc_block();
        }        
        this->bm->write_block(ino->blocks[NDIRECT],bl_buf);
        delete [] bl_buf;

        // to write indirect block
        // write block by block
        for(int i = 0;i < NDIRECT;++i){
          // think about the situlation that the writing bytes is smaller than BLOCK_SIZE
          if(writed_bytes + BLOCK_SIZE > size){
            char* new_buf = new char[BLOCK_SIZE];
            memcpy(new_buf,buf+offset,size - writed_bytes);
            memset(new_buf+size-writed_bytes,0,writed_bytes + BLOCK_SIZE - size);
            this->bm->write_block(ino->blocks[i],new_buf);
            delete [] new_buf;
            break;
          }

          this->bm->write_block(ino->blocks[i],buf + offset);
          offset += BLOCK_SIZE;
          writed_bytes += BLOCK_SIZE;
        }

        // get indirect block
        bl_buf = new char[BLOCK_SIZE];
        this->bm->read_block(ino->blocks[NDIRECT],bl_buf);
        block_index = (blockid_t *)bl_buf;

        // write indirect block
        for(int i = NDIRECT;i < need_block;++i){
          // think about the situlation that the writing bytes is smaller than BLOCK_SIZE
          if(writed_bytes + BLOCK_SIZE > size){
            char* new_buf = new char[BLOCK_SIZE];
            memcpy(new_buf,buf+offset,size - writed_bytes);
            memset(new_buf+size-writed_bytes,0,writed_bytes + BLOCK_SIZE - size);
            this->bm->write_block(block_index[i - NDIRECT],new_buf);
            delete [] new_buf;
            break;
          }

          this->bm->write_block(block_index[i - NDIRECT],buf + offset);
          offset += BLOCK_SIZE;
          writed_bytes += BLOCK_SIZE;
        }

        // update the inode
        ino->size = size;
        this->put_inode(inum,ino);
        delete ino;
        delete [] bl_buf;
        return;
      }
    }else{
      printf("wrong in write file need to alloc new block\n");
      return;
    }
  }else{
    // donn't need to alloc new block but need to free some blocks

    // first , compute the num of blocks need to be freed
    int ino_block = ((ino->size + BLOCK_SIZE - 1) / BLOCK_SIZE);
    int need_block = ((size + BLOCK_SIZE - 1) / BLOCK_SIZE);
    if(ino_block == need_block){
      // don't need to free block

      int writed_bytes = 0;
      int offset = 0;

      if(need_block <= NDIRECT){
        // write block by block
        for(int i = 0;i < need_block;++i){
          // think about the situlation that the writing bytes is smaller than BLOCK_SIZE
          if(writed_bytes + BLOCK_SIZE > size){
            char* new_buf = new char[BLOCK_SIZE];
            memcpy(new_buf,buf+offset,size - writed_bytes);
            memset(new_buf+size-writed_bytes,0,writed_bytes + BLOCK_SIZE - size);
            this->bm->write_block(ino->blocks[i],new_buf);
            delete [] new_buf;
            break;
          }

          this->bm->write_block(ino->blocks[i],buf + offset);
          offset += BLOCK_SIZE;
          writed_bytes += BLOCK_SIZE;
        }

        // update the inode
        ino->size = size;
        this->put_inode(inum,ino);
        delete ino;
        return;
      }else{
        // need to write indirect block
        // write block by block
        for(int i = 0;i < NDIRECT;++i){
          // think about the situlation that the writing bytes is smaller than BLOCK_SIZE
          if(writed_bytes + BLOCK_SIZE > size){
            char* new_buf = new char[BLOCK_SIZE];
            memcpy(new_buf,buf+offset,size - writed_bytes);
            memset(new_buf+size-writed_bytes,0,writed_bytes + BLOCK_SIZE - size);
            this->bm->write_block(ino->blocks[i],new_buf);
            delete [] new_buf;
            break;
          }

          this->bm->write_block(ino->blocks[i],buf + offset);
          offset += BLOCK_SIZE;
          writed_bytes += BLOCK_SIZE;
        }

        // get indirect block
        char *bl_buf = new char[BLOCK_SIZE];
        this->bm->read_block(ino->blocks[NDIRECT],bl_buf);
        blockid_t* block_index = (blockid_t *)bl_buf;

        // write indirect block
        for(int i = NDIRECT;i < need_block;++i){
          // think about the situlation that the writing bytes is smaller than BLOCK_SIZE
          if(writed_bytes + BLOCK_SIZE > size){
            char* new_buf = new char[BLOCK_SIZE];
            memcpy(new_buf,buf+offset,size - writed_bytes);
            memset(new_buf+size-writed_bytes,0,writed_bytes + BLOCK_SIZE - size);
            this->bm->write_block(block_index[i - NDIRECT],new_buf);
            delete [] new_buf;
            break;
          }

          this->bm->write_block(block_index[i - NDIRECT],buf + offset);
          offset += BLOCK_SIZE;
          writed_bytes += BLOCK_SIZE;
        }

        // update the inode
        ino->size = size;
        this->put_inode(inum,ino);
        delete ino;
        delete [] bl_buf;
        return;
      }
    }else if(ino_block > need_block){
      // write to core file
      int writed_bytes = 0;
      int offset = 0;
      if(ino_block <= NDIRECT){
        // need to free block
        // free block
        for(int i = need_block;i < ino_block;++i){
          this->bm->free_block(ino->blocks[i]);
          ino->blocks[i] = 0;
        }

        // write block by block
        for(int i = 0;i < need_block;++i){
          // think about the situlation that the writing bytes is smaller than BLOCK_SIZE
          if(writed_bytes + BLOCK_SIZE > size){
            char* new_buf = new char[BLOCK_SIZE];
            memcpy(new_buf,buf+offset,size - writed_bytes);
            memset(new_buf+size-writed_bytes,0,writed_bytes + BLOCK_SIZE - size);
            this->bm->write_block(ino->blocks[i],new_buf);
            delete [] new_buf;
            break;
          }

          this->bm->write_block(ino->blocks[i],buf + offset);
          offset += BLOCK_SIZE;
          writed_bytes += BLOCK_SIZE;
        }

        // update the inode
        ino->size = size;
        this->put_inode(inum,ino);
        delete ino;
        return;
      }else if(need_block > NDIRECT){
        // need to free block which is indirect
        // first , read the core indirect block
        char *bl_buf = new char[BLOCK_SIZE];
        this->bm->read_block(ino->blocks[NDIRECT],bl_buf);
        blockid_t* block_index = (blockid_t *)bl_buf;
        //blockid_t* block_index = get_indirect(ino,this->bm);

        for(int i = need_block;i < ino_block;++i){
          this->bm->free_block(block_index[i - NDIRECT]);
          block_index[i - NDIRECT] = 0;
        }

        // write the block back
        this->bm->write_block(ino->blocks[NDIRECT],bl_buf);
        delete [] bl_buf;

        // write to file
        // need to write indirect block
        // write block by block
        for(int i = 0;i < NDIRECT;++i){
          // think about the situlation that the writing bytes is smaller than BLOCK_SIZE
          if(writed_bytes + BLOCK_SIZE > size){
            char* new_buf = new char[BLOCK_SIZE];
            memcpy(new_buf,buf+offset,size - writed_bytes);
            memset(new_buf+size-writed_bytes,0,writed_bytes + BLOCK_SIZE - size);
            this->bm->write_block(ino->blocks[i],new_buf);
            delete [] new_buf;
            break;
          }

          this->bm->write_block(ino->blocks[i],buf + offset);
          offset += BLOCK_SIZE;
          writed_bytes += BLOCK_SIZE;
        }

        // get indirect block
        bl_buf = new char[BLOCK_SIZE];
        this->bm->read_block(ino->blocks[NDIRECT],bl_buf);
        block_index = (blockid_t *)bl_buf;

        // write indirect block
        for(int i = NDIRECT;i < need_block;++i){
          // think about the situlation that the writing bytes is smaller than BLOCK_SIZE
          if(writed_bytes + BLOCK_SIZE > size){
            char* new_buf = new char[BLOCK_SIZE];
            memcpy(new_buf,buf+offset,size - writed_bytes);
            memset(new_buf+size-writed_bytes,0,writed_bytes + BLOCK_SIZE - size);
            this->bm->write_block(block_index[i - NDIRECT],new_buf);
            delete [] new_buf;
            break;
          }

          this->bm->write_block(block_index[i - NDIRECT],buf + offset);
          offset += BLOCK_SIZE;
          writed_bytes += BLOCK_SIZE;
        }

        // update the inode
        ino->size = size;
        this->put_inode(inum,ino);
        delete ino;
        delete [] bl_buf;
        return;
      }else{
        // first , free direct block
        for(int i = need_block;i < NDIRECT;++i){
          this->bm->free_block(ino->blocks[i]);
        }

        // second , free indirect block
        // read indirect block
        char *bl_buf = new char[BLOCK_SIZE];
        this->bm->read_block(ino->blocks[NDIRECT],bl_buf);
        blockid_t* block_index = (blockid_t *)bl_buf;

        for(int i = NDIRECT;i < ino_block;++i){
          this->bm->free_block(block_index[i - NDIRECT]);
          block_index[i - NDIRECT] = 0;
        }

        // write back
        this->bm->write_block(ino->blocks[NDIRECT],bl_buf);
        delete [] bl_buf;

        if(need_block < NDIRECT){
          this->bm->free_block(ino->blocks[NDIRECT]);
        }

        // write to core block
        // write block by block
        for(int i = 0;i < need_block;++i){
          // think about the situlation that the writing bytes is smaller than BLOCK_SIZE
          if(writed_bytes + BLOCK_SIZE > size){
            char* new_buf = new char[BLOCK_SIZE];
            memcpy(new_buf,buf+offset,size - writed_bytes);
            memset(new_buf+size-writed_bytes,0,writed_bytes + BLOCK_SIZE - size);
            this->bm->write_block(ino->blocks[i],new_buf);
            delete [] new_buf;
            break;
          }

          this->bm->write_block(ino->blocks[i],buf + offset);
          offset += BLOCK_SIZE;
          writed_bytes += BLOCK_SIZE;
        }

        // update the inode
        ino->size = size;
        this->put_inode(inum,ino);
        delete ino;
        return;
      }
    }else{
      printf("wrongin write file that need to free\n");
      return;
    }

  }
  
  return;
}

void
inode_manager::get_attr(uint32_t inum, extent_protocol::attr &a)
{
  /*
   * your code goes here.
   * note: get the attributes of inode inum.
   * you can refer to "struct attr" in extent_protocol.h
   */

  // get the messge of the inode
  inode* ino = get_inode(inum);
  if(ino == NULL){
    return;
  }else{
    a.atime = ino->atime;
    a.ctime = ino->ctime;
    a.mtime = ino->mtime;
    a.size = ino->size;
    a.type = ino->type;
    //printf("receive type is %d\n",a.type);
    delete ino;
    return;
  } 
  return;
}

void
inode_manager::remove_file(uint32_t inum)
{
  /*
   * your code goes here
   * note: you need to consider about both the data block and inode of the file
   */

  // first , clean the data block
  inode* ino = get_inode(inum);
  int need_block = ((ino->size + BLOCK_SIZE - 1)) / BLOCK_SIZE;
  if(need_block <= NDIRECT){
    for(int i = 0;i < need_block;++i){
      this->bm->free_block(ino->blocks[i]);
    }
  }
  else{
    for(int i = 0;i < NDIRECT;++i){
      this->bm->free_block(ino->blocks[i]);
    }

    // read indirect block
    char *bl_buf = new char[BLOCK_SIZE];
    this->bm->read_block(ino->blocks[NDIRECT],bl_buf);
    blockid_t* block_index = (blockid_t *)bl_buf;

    for(int i = NDIRECT;i < need_block;++i){
      this->bm->free_block(block_index[i - NDIRECT]);
    }

    this->bm->free_block(ino->blocks[NDIRECT]);
  }

  // second , free the inode
  this->free_inode(inum);
  
  return;
}
