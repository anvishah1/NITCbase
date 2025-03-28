#include "BlockBuffer.h"

#include <cstdlib>
#include <cstring>

BlockBuffer::BlockBuffer(char blockType){
  //if the type is rec then assign that as blocktype and then call the getfreeblock
  int type;
  if (blockType=='R')
    type=REC;
  else if(blockType =='L')
    type=IND_LEAF;
  else if(blockType== 'I')
    type=IND_INTERNAL;
  else
    type=UNUSED_BLK;

  int blocknum=getFreeBlock(type);
  this->blockNum=blocknum;
  }

BlockBuffer::BlockBuffer(int blockNum) {
  this->blockNum = blockNum;
}

RecBuffer::RecBuffer() : BlockBuffer('R') {}
RecBuffer::RecBuffer(int blockNum) : BlockBuffer::BlockBuffer(blockNum) {}
IndBuffer::IndBuffer(char blockType) : BlockBuffer(blockType){}//constructor called if a new index block of the blocktype either leaf index or internal has to be allocated
IndBuffer::IndBuffer(int blockNum) :BlockBuffer(blockNum){}//called when a block has allready been initalised as an index block
IndLeaf::IndLeaf():IndBuffer('L'){}// the L tells that its a leafindex block and it is used to allocate a new leaf index block
IndLeaf::IndLeaf(int blockNum) : IndBuffer(blockNum){}//called when the block is already initialised as leaf
IndInternal::IndInternal():IndBuffer('I'){}//to allocate a new block of type internalindex
IndInternal::IndInternal(int blockNum):IndBuffer(blockNum){}//access a already existing internal index block


/*-----------------GETHEADER---------------------*/
// load the block header into the argument pointer
int BlockBuffer::getHeader(struct HeadInfo *head) {
  //unsigned char buffer[BLOCK_SIZE];
    unsigned char *bufferPtr;
    int ret = loadBlockAndGetBufferPtr(&bufferPtr);
    while(ret != SUCCESS){
        return ret;
    }
  // read the block at this.blockNum into the buffer
  //Disk::readBlock(buffer,this->blockNum);

  // populate the numEntries, numAttrs and numSlots fields in *head
  memcpy(&head->numSlots, bufferPtr + 24, 4);
  memcpy(&head->numEntries, bufferPtr + 16, 4);
  memcpy(&head->numAttrs, bufferPtr + 20, 4);
  memcpy(&head->rblock, bufferPtr + 12, 4);
  memcpy(&head->lblock, bufferPtr + 8, 4);

  return SUCCESS;
}


/*---------------------SETHEADER-----------------------*/
/*
set header will populate the head of the newly assigned block
it gets the target address i.e the pointer to the head of the block using the loadbuffer
and then one by one copied the content in the headinfo to the block
*/
int BlockBuffer::setHeader(struct HeadInfo *head){
  //get the address of the block's head
  unsigned char *bufferPtr;
  int ret=loadBlockAndGetBufferPtr(&bufferPtr);
  if(ret!=SUCCESS)
    return ret;
//this tells the system that the data pointed by bufferptr is not just raw data but should be treated in the format of headinfo structure
//then we store it in the bufferheader
    struct HeadInfo *bufferHeader=(struct HeadInfo *)bufferPtr;
    //copy the content from the headinfo to the actual head of the block
    //except for reserved
    bufferHeader->blockType=head->blockType;
    bufferHeader->lblock=head->lblock;
    bufferHeader->numAttrs=head->numAttrs;
    bufferHeader->numEntries=head->numEntries;
    bufferHeader->numSlots=head->numSlots;
    bufferHeader->pblock=head->pblock;
    bufferHeader->rblock=head->rblock;

//update the dirtybit so that the system knows to writeback thw block
int check=StaticBuffer::setDirtyBit(this->blockNum);
Disk::writeBlock(bufferPtr, this->blockNum);
return check;
}

/*------------SETSLOTMAP----------------------*/
int RecBuffer::setSlotMap(unsigned char *SlotMap){
  //bufferPtr points to the the buffer in which the block is placed into
  unsigned char *bufferPtr;
  int ret=loadBlockAndGetBufferPtr(&bufferPtr);
  if(ret!=SUCCESS)
    return ret;
//get the header info of the block
    struct HeadInfo head;
    getHeader(&head);
  
    int numSlots=head.numSlots;
    //copy the contents of the new SlotMap into the actual slotMap of the block
    memcpy(bufferPtr+HEADER_SIZE,SlotMap,numSlots);

   int check= StaticBuffer::setDirtyBit(this->blockNum);
   Disk::writeBlock(bufferPtr, this->blockNum);
    return check;
}

/*------------------------------SETBLOCKTYPE----------------------*/
//update the blocktype in the header of the block and the blockallocmap
int BlockBuffer::setBlockType(int blockType){
  unsigned char *bufferPtr;
  int ret=loadBlockAndGetBufferPtr(&bufferPtr);
  if(ret!=SUCCESS)
    return ret;
  //updating the blocktype in the header of the block
  //casting the bufferPtr and input the block type in the first 4 bytes of the buffer
  *((int32_t *)bufferPtr)=blockType;

//update the blocktype in the blockallocationmap
  StaticBuffer::blockAllocMap[this->blockNum]=blockType;

//update the changes
  int check=StaticBuffer::setDirtyBit(this->blockNum);

    return check;
}


/*----------------GETFREEBLOCK-----------------*/ 
int BlockBuffer::getFreeBlock(int blockType){
  //find an empty block in the buffer
  int freeblockindex=-1;
  for(int i=0;i<DISK_BLOCKS;i++){
    if(StaticBuffer::blockAllocMap[i]==UNUSED_BLK){
      freeblockindex=i;
      break;
    }
  }
  //if no block is free
  if(freeblockindex==DISK_BLOCKS)
    return E_DISKFULL;

//assigning the new block ka number to this->blocknum
this->blockNum=freeblockindex;
//allocate the new block into the buffer
   int buffernum=StaticBuffer::getFreeBuffer(freeblockindex);
  
  //to set the header of this new block
  //using the *head and no *head makes a differnece so dont use that
  struct HeadInfo head;
  head.pblock=-1;
  head.lblock=-1;
  head.rblock=-1;
  head.numEntries=0;
  head.numAttrs=0;
  head.numSlots=0;

  //set the header and the blocktype
  BlockBuffer::setHeader(&head);
  BlockBuffer::setBlockType(blockType);

return freeblockindex;
}

/*---------------------GETRECORD------------------*/
// load the content of the slotnum into an attribute record using the slotpointer calculated by headersize+bufferptr+slotcount+recordsize*slot num
int RecBuffer::getRecord(union Attribute *rec, int slotNum) {
  struct HeadInfo head;

  // get the header using this.getHeader() function
  this->getHeader(&head);

  int attrCount = head.numAttrs;
  int slotCount = head.numSlots;

// load block and get the bufferptr
  unsigned char *bufferPtr;
  int ret = loadBlockAndGetBufferPtr(&bufferPtr);
  while(ret != SUCCESS){
    return ret;
  }

  /* record at slotNum will be at offset HEADER_SIZE + slotMapSize + (recordSize * slotNum)
     - each record will have size attrCount * ATTR_SIZE
     - slotMap will be of size slotCount
  */
 int slotMapSize = slotCount;
  int recordSize = attrCount * ATTR_SIZE;
  int offset = HEADER_SIZE + slotMapSize + (recordSize * slotNum);
  unsigned char *slotPointer = bufferPtr + offset;
  //copy the content at the slotpointer into the record for recordSize bytes
  memcpy(rec, slotPointer, recordSize);

  return SUCCESS;
}

/*---------------------SETRECORD--------------------*/
int RecBuffer::setRecord(union Attribute *rec, int slotNum) {
//load the block and get the buffer ptr
  unsigned  char *bufferPtr;
  int ret = BlockBuffer::loadBlockAndGetBufferPtr(&bufferPtr);

  if(ret != SUCCESS){
    return ret;
  }
  //get header
  struct HeadInfo head;
  BlockBuffer::getHeader(&head);
  int attrCount = head.numAttrs;
  int slotCount = head.numSlots;

  if(slotNum < 0 || slotNum >= slotCount){
    return E_OUTOFBOUND;
  }
  /* record at slotNum will be at offset HEADER_SIZE + slotMapSize + (recordSize * slotNum)
     - each record will have size attrCount * ATTR_SIZE
     - slotMap will be of size slotCount
  */
 int slotMapSize = slotCount;
  int recordSize = attrCount * ATTR_SIZE;
  int offset = HEADER_SIZE + slotMapSize + (recordSize * slotNum);
  unsigned char *slotPointer = bufferPtr + offset;

  // copy the content of the rec into the slotpointer for recordsize bytes
  memcpy(slotPointer, rec, recordSize);
  //Disk::writeBlock(buffer,this->blockNum);

//mark that bufferindex as occupied
    StaticBuffer::setDirtyBit(this->blockNum);

Disk::writeBlock(bufferPtr, this->blockNum);
  return SUCCESS;
}

/*--------------LOADBLOCKANDGETBUFFERPTR-----------------*/
int BlockBuffer::loadBlockAndGetBufferPtr(unsigned char **buffPtr) {
  //check if the block is already in the pointer
  int bufferNum = StaticBuffer::getBufferNum(this->blockNum);
//if block is in the buffer
  if (bufferNum != E_BLOCKNOTINBUFFER) {
 //increase the timestamp for all
    for (int bufferIndex = 0; bufferIndex < BUFFER_CAPACITY; bufferIndex++) {
      StaticBuffer::metainfo[bufferIndex].timeStamp++;
    }
    //that block has been accessed so reset the timestamp to 0
    StaticBuffer::metainfo[bufferNum].timeStamp = 0;
  } else {
    //if the block is not already in the buffer
    bufferNum = StaticBuffer::getFreeBuffer(this->blockNum);
    if (bufferNum == E_OUTOFBOUND) {
      return E_OUTOFBOUND; 
    }
    //load the block from blocknum to the buffer
    Disk::readBlock(StaticBuffer::blocks[bufferNum], this->blockNum);
  }
  //pointer to the buffer in which the block has been loaded
  *buffPtr=StaticBuffer::blocks[bufferNum];
  return SUCCESS;
}

/*----------------GETSLOTMAP---------------------*/
int RecBuffer::getSlotMap(unsigned char *slotMap) {
  unsigned char *bufferPtr;
  // get the starting address of the buffer containing the block using loadBlockAndGetBufferPtr().
  int ret = loadBlockAndGetBufferPtr(&bufferPtr);
  if (ret != SUCCESS) {
    return ret;
  }
  struct HeadInfo head;
  BlockBuffer::getHeader(&head);
  // get the header of the block using getHeader() function
  int slotCount = head.numSlots;
  // get a pointer to the beginning of the slotmap in memory by offsetting HEADER_SIZE
  unsigned char *slotMapInBuffer = bufferPtr + HEADER_SIZE;
 // copy the values from `slotMapInBuffer` to `slotMap` (size is `slotCount`)
  memcpy(slotMap, slotMapInBuffer, slotCount);


  return SUCCESS;
}
/*----------GETBLOCKNUM-----------*/
int BlockBuffer::getBlockNum(){
  return this->blockNum;
}
/*----------------COMPARE--------------------*/
int compareAttrs(union Attribute attr1, union Attribute attr2, int attrType) {
    double diff;
   if(attrType == STRING){
    diff = strcmp(attr1.sVal, attr2.sVal);
   }
   else{
    diff = attr1.nVal - attr2.nVal;
   }

    if(diff < 0){
        return -1;
    }
    else if(diff > 0){
        return 1;
    }
    else{
        return 0;
    }
}
/*-----------------RELEASEBLOCK--------------*/
void::BlockBuffer::releaseBlock(){
if(this->blockNum==-1)
  return;
  else{
    int ret=StaticBuffer::getBufferNum(this->blockNum);
    if(ret!=E_BLOCKNOTINBUFFER){
      StaticBuffer::metainfo[ret].free=true;
    }
    StaticBuffer::blockAllocMap[this->blockNum]=UNUSED_BLK;
    this->blockNum=-1;
  }
   
}

int IndInternal::getEntry(void *ptr,int indexNum){
//we use this->blocknum 
  if(indexNum < 0 || indexNum > MAX_KEYS_INTERNAL){
    return E_OUTOFBOUND;
  }

  unsigned char *bufferPtr;
  //load the block into the buffer 
  int ret=loadBlockAndGetBufferPtr(&bufferPtr);
  if(ret!=SUCCESS)
    return ret;

//typecasting the place where well store the values form the index entry as internalEntry
struct InternalEntry *internalentry=(struct InternalEntry *)ptr;

// the entrypoint for the buffer where the entry is stored is gonna be the starting of the buffer + the headersize + that index position *20
//its 20 cause its the attributeval size which is 16 + the 4 bytes taken by the pointer to left/right child
//only one cause the right child pointer can act as the left child pointer for the next one 
unsigned char *entryPtr= bufferPtr + HEADER_SIZE +(indexNum * 20);

memcpy(&(internalentry->lChild),entryPtr,sizeof(int32_t));// the structure ka lchild will have the pointer to the lchild that the entry is pointing to which is 4 bytes
memcpy(&(internalentry->attrVal),entryPtr+4,sizeof(Attribute));
memcpy(&(internalentry->rChild),entryPtr+20,sizeof(int32_t));
return SUCCESS;
}

int IndLeaf::getEntry(void *ptr,int indexNum){
    if(indexNum < 0 || indexNum > MAX_KEYS_INTERNAL){
    return E_OUTOFBOUND;
  }
    unsigned char *bufferPtr;
  //load the block into the buffer 
  int ret=loadBlockAndGetBufferPtr(&bufferPtr);
  if(ret!=SUCCESS)
    return ret;

//get the location of the entry of the leaf index at the indexnum of that block
//size of leaf index entry is 32bytes.
unsigned char *entryPtr=bufferPtr+ HEADER_SIZE+(indexNum * LEAF_ENTRY_SIZE);
memcpy((struct Index *)ptr,entryPtr,LEAF_ENTRY_SIZE);
return SUCCESS;
}

//added to avoid compilation errors rn we only want to get the entries 
int IndInternal::setEntry(void *ptr, int indexNum) {
  if(indexNum < 0 || indexNum > MAX_KEYS_INTERNAL)
    return E_OUTOFBOUND;
  
  unsigned char *bufferPtr;
  int ret=loadBlockAndGetBufferPtr(&bufferPtr);
  if(ret!=SUCCESS)
    return ret;

  struct InternalEntry *internalentry=(struct InternalEntry *)ptr;

  unsigned char *entryPtr= bufferPtr+ HEADER_SIZE+ (indexNum * 20);
  memcpy(entryPtr,&(internalentry->lChild),4);
  memcpy(entryPtr + 4,&(internalentry->attrVal),16);
  memcpy(entryPtr+20,&(internalentry->rChild),4);
  int rev=StaticBuffer::setDirtyBit(this->blockNum);
  if(rev!=SUCCESS)
    return rev;

  return SUCCESS;
}

int IndLeaf::setEntry(void *ptr, int indexNum) {
  if(indexNum < 0 || indexNum > MAX_KEYS_LEAF)
    return E_OUTOFBOUND;
  
  unsigned char *bufferPtr;
  int ret=loadBlockAndGetBufferPtr(&bufferPtr);
  if(ret!=SUCCESS)
  return ret;

  unsigned char *entryPtr=bufferPtr + HEADER_SIZE + (indexNum * LEAF_ENTRY_SIZE);
  memcpy(entryPtr,(struct index *)ptr,LEAF_ENTRY_SIZE);

  int ret=StaticBuffer::setDirtyBit(this->blockNum);
  if(ret!=SUCCESS)
    return ret;
  
  return SUCCESS;
}