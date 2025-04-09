#include "BlockBuffer.h"
#include <cstdlib>
#include <cstring>
#include <iostream>

// the declarations for these functions can be found in "BlockBuffer.h"

BlockBuffer::BlockBuffer(int blockNum)
{
	// initialise this.blockNum with the argument
	if (blockNum < 0 || blockNum >= DISK_BLOCKS)
		this->blockNum = E_DISKFULL;
	else
		this->blockNum = blockNum;
}

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
IndInternal::IndInternal(int blockNum):IndBuffer(blockNum){}//access a already

int BlockBuffer::getBlockNum(){
	return this->blockNum;
}

//* loads the block header into the argument pointer
int BlockBuffer::getHeader(HeadInfo *head)
{
	// reading the buffer block from cache
	// //Disk::readBlock(buffer, this->blockNum);
	unsigned char *buffer;
	int ret = loadBlockAndGetBufferPtr(&buffer);
	if (ret != SUCCESS)
		return ret;

	// TODO: populate the numEntries, numAttrs and numSlots fields in *head
	memcpy(&head->pblock, buffer + 4, 4);
	memcpy(&head->lblock, buffer + 8, 4);
	memcpy(&head->rblock, buffer + 12, 4);
	memcpy(&head->numEntries, buffer + 16, 4);
	memcpy(&head->numAttrs, buffer + 20, 4);
	memcpy(&head->numSlots, buffer + 24, 4);

	return SUCCESS;
}

int BlockBuffer::setHeader(struct HeadInfo *head){

    unsigned char *bufferPtr;
    // get the starting address of the buffer containing the block using
    // loadBlockAndGetBufferPtr(&bufferPtr).
	int ret = loadBlockAndGetBufferPtr(&bufferPtr);

    // if loadBlockAndGetBufferPtr(&bufferPtr) != SUCCESS
	if (ret != SUCCESS) return ret;

    // cast bufferPtr to type HeadInfo*
    struct HeadInfo *bufferHeader = (struct HeadInfo *)bufferPtr;

    // copy the fields of the HeadInfo pointed to by head (except reserved) to
    // the header of the block (pointed to by bufferHeader)
    //(hint: bufferHeader->numSlots = head->numSlots )
	bufferHeader->blockType = head->blockType;
	bufferHeader->lblock = head->lblock;
	bufferHeader->rblock = head->rblock;
	bufferHeader->pblock = head->pblock;
	bufferHeader->numAttrs = head->numAttrs;
	bufferHeader->numEntries = head->numEntries;
	bufferHeader->numSlots = head->numSlots;

	// memcpy(&head->pblock, bufferHeader + 4, 4);
	// memcpy(&head->lblock, bufferHeader + 8, 4);
	// memcpy(&head->rblock, bufferHeader + 12, 4);
	// memcpy(&head->numEntries, bufferHeader + 16, 4);
	// memcpy(&head->numAttrs, bufferHeader + 20, 4);
	// memcpy(&head->numSlots, bufferHeader + 24, 4);

    // update dirty bit by calling StaticBuffer::setDirtyBit()
	ret = StaticBuffer::setDirtyBit(this->blockNum);

    // if setDirtyBit() failed, return the error code
	if (ret != SUCCESS) return ret;

    return SUCCESS;
}

//* loads the record at slotNum into the argument pointer
int RecBuffer::getRecord(union Attribute *record, int slotNum)
{
	// get the header using this.getHeader() function
	HeadInfo head;
	BlockBuffer::getHeader(&head);

	int attrCount = head.numAttrs;
	int slotCount = head.numSlots;

	// read the block at this.blockNum into a buffer
	unsigned char *buffer;
	// // Disk::readBlock(buffer, this->blockNum);
	int ret = loadBlockAndGetBufferPtr(&buffer);
	if (ret != SUCCESS)
		return ret;

	//* record at slotNum will be at offset HEADER_SIZE + slotMapSize + (recordSize * slotNum)
	//     each record will have size attrCount * ATTR_SIZE
	//     slotMap will be of size slotCount
	int recordSize = attrCount * ATTR_SIZE;
	unsigned char *slotPointer = buffer + (32 + slotCount + (recordSize * slotNum)); // calculate buffer + offset

	// load the record into the rec data structure
	memcpy(record, slotPointer, recordSize);

	return SUCCESS;
}

//* load the record at slotNum into the argument pointer
int RecBuffer::setRecord(union Attribute *record, int slotNum)
{
	// read the block at this.blockNum into a buffer
	unsigned char *buffer;
	//// Disk::readBlock(buffer, this->blockNum);
	int ret = loadBlockAndGetBufferPtr(&buffer);
	if (ret != SUCCESS)
		return ret;
	
	// get the header using this.getHeader() function
	HeadInfo head;
	BlockBuffer::getHeader(&head);

	// get number of attributes in the block.
	int attrCount = head.numAttrs;

    // get the number of slots in the block.
	int slotCount = head.numSlots;

	//! if input slotNum is not in the permitted range 
	if (slotNum >= slotCount) return E_OUTOFBOUND;

	int recordSize = attrCount * ATTR_SIZE;
	unsigned char *slotPointer = buffer + (HEADER_SIZE + slotCount + (recordSize * slotNum)); // calculate buffer + offset

	// load the record into the rec data structure
	memcpy(slotPointer, record, recordSize);

	ret = StaticBuffer::setDirtyBit(this->blockNum);

	//! The above function call should not fail since the block is already
    //! in buffer and the blockNum is valid. If the call does fail, there
    //! exists some other issue in the code) 
	if (ret != SUCCESS) {
		std::cout << "There is some error in the code!\n";
		exit(1);
	}

	// // Disk::writeBlock(buffer, this->blockNum);

	return SUCCESS;
}
int BlockBuffer::loadBlockAndGetBufferPtr(unsigned char **buffPtr)
{
	// check whether the block is already present in the buffer
	// using StaticBuffer.getBufferNum()
	int bufferNum = StaticBuffer::getBufferNum(this->blockNum);
	if (bufferNum == E_OUTOFBOUND)
		return E_OUTOFBOUND;

	// if present (!=E_BLOCKNOTINBUFFER),
	// 		set the timestamp of the corresponding buffer to 0 and increment the
	// 		timestamps of all other occupied buffers in BufferMetaInfo.
	if (bufferNum != E_BLOCKNOTINBUFFER) {
		for (int bufferIndex = 0; bufferIndex < BUFFER_CAPACITY; bufferIndex++) {
			StaticBuffer::metainfo[bufferIndex].timeStamp++;
		}
		StaticBuffer::metainfo[bufferNum].timeStamp = 0;
	}
	else if (bufferNum == E_BLOCKNOTINBUFFER) // the block is not present in the buffer
	{ 
		bufferNum = StaticBuffer::getFreeBuffer(this->blockNum);

		//! no free space found in the buffer (currently)
		//! or some other error occurred in the process
		if (bufferNum == E_OUTOFBOUND || bufferNum == FAILURE)
			return bufferNum;

		Disk::readBlock(StaticBuffer::blocks[bufferNum], this->blockNum);
	}

	// store the pointer to this buffer (blocks[bufferNum]) in *buffPtr
	*buffPtr = StaticBuffer::blocks[bufferNum];

	return SUCCESS;
}

/* used to get the slotmap from a record block
NOTE: this function expects the caller to allocate memory for `*slotMap`
*/
int RecBuffer::getSlotMap(unsigned char *slotMap)
{
	unsigned char *bufferPtr;

	// get the starting address of the buffer containing the block using loadBlockAndGetBufferPtr().
	int ret = loadBlockAndGetBufferPtr(&bufferPtr);
	if (ret != SUCCESS)
		return ret;

	// get the header of the block using getHeader() function
	RecBuffer recordBlock (this->blockNum);
	struct HeadInfo head;
	recordBlock.getHeader(&head);

	int slotCount = head.numSlots;

	// get a pointer to the beginning of the slotmap in memory by offsetting HEADER_SIZE
	unsigned char *slotMapInBuffer = bufferPtr + HEADER_SIZE;

	// copy the values from `slotMapInBuffer` to `slotMap` (size is `slotCount`)
	for (int slot = 0; slot < slotCount; slot++) 
		*(slotMap+slot)= *(slotMapInBuffer+slot);

	return SUCCESS;
}

int RecBuffer::setSlotMap(unsigned char *slotMap) {
    unsigned char *bufferPtr;
    // get the starting address of the buffer containing the block using
    // loadBlockAndGetBufferPtr(&bufferPtr)
	int ret = loadBlockAndGetBufferPtr(&bufferPtr);

    // if loadBlockAndGetBufferPtr(&bufferPtr) != SUCCESS
	// return the value returned by the call.
	if (ret != SUCCESS) return ret;

    // get the header of the block using the getHeader() function
	HeadInfo blockHeader;
	getHeader(&blockHeader);

    int numSlots = blockHeader.numSlots;

    // the slotmap starts at bufferPtr + HEADER_SIZE. Copy the contents of the
    // argument `slotMap` to the buffer replacing the existing slotmap.
    // Note that size of slotmap is `numSlots`
	unsigned char *slotPointer = bufferPtr + HEADER_SIZE;
	memcpy(slotPointer, slotMap, numSlots);

    // update dirty bit using StaticBuffer::setDirtyBit
	ret = StaticBuffer::setDirtyBit(this->blockNum);

    // if setDirtyBit failed, return the value returned by the call
	if (ret != SUCCESS) return ret;

    return SUCCESS;
}

int BlockBuffer::setBlockType(int blockType){

    unsigned char *bufferPtr;
    /* get the starting address of the buffer containing the block
       using loadBlockAndGetBufferPtr(&bufferPtr). */

	int ret = loadBlockAndGetBufferPtr(&bufferPtr);

    // if loadBlockAndGetBufferPtr(&bufferPtr) != SUCCESS
        // return the value returned by the call.
	if (ret != SUCCESS) return ret;

    // store the input block type in the first 4 bytes of the buffer.
    // (hint: cast bufferPtr to int32_t* and then assign it)
    // *((int32_t *)bufferPtr) = blockType;
	(*(int32_t*) bufferPtr) = blockType;

    // update the StaticBuffer::blockAllocMap entry corresponding to the
    // object's block number to `blockType`.
	StaticBuffer::blockAllocMap[this->blockNum] = blockType;

    // update dirty bit by calling StaticBuffer::setDirtyBit()
    // if setDirtyBit() failed
        // return the returned value from the call

	ret = StaticBuffer::setDirtyBit(this->blockNum);
	if (ret != SUCCESS) return ret;

    return SUCCESS;
}

int BlockBuffer::getFreeBlock(int blockType){
    // iterate through the StaticBuffer::blockAllocMap and find the block number
    // of a free block in the disk.
	int blockNum = 0;
	for (; blockNum < DISK_BLOCKS; blockNum++) {
		if (StaticBuffer::blockAllocMap[blockNum] == UNUSED_BLK)
			break;
	}

    //! if no block is free, return E_DISKFULL.
	if (blockNum == DISK_BLOCKS) return E_DISKFULL;

    // set the object's blockNum to the block number of the free block.
	this->blockNum = blockNum;

    // find a free buffer using StaticBuffer::getFreeBuffer() .
	int bufferIndex = StaticBuffer::getFreeBuffer(blockNum);

	if (bufferIndex < 0 && bufferIndex >= BUFFER_CAPACITY) {
		printf ("Error: Buffer is full\n");
		return bufferIndex;
	}
	
    // initialize the header of the block passing a struct HeadInfo with values
    // pblock: -1, lblock: -1, rblock: -1, numEntries: 0, numAttrs: 0, numSlots: 0
    // to the setHeader() function.
	HeadInfo blockHeader;
	blockHeader.pblock = blockHeader.rblock = blockHeader.lblock = -1;
	blockHeader.numEntries = blockHeader.numAttrs = blockHeader.numSlots = 0;

	setHeader(&blockHeader);

    // update the block type of the block to the input block type using setBlockType().
	setBlockType(blockType);

    // return block number of the free block.
	return blockNum;
}

void BlockBuffer::releaseBlock()
{
    // if blockNum is INVALID_BLOCK (-1), or it is invalidated already, do nothing
	if (blockNum == INVALID_BLOCKNUM || 
		StaticBuffer::blockAllocMap[blockNum] == UNUSED_BLK)
		return;

	int bufferIndex = StaticBuffer::getBufferNum(blockNum);

	// if the block is present in the buffer, free the buffer
	// by setting the free flag of its StaticBuffer::tableMetaInfo entry
	// to true.
	if (bufferIndex >= 0 && bufferIndex < BUFFER_CAPACITY)
		StaticBuffer::metainfo[bufferIndex].free = true;

	// free the block in disk by setting the data type of the entry
	// corresponding to the block number in StaticBuffer::blockAllocMap
	// to UNUSED_BLK.
	StaticBuffer::blockAllocMap[blockNum] = UNUSED_BLK;

	// set the object's blockNum to INVALID_BLOCK (-1)
	this->blockNum = INVALID_BLOCKNUM;
}

int compareAttrs(Attribute attr1, Attribute attr2, int attrType) {
	return attrType == NUMBER ? 
		(attr1.nVal < attr2.nVal ? -1 : (attr1.nVal > attr2.nVal ? 1 : 0)) : 
		strcmp(attr1.sVal, attr2.sVal) ;
}

int IndInternal::getEntry(void *ptr, int indexNum) {
	//if index is out of the the blocks
	if (indexNum < 0 || indexNum >= MAX_KEYS_INTERNAL) 
		return E_OUTOFBOUND;

	//load the block into the main memory and get a bufferPtr to it 
    unsigned char *bufferPtr;
	int ret = loadBlockAndGetBufferPtr(&bufferPtr);
	if (ret != SUCCESS) 
		return ret;

	//typecast the void ptr to internalEntry type
    struct InternalEntry *internalEntry = (struct InternalEntry *)ptr;
    
	//get the pointer to that index using bufferPtr(i.e where the block is) + headersize + (indexnum *20)
	//20 cause it is 16 for the attrVal and 4 bytes for the pointer 
	//rchild pointer of one entry acts as the lchild ptr for the next entry
	unsigned char *entryPtr = bufferPtr + HEADER_SIZE + (indexNum * 20);
	//copy the content of the index into the internalEntry structure
	//first copy the the lchild which is 4 bytes
    memcpy(&(internalEntry->lChild), entryPtr, sizeof(int32_t));
	//copy the attrVal at +4 for 16 bytes
    memcpy(&(internalEntry->attrVal), entryPtr + 4, sizeof(Attribute));
	//copy the rchild at + 20 from entryPtr for 4 bytes
    memcpy(&(internalEntry->rChild), entryPtr + 20, 4);

    return SUCCESS;
}

int IndLeaf::getEntry(void *ptr, int indexNum) 
{
    // if the indexNum is outof bound 
	if (indexNum < 0 || indexNum >= MAX_KEYS_LEAF) return E_OUTOFBOUND;

    unsigned char *bufferPtr;
	//load the block into the main memory and get the pointer to it 
	int ret = loadBlockAndGetBufferPtr(&bufferPtr);
	if (ret != SUCCESS)
	return ret;
	//get the index position as indexNum + size of leaf entry which is 32 bytes(attrval for 16 bytes, block and slot takes 8 byes and 8 are unused)
	unsigned char *entryPtr = bufferPtr + HEADER_SIZE + (indexNum * LEAF_ENTRY_SIZE);
    memcpy((struct Index *)ptr, entryPtr, LEAF_ENTRY_SIZE);

    return SUCCESS;
}

int IndInternal::setEntry(void *ptr, int indexNum) {
	// if the indexNum is not in the valid range of [0, MAX_KEYS_INTERNAL-1]
    //     return E_OUTOFBOUND.
	if (indexNum < 0 || indexNum >= MAX_KEYS_INTERNAL) return E_OUTOFBOUND;

    unsigned char *bufferPtr;
    /* get the starting address of the buffer containing the block
       using loadBlockAndGetBufferPtr(&bufferPtr). */
	int ret = loadBlockAndGetBufferPtr(&bufferPtr);

    // if loadBlockAndGetBufferPtr(&bufferPtr) != SUCCESS
    //     return the value returned by the call.
	if (ret != SUCCESS) return ret;

    // typecast the void pointer to an internal entry pointer
    struct InternalEntry *internalEntry = (struct InternalEntry *)ptr;


    
	unsigned char *entryPtr = bufferPtr + HEADER_SIZE + (indexNum * 20);

    memcpy(entryPtr, &(internalEntry->lChild), sizeof(int32_t));
    memcpy(entryPtr + 4, &(internalEntry->attrVal), sizeof(Attribute));
    memcpy(entryPtr + 20, &(internalEntry->rChild), 4);

    return SUCCESS;
}

int IndLeaf::setEntry(void *ptr, int indexNum) {
  	// if the indexNum is not in the valid range of [0, MAX_KEYS_LEAF-1]
    //     return E_OUTOFBOUND.
	if (indexNum < 0 || indexNum >= MAX_KEYS_LEAF) return E_OUTOFBOUND;

    unsigned char *bufferPtr;
    /* get the starting address of the buffer containing the block
       using loadBlockAndGetBufferPtr(&bufferPtr). */
	int ret = loadBlockAndGetBufferPtr(&bufferPtr);


	if (ret != SUCCESS) 
		return ret;   
	unsigned char *entryPtr = bufferPtr + HEADER_SIZE + (indexNum * LEAF_ENTRY_SIZE);
    memcpy(entryPtr, (struct Index *)ptr, LEAF_ENTRY_SIZE);

    return SUCCESS;
}