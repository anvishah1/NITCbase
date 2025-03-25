#include "StaticBuffer.h"
#include <iostream>

// the declarations for this class can be found at "StaticBuffer.h"

unsigned char StaticBuffer::blocks[BUFFER_CAPACITY][BLOCK_SIZE];
struct BufferMetaInfo StaticBuffer::metainfo[BUFFER_CAPACITY];
unsigned char StaticBuffer::blockAllocMap[DISK_BLOCKS];
StaticBuffer::StaticBuffer() {
/*
so were kinda replicating the blockallocmap in the buffer so were copying
the content in the actual blockaccesmap in the disk into a fake one we created
in the buffer and cipying the content at the correct offset values bsed on the block
*/
for(int i=0;i<4;i++){
  //readblock(bufferptr,disk) bring the disk->buffer
  //Disk::readBlock(blockAllocMap+(i*BLOCK_SIZE),i);
  unsigned char buffer[BLOCK_SIZE];
  Disk::readBlock(buffer,i);
  memcpy(blockAllocMap+(i*BLOCK_SIZE),buffer,BLOCK_SIZE);
}
  for (int bufferIndex = 0; bufferIndex < BUFFER_CAPACITY; bufferIndex++) {
    metainfo[bufferIndex].free = true;
    metainfo[bufferIndex].dirty = false;
    metainfo[bufferIndex].timeStamp = -1;
    metainfo[bufferIndex].blockNum = -1;
  }
}


StaticBuffer::~StaticBuffer() {
    for(int i=0;i<4;i++){
      //write back the content of the replica blockallocmap in the buffer to the actual disk blockaccessmap
      unsigned char buffer[BLOCK_SIZE];
      memcpy(buffer,blockAllocMap+(i*BLOCK_SIZE),BLOCK_SIZE);
      Disk::writeBlock(buffer,i);
        //  Disk::writeBlock(blockAllocMap+(i*BLOCK_SIZE),i);
        }
    for(int bufferIndex = 0 ; bufferIndex < BUFFER_CAPACITY ; bufferIndex++){
        if(metainfo[bufferIndex].free == false and metainfo[bufferIndex].dirty == true){
            //writeback whatever it at the blocks[bufferindex] to the blocknum
            Disk::writeBlock(StaticBuffer::blocks[bufferIndex], metainfo[bufferIndex].blockNum);
        }

    }
}


int StaticBuffer::getFreeBuffer(int blockNum) {
  if (blockNum < 0 || blockNum >= DISK_BLOCKS) {
    return E_OUTOFBOUND;
  }
  int allocatedBuffer = -1, BufferwithMaxTimeStamp = -1, maxTimeStamp = -1;

  // iterate through all the blocks in the StaticBuffer
  // find the first free block in the buffer (check metainfo)
  // assign allocatedBuffer = index of the free block
  for(int i = 0 ; i < BUFFER_CAPACITY ; i++){
    if(metainfo[i].free == true and allocatedBuffer == -1){
        allocatedBuffer = i;
    }

    if(metainfo[i].free == false){
        metainfo[i].timeStamp += 1;

        if(maxTimeStamp < metainfo[i].timeStamp){
            maxTimeStamp = metainfo[i].timeStamp;
            BufferwithMaxTimeStamp = i;
        }
    }
  }

  if(allocatedBuffer == -1){
    if(metainfo[BufferwithMaxTimeStamp].dirty == true){
        Disk::writeBlock(StaticBuffer::blocks[BufferwithMaxTimeStamp],metainfo[BufferwithMaxTimeStamp].blockNum);
    }
    allocatedBuffer = BufferwithMaxTimeStamp;
  }

  metainfo[allocatedBuffer].free = false;
  metainfo[allocatedBuffer].dirty = false;
  metainfo[allocatedBuffer].blockNum = blockNum;
  metainfo[allocatedBuffer].timeStamp = 0;

  return allocatedBuffer;
}

int StaticBuffer::getBufferNum(int blockNum) {
  if(blockNum<0 || blockNum>DISK_BLOCKS){
    return E_OUTOFBOUND;
  }

  for(int i = 0 ; i < BUFFER_CAPACITY ; i++){
    if(metainfo[i].blockNum == blockNum and metainfo[i].free == false){
        return i;
    }
  }

  return E_BLOCKNOTINBUFFER;
}


int StaticBuffer::setDirtyBit(int blockNum){
   

    int buffNum = StaticBuffer::getBufferNum(blockNum);

    if(buffNum == E_BLOCKNOTINBUFFER){
        return E_BLOCKNOTINBUFFER;
    }

    if(buffNum == E_OUTOFBOUND){
        return E_OUTOFBOUND;
    }

    metainfo[buffNum].dirty = true;

    return SUCCESS;

}

//function to return the type of the block which will be used later in bptree search
int StaticBuffer::getStaticBlockType(int blockNum){
  if(blockNum ==0 || blockNum > DISK_BLOCKS)
    return E_OUTOFBOUND;

   return (int)blockAllocMap[blockNum];
}