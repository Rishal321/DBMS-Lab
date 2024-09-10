#include "StaticBuffer.h"

unsigned char StaticBuffer::blocks[BUFFER_CAPACITY][BLOCK_SIZE];
struct BufferMetaInfo StaticBuffer::metainfo[BUFFER_CAPACITY];
unsigned char StaticBuffer::blockAllocMap[DISK_BLOCKS];


StaticBuffer::StaticBuffer(){
  
  int blockmapsslot=0;
  for(int i=0;i<=3;i++){
    unsigned char bufr[BLOCK_SIZE];
     Disk::readBlock(bufr,i);
     for(int slot=0;slot<BLOCK_SIZE;slot++){
        blockAllocMap[blockmapsslot]=bufr[slot];
        blockmapsslot++;
     }
  }

  for (int bufferIndex = 0; bufferIndex < BUFFER_CAPACITY; bufferIndex++) {
		metainfo[bufferIndex].free = true;
		metainfo[bufferIndex].dirty = false;
		metainfo[bufferIndex].timeStamp = -1;
		metainfo[bufferIndex].blockNum = -1;
	}

}

StaticBuffer::~StaticBuffer(){
   
  int blockmapsslot=0;
  for(int i=0;i<=3;i++){
    unsigned char bufr[BLOCK_SIZE];
     for(int slot=0;slot<BLOCK_SIZE;slot++){
        bufr[slot]=StaticBuffer:: blockAllocMap[blockmapsslot];
        blockmapsslot++;
     }
     Disk::writeBlock(bufr,i);
  }

  for (int bufferIndex = 0; bufferIndex < BUFFER_CAPACITY; bufferIndex++) {
    if(metainfo[bufferIndex].free==false and metainfo[bufferIndex].dirty==true){
      Disk::writeBlock(blocks[bufferIndex],metainfo[bufferIndex].blockNum);
    }
  }
}

int StaticBuffer::getFreeBuffer(int blockNum){
   
  if (blockNum < 0 || blockNum > DISK_BLOCKS) {
    return E_OUTOFBOUND;
  }
  
  int allocatedBuffer=-1;
  int timeStamp=0,maxindex=0;

    for(int i=0;i<BUFFER_CAPACITY;i++){

      if(metainfo[i].timeStamp>timeStamp){
      timeStamp=metainfo[i].timeStamp;
      maxindex=i;
      }

      if(metainfo[i].free){
            allocatedBuffer=i;
            break;
      }
    }

  if(allocatedBuffer==-1){
    if(metainfo[maxindex].dirty==true){
      Disk::writeBlock(blocks[maxindex],metainfo[maxindex].blockNum);
      allocatedBuffer=maxindex;
    }  
  }
  
  metainfo[allocatedBuffer].free = false, 
	metainfo[allocatedBuffer].dirty = false,
	metainfo[allocatedBuffer].timeStamp = 0, 
	metainfo[allocatedBuffer].blockNum = blockNum;

 return allocatedBuffer;
}

int StaticBuffer::getBufferNum(int blockNum){
  if (blockNum < 0 || blockNum > DISK_BLOCKS) {
    return E_OUTOFBOUND;
    }
  
  for(int i=0;i<BUFFER_CAPACITY;i++){
        if(metainfo[i].blockNum==blockNum){
            return i;
        }
    }

  return E_BLOCKNOTINBUFFER;   
}

int StaticBuffer::setDirtyBit(int blockNum){
	int bufferIndex = getBufferNum(blockNum);

	if (bufferIndex == E_BLOCKNOTINBUFFER)
        return E_BLOCKNOTINBUFFER;

	if (bufferIndex == E_OUTOFBOUND)
        return E_OUTOFBOUND;

	metainfo[bufferIndex].dirty = true;

 return SUCCESS;
}