#include "BlockBuffer.h"
#include <iostream>
#include <cstdlib>
#include <cstring>

BlockBuffer::BlockBuffer(int blockNum){
    this->blockNum=blockNum;
}

BlockBuffer::BlockBuffer(char blockType){

   int blockNum= getFreeBlock(blockType);
   if (blockNum < 0 || blockNum >= DISK_BLOCKS) {
		std::cout << "Error: Block is not available\n";
		this->blockNum = blockNum;
		return;
	}
    this->blockNum = blockNum;

}

RecBuffer::RecBuffer(int blockNum) :BlockBuffer::BlockBuffer(blockNum){}
RecBuffer::RecBuffer() : BlockBuffer('R'){}

int BlockBuffer::getHeader(struct HeadInfo * head){

  unsigned char *bufferPtr;
  int ret = loadBlockAndGetBufferPtr(&bufferPtr);
  if (ret != SUCCESS) {
    return ret;   
  }
   
   memcpy(&head->pblock,bufferPtr+4,4);
   memcpy(&head->lblock,bufferPtr+8,4);
   memcpy(&head->rblock,bufferPtr+12,4);
   memcpy(&head->numEntries,bufferPtr+16,4);
   memcpy(&head->numAttrs,bufferPtr+20,4);
   memcpy(&head->numSlots,bufferPtr+24,4);

return SUCCESS;
}

int BlockBuffer::setHeader(struct HeadInfo * head){

  unsigned char *bufferPtr;
  int ret = loadBlockAndGetBufferPtr(&bufferPtr);
  if (ret != SUCCESS) {
    return ret;   
  }

  struct HeadInfo *bufferHeader = (struct HeadInfo *)bufferPtr;
   
  bufferHeader->blockType = head->blockType;
	bufferHeader->lblock = head->lblock;
	bufferHeader->rblock = head->rblock;
	bufferHeader->pblock = head->pblock;
	bufferHeader->numAttrs = head->numAttrs;
	bufferHeader->numEntries = head->numEntries;
	bufferHeader->numSlots = head->numSlots;
  
  ret = StaticBuffer::setDirtyBit(this->blockNum);
  if (ret != SUCCESS) return ret;

return SUCCESS;
}

int RecBuffer::getRecord(union Attribute *rec ,int slotNum){

  struct HeadInfo head;
  BlockBuffer::getHeader(&head);

  int attrCount = head.numAttrs;
  int slotCount = head.numSlots;

  unsigned char *bufferPtr;
  int ret = loadBlockAndGetBufferPtr(&bufferPtr);
  if (ret != SUCCESS) {
    return ret;   
  }

  int recordSize = attrCount * ATTR_SIZE;
  unsigned char *slotPointer = bufferPtr + (HEADER_SIZE + slotCount + (recordSize * slotNum));
  
  memcpy(rec,slotPointer,recordSize);

  return SUCCESS;
}

int RecBuffer::setRecord(union Attribute *rec ,int slotNum){

  struct HeadInfo head;
  BlockBuffer::getHeader(&head);

  int attrCount = head.numAttrs;
  int slotCount = head.numSlots;

  unsigned char *bufferPtr;
  int ret = loadBlockAndGetBufferPtr(&bufferPtr);
  if (ret != SUCCESS) {
    return ret;   
  }

  int recordSize = attrCount * ATTR_SIZE;
  unsigned char *slotPointer = bufferPtr + (HEADER_SIZE + slotCount + (recordSize * slotNum));
  
  memcpy(slotPointer,rec,recordSize);

  ret = StaticBuffer::setDirtyBit(this->blockNum);
  if (ret != SUCCESS) return ret; 

  return SUCCESS;
}

int BlockBuffer::loadBlockAndGetBufferPtr(unsigned char **buffPtr ){
   
  int bufferNum = StaticBuffer::getBufferNum(this->blockNum);

 if (bufferNum == E_OUTOFBOUND) {
      return E_OUTOFBOUND;
    }

 if (bufferNum != E_BLOCKNOTINBUFFER) {
		for (int bufferIndex = 0; bufferIndex < BUFFER_CAPACITY; bufferIndex++) {
			StaticBuffer::metainfo[bufferIndex].timeStamp++;
		}
		StaticBuffer::metainfo[bufferNum].timeStamp = 0; 
  }

  if (bufferNum == E_BLOCKNOTINBUFFER) {

     bufferNum = StaticBuffer::getFreeBuffer(this->blockNum);

    if (bufferNum == E_OUTOFBOUND) {
      return E_OUTOFBOUND;
    }

    Disk::readBlock(StaticBuffer::blocks[bufferNum], this->blockNum);

  }
  
  *buffPtr = StaticBuffer::blocks[bufferNum];

  return SUCCESS;
}

int RecBuffer::getSlotMap(unsigned char *slotMap)
{
	unsigned char *bufferPtr;
	int ret = loadBlockAndGetBufferPtr(&bufferPtr);
	if (ret != SUCCESS)
		return ret;

	RecBuffer recordBlock (this->blockNum);
	struct HeadInfo head;
	recordBlock.getHeader(&head);

	int slotCount = head.numSlots;

	unsigned char *slotMapInBuffer = bufferPtr + HEADER_SIZE;

	for (int slot = 0; slot < slotCount; slot++) 
		*(slotMap+slot)= *(slotMapInBuffer+slot);

	return SUCCESS;
}

int RecBuffer::setSlotMap(unsigned char *slotMap){
	
  unsigned char *bufferPtr;
	int ret = loadBlockAndGetBufferPtr(&bufferPtr);
	if (ret != SUCCESS)
		return ret;

	RecBuffer recordBlock (this->blockNum);
	struct HeadInfo head;
	recordBlock.getHeader(&head);

	int slotCount = head.numSlots;

	unsigned char *slotMapInBuffer = bufferPtr + HEADER_SIZE;

	for (int slot = 0; slot < slotCount; slot++) 
		 *(slotMapInBuffer+slot)=*(slotMap+slot);

  ret = StaticBuffer::setDirtyBit(this->blockNum);
  if (ret != SUCCESS) return ret; 

 return SUCCESS;
}

int compareAttrs(Attribute attr1, Attribute attr2, int attrType){

    double diff;

    if (attrType == STRING){
        diff = strcmp(attr1.sVal, attr2.sVal);
    }
    else{
        diff = attr1.nVal - attr2.nVal;
    }
    
    if (diff > 0)
        return 1;
    if (diff < 0)
        return -1;
    return 0;
}

int BlockBuffer::setBlockType(int blockType){

    unsigned char *bufferPtr;
   int ret = loadBlockAndGetBufferPtr(&bufferPtr);
    if (ret != SUCCESS) {
    return ret;
    }

    *((int32_t *)bufferPtr) = blockType;

     StaticBuffer::blockAllocMap [this->blockNum]=blockType;

     return StaticBuffer ::setDirtyBit(this->blockNum);
    
}

int BlockBuffer::getFreeBlock(int blockType){

    
  int blockNum;

  for(blockNum=0;blockNum<DISK_BLOCKS;blockNum++){
      if(StaticBuffer::blockAllocMap[blockNum]==UNUSED_BLK)
       break;
  }

  if(blockNum==DISK_BLOCKS)
  return E_DISKFULL;

  this->blockNum=blockNum;

  int bufferNum=StaticBuffer::getFreeBuffer(blockNum);

  struct HeadInfo header;
  header.lblock = header.pblock = header.rblock = -1;
  header.numAttrs = header.numEntries = header.numSlots = 0;
 
  setHeader(&header);
  setBlockType(blockType);

  return blockNum;
}

int BlockBuffer::getBlockNum(){
    return(this->blockNum);
}

void BlockBuffer::releaseBlock()
{
	if (blockNum == INVALID_BLOCKNUM || 
		StaticBuffer::blockAllocMap[blockNum] == UNUSED_BLK)
		return;

	int bufferIndex = StaticBuffer::getBufferNum(blockNum);

	if (bufferIndex >= 0 && bufferIndex < BUFFER_CAPACITY)
		StaticBuffer::metainfo[bufferIndex].free = true;
    
  StaticBuffer::blockAllocMap[blockNum] = UNUSED_BLK;
	this->blockNum = INVALID_BLOCKNUM;

}