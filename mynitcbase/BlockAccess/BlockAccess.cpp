#include "BlockAccess.h"
#include<iostream>
#include <cstring>


RecId BlockAccess::linearSearch(int relId, char attrName[ATTR_SIZE], union Attribute attrVal, int op)
{
  //get prev searchindex
	RecId prevRecId;
	RelCacheTable::getSearchIndex(relId, &prevRecId);

	int block = -1, slot = -1;

//recid-1-1 then start from firstblk of that relationelse slot ++  |||get the block and slot of record for starting
	if (prevRecId.block == -1 && prevRecId.slot == -1)
	{
		RelCatEntry relCatBuffer;
		RelCacheTable::getRelCatEntry(relId, &relCatBuffer);

		block = relCatBuffer.firstBlk, slot = 0;
	}
	else
	{
		block = prevRecId.block, slot = prevRecId.slot + 1;
	}



	while (block != -1){//searching in each slot using slot map 
		RecBuffer blockBuffer(block);
		HeadInfo blockHeader;
		blockBuffer.getHeader(&blockHeader); //for get numslot to assign slotmap arr

		unsigned char slotMap[blockHeader.numSlots];
		blockBuffer.getSlotMap(slotMap);

		if (slot >= blockHeader.numSlots)
		{  
			block = blockHeader.rblock, slot = 0;
			continue; 
		}
		if (slotMap[slot] == SLOT_UNOCCUPIED)
		{ 
			slot++;
			continue;
		}

		Attribute record[blockHeader.numAttrs];// get record of slot
		blockBuffer.getRecord(record, slot); 

		AttrCatEntry attrCatBuffer;
		AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatBuffer);//getattrcatentry for attroffset and attrtype

		int attrOffset = attrCatBuffer.offset;

		int cmpVal = compareAttrs(record[attrOffset], attrVal, attrCatBuffer.attrType);  //compare record and attr val

		
		if (
			(op == NE && cmpVal != 0) || 
			(op == LT && cmpVal < 0) ||	 
			(op == LE && cmpVal <= 0) ||
			(op == EQ && cmpVal == 0) || 
			(op == GT && cmpVal > 0) ||	
			(op == GE && cmpVal >= 0)	 
		)//if condition satisfies then setsearch index return the block and slot
		{
			RecId newRecId = {block, slot};
			RelCacheTable::setSearchIndex(relId, &newRecId);
       
		 return RecId{block, slot};
		}

		slot++;
	}

	RelCacheTable::resetSearchIndex(relId);
	return RecId{-1, -1};
}


int BlockAccess::renameRelation(char oldName[ATTR_SIZE], char newName[ATTR_SIZE]){
    
    //ls for new relation exit or not in relcat here we want relnotexit
    RelCacheTable::resetSearchIndex(RELCAT_RELID) ;//resetsearch index
    Attribute newRelationName;    
    strcpy(newRelationName.sVal,newName);
	  char attrname[]=RELCAT_ATTR_RELNAME;
    
    RecId relCatRecId=BlockAccess::linearSearch(RELCAT_RELID,attrname,newRelationName,EQ);
    if(relCatRecId.block!=-1 && relCatRecId.slot!=-1)
    return E_RELEXIST;

   //similarly for oldname here we want relexit
    RelCacheTable::resetSearchIndex(RELCAT_RELID) ; 
    Attribute oldRelationName;    
    strcpy(oldRelationName.sVal,oldName);

    relCatRecId=BlockAccess::linearSearch(RELCAT_RELID,attrname,oldRelationName,EQ);
    if(relCatRecId.block ==-1 && relCatRecId.slot==-1)
    return E_RELNOTEXIST;
  
  //change relname in relcat  first get record of oldrename in relcat  and change setrecord
  RecBuffer Buffer(relCatRecId.block);
 	Attribute catRecord[RELCAT_NO_ATTRS];
 	Buffer.getRecord(catRecord,relCatRecId.slot);//we got relid from above ls for oldrelname
 	strcpy(catRecord[RELCAT_REL_NAME_INDEX].sVal,newName);
 	Buffer.setRecord(catRecord,relCatRecId.slot);
   
  //similary change in attrcat 
	RelCacheTable::resetSearchIndex(ATTRCAT_RELID);

    for(int i=0;i<catRecord[RELCAT_NO_ATTRIBUTES_INDEX].nVal;i++){
		    char attn[]=ATTRCAT_ATTR_RELNAME;
        relCatRecId=BlockAccess::linearSearch(ATTRCAT_RELID,attn,oldRelationName,EQ);
         
        RecBuffer attrCatBlock(relCatRecId.block);
        Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
        attrCatBlock.getRecord(attrCatRecord,relCatRecId.slot);
        strcpy(attrCatRecord[ATTRCAT_REL_NAME_INDEX].sVal,newName);
        attrCatBlock.setRecord(attrCatRecord,relCatRecId.slot);

    }

    return SUCCESS;
}

int BlockAccess::renameAttribute(char relName[ATTR_SIZE], char oldName[ATTR_SIZE], char newName[ATTR_SIZE]) {
  
   //ls for new relation exit or not in relcat here we want relexit
    RelCacheTable::resetSearchIndex(RELCAT_RELID);
    Attribute relNameAttr;   
    strcpy(relNameAttr.sVal,relName);
    char attrname[]=RELCAT_ATTR_RELNAME;

    RecId relcatRecId = BlockAccess::linearSearch(RELCAT_RELID,attrname, relNameAttr, EQ) ;
    if (relcatRecId.block == -1 and relcatRecId.slot == -1)
    return E_RELNOTEXIST;
    
    RelCacheTable::resetSearchIndex(ATTRCAT_RELID);

    RecId attrToRenameRecId{-1, -1};
    Attribute attrCatEntryRecord[ATTRCAT_NO_ATTRS];


    while (true) {
     // ls in attrcat search relname ,then get id ofattr of that rel 
      char attn[]=ATTRCAT_ATTR_RELNAME;
      RecId searchIndex=BlockAccess::linearSearch(ATTRCAT_RELID,attn,relNameAttr,EQ);
         if (searchIndex.block == -1 and searchIndex.slot == -1)
         break;

        RecBuffer attrCatBlock(searchIndex.block);
        attrCatBlock.getRecord(attrCatEntryRecord,searchIndex.slot);
       
        if (strcmp(attrCatEntryRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, oldName) == 0) {
           attrToRenameRecId = searchIndex;
         break;
       }
        if (strcmp(attrCatEntryRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, newName) == 0) {
          return E_ATTREXIST;
       }
    }

    if(attrToRenameRecId.slot==-1 and attrToRenameRecId.block==-1){
      return E_ATTRNOTEXIST;
    }

  RecBuffer attrCatBlock(attrToRenameRecId.block);
  Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
  attrCatBlock.getRecord(attrCatRecord,attrToRenameRecId.slot);
  strcpy(attrCatRecord[ATTRCAT_ATTR_NAME_INDEX].sVal,newName);
  attrCatBlock.setRecord(attrCatRecord,attrToRenameRecId.slot);

 return SUCCESS;
}


int BlockAccess::insert(int relId, Attribute *record) {
  //to relcatlog entry 
   RelCatEntry relCatEntry;
   RelCacheTable::getRelCatEntry(relId,&relCatEntry);
   
    int blockNum =relCatEntry.firstBlk;
    RecId rec_id = {-1, -1};
    int numOfSlots = relCatEntry.numSlotsPerBlk;
    int numOfAttributes = relCatEntry.numAttrs;
    int prevBlockNum = -1;
   //get free slot in block to save record
    while (blockNum != -1) {
     //get slotmap to get free slot
      RecBuffer recBuffer(blockNum);
      HeadInfo header;
      recBuffer.getHeader(&header);
      unsigned char *slotMap =
      (unsigned char *)malloc(sizeof(unsigned char) * header.numSlots);
      recBuffer.getSlotMap(slotMap);
       
      for (int slot = 0; slot < header.numSlots; slot++) {
       if (slotMap[slot] == SLOT_UNOCCUPIED) {
        rec_id.block = blockNum;
        rec_id.slot = slot;
        break;
        }
      }

      if (rec_id.slot != -1 and rec_id.block != -1) {
         break;
       }
     //if given block fully traversed,then goto right block
       prevBlockNum = blockNum;
       blockNum = header.rblock;       
    }
    //if there is no space in block we want to get new block
    if (rec_id.block == -1 and rec_id.slot == -1)
    {
       
	   if (relId == RELCAT_RELID) {
      return E_MAXRELATIONS;
      }

	    RecBuffer blockBuffer;//it calls getfreeblock
      blockNum = blockBuffer.getBlockNum();

      if (blockNum == E_DISKFULL) {
            return E_DISKFULL;
      }

      rec_id.block = blockNum;
      rec_id.slot = 0;
      //headinfo for newblock
      HeadInfo blockheader;
      blockheader.pblock = blockheader.rblock = -1;
      blockheader.blockType = REC;
      blockheader.lblock = -1;

      if (relCatEntry.numRecs == 0) {
       blockheader.lblock = -1;
      }   
	   else//if newblock has prevblock which relation record
     {
      blockheader.lblock = prevBlockNum;
     }

      blockheader.rblock=-1;
      blockheader.numAttrs = relCatEntry.numAttrs;
      blockheader.numEntries = 0;
      blockheader.numSlots = relCatEntry.numSlotsPerBlk;
      blockBuffer.setHeader(&blockheader);//set header
      //set slotmap
      unsigned char *slotMap = (unsigned char *)malloc(sizeof(unsigned char) * relCatEntry.numSlotsPerBlk);
      
      for (int slot = 0; slot < relCatEntry.numSlotsPerBlk; slot++) {
      slotMap[slot] = SLOT_UNOCCUPIED;
      }

      blockBuffer.setSlotMap(slotMap);
        
      if (prevBlockNum != -1)//change prev block headerrblock,if there is a prevblock
		  {
          RecBuffer prevBuffer(prevBlockNum);
          HeadInfo prevHeader;
          prevBuffer.getHeader(&prevHeader);
          prevHeader.rblock = blockNum;
          prevBuffer.setHeader(&prevHeader);
      }
      else //change rel firstblock
      {
            relCatEntry.firstBlk=rec_id.block;
            RelCacheTable::setRelCatEntry(relId,&relCatEntry);
      }

       relCatEntry.lastBlk = rec_id.block;
       RelCacheTable::setRelCatEntry(relId, &relCatEntry);
      
    }
 //save record into block which we got
   RecBuffer blockBuffer(rec_id.block);
   int ret= blockBuffer.setRecord(record,rec_id.slot);
   if(ret!=SUCCESS){
      printf("Record not saved successfully.\n");
        exit(1);
   }
    
   unsigned char *slotMap=(unsigned char *)malloc(sizeof(unsigned char )*relCatEntry.numSlotsPerBlk);//update slot map
   blockBuffer.getSlotMap(slotMap);
   slotMap[rec_id.slot]=SLOT_OCCUPIED;
   blockBuffer.setSlotMap(slotMap);
   
    HeadInfo header;//increase no entries in block
    blockBuffer.getHeader(&header);
    header.numEntries=header.numEntries+1;
    blockBuffer.setHeader(&header);

    relCatEntry.numRecs++;//increase no rec in relcache

    RelCacheTable::setRelCatEntry(relId,&relCatEntry);
   
 return SUCCESS;
}


int BlockAccess::search(int relId, Attribute *record, char attrName[ATTR_SIZE], Attribute attrVal, int op) {

    RecId recId;
    recId = BlockAccess::linearSearch(relId, attrName, attrVal, op);

    if (recId.block == -1 and recId.slot == -1)
    return E_NOTFOUND;

	RecBuffer recBuffer(recId.block);
    int ret = recBuffer.getRecord(record, recId.slot);
    if (ret != SUCCESS)
    return ret;
	

 return SUCCESS;
}

int BlockAccess::deleteRelation(char relName[ATTR_SIZE]) {
  // check rel is relcat or attrcat
	if (strcmp(relName, RELCAT_RELNAME) == 0 || strcmp(relName, ATTRCAT_RELNAME) == 0)
        return E_NOTPERMITTED;

   RelCacheTable::resetSearchIndex(RELCAT_RELID);

  // relname into attribute form
    Attribute relNameAttr;
	strcpy((char*)relNameAttr.sVal,(const char*)relName);
	
	char attrname[]= RELCAT_ATTR_RELNAME;
 //get block and slot of relname in relcat 
	RecId relCatRecId = BlockAccess::linearSearch(RELCAT_RELID,attrname, relNameAttr ,EQ);
    
	if (relCatRecId.block==-1 or relCatRecId.slot==-1) return E_RELNOTEXIST;

    RecBuffer relCatBlockBuffer (relCatRecId.block);
    Attribute relCatEntryRecord[RELCAT_NO_ATTRS];
	relCatBlockBuffer.getRecord(relCatEntryRecord, relCatRecId.slot);

    int firstBlock = relCatEntryRecord[RELCAT_FIRST_BLOCK_INDEX].nVal;
	int numAttributes = relCatEntryRecord[RELCAT_NO_ATTRIBUTES_INDEX].nVal;

   // set first block as current blk
    int currentBlockNum = firstBlock;
  
	while (currentBlockNum != -1) {// release every current blk,update current as right blk
		RecBuffer bufrblk (currentBlockNum);
		HeadInfo blkheader;
		bufrblk.getHeader(&blkheader);

		currentBlockNum = blkheader.rblock;
		bufrblk.releaseBlock();
	}

	RelCacheTable::resetSearchIndex(ATTRCAT_RELID);//search in attrcat


    int numberOfAttributesDeleted = 0;

    while(true)
    {
      char attrname[]= RELCAT_ATTR_RELNAME;
      RecId attrCatRecId = BlockAccess::linearSearch(ATTRCAT_RELID,attrname, relNameAttr, EQ);
     
		  if (attrCatRecId.block==-1 or attrCatRecId.slot==-1) break;

      numberOfAttributesDeleted++;

		  RecBuffer attrCatBlockBuffer (attrCatRecId.block);
		  HeadInfo attrCatHeader;
		  attrCatBlockBuffer.getHeader(&attrCatHeader);
		  Attribute attrCatRecord [ATTRCAT_NO_ATTRS];
		  attrCatBlockBuffer.getRecord(attrCatRecord, attrCatRecId.slot);

      int rootBlock = attrCatRecord[ATTRCAT_ROOT_BLOCK_INDEX].nVal;//get root block

		  unsigned char slotmap [attrCatHeader.numSlots];// get slot map of attribute containing blk and set slot unoccupied
		  attrCatBlockBuffer.getSlotMap(slotmap);
		  slotmap[attrCatRecId.slot] = SLOT_UNOCCUPIED;
		  attrCatBlockBuffer.setSlotMap(slotmap);

		 attrCatHeader.numEntries--;//decrease noof entries in header
		 attrCatBlockBuffer.setHeader(&attrCatHeader);
 
 		    
      if (attrCatHeader.numEntries == 0) {//change in prev leftblk=currentblk right 
         RecBuffer prevBlock (attrCatHeader.lblock);
          HeadInfo leftHeader;
          prevBlock.getHeader(&leftHeader);
          leftHeader.rblock = attrCatHeader.rblock;
          prevBlock.setHeader(&leftHeader);

        if (attrCatHeader.rblock != INVALID_BLOCKNUM)//if there is right change rightblk left is currentblk left
        {
          RecBuffer nextBlock (attrCatHeader.rblock);
          HeadInfo rightHeader;
          nextBlock.getHeader(&rightHeader);
          rightHeader.lblock = attrCatHeader.lblock;
          nextBlock.setHeader(&rightHeader);
        } 
        else//set attrcatblk 's lastblk is currentblk left
        {          
          RelCatEntry relCatEntryBuffer;
          RelCacheTable::getRelCatEntry(ATTRCAT_RELID, &relCatEntryBuffer);
          relCatEntryBuffer.lastBlk = attrCatHeader.lblock;
        }

        attrCatBlockBuffer.releaseBlock();//then relase blk
      }

      //  if (rootBlock != -1) {
            // delete the bplus tree rooted at rootBlock using BPlusTree::bPlusDestroy()
       // }
    }
    
	HeadInfo relCatHeader;//decrese no entries in relcatblk 
	relCatBlockBuffer.getHeader(&relCatHeader);
	relCatHeader.numEntries--;
	relCatBlockBuffer.setHeader(&relCatHeader);
    
	unsigned char slotmap [relCatHeader.numSlots];//also set unoccupied in slotmap
	relCatBlockBuffer.getSlotMap(slotmap);
	slotmap[relCatRecId.slot] = SLOT_UNOCCUPIED;
	relCatBlockBuffer.setSlotMap(slotmap);

	RelCatEntry relCatEntryBuffer;//in relcache one rel record deleted
	RelCacheTable::getRelCatEntry(RELCAT_RELID, &relCatEntryBuffer);
	relCatEntryBuffer.numRecs--;
	RelCacheTable::setRelCatEntry(RELCAT_RELID, &relCatEntryBuffer);

	RelCacheTable::getRelCatEntry(ATTRCAT_RELID, &relCatEntryBuffer);//in attrcache noofattrdeleted is deleted
	relCatEntryBuffer.numRecs -= numberOfAttributesDeleted;
	RelCacheTable::setRelCatEntry(ATTRCAT_RELID, &relCatEntryBuffer);


 return SUCCESS;
}

