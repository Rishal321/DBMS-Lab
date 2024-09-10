#include "OpenRelTable.h"
#include <stdlib.h>
#include <stdio.h>
#include <cstring>

struct OpenRelTableMetaInfo OpenRelTable::tableMetaInfo[MAX_OPEN];

AttrCacheEntry *createAttrCacheEntryList(int size)
{
	AttrCacheEntry *head = nullptr, *curr = nullptr;
	head = curr = (AttrCacheEntry *)malloc(sizeof(AttrCacheEntry));
	size--;
	while (size--)
	{
		curr->next = (AttrCacheEntry *)malloc(sizeof(AttrCacheEntry));
		curr = curr->next;
	}
	curr->next = nullptr;

 return head;
}

OpenRelTable::OpenRelTable() {
 //initialise relCache,attrcache tablemeta info
 for (int i = 0; i < MAX_OPEN; ++i) {
    RelCacheTable::relCache[i] = nullptr;
    AttrCacheTable::attrCache[i] = nullptr;
    tableMetaInfo[i].free = true;
  }
 // assign  relcat and attrcat in relcache
 RecBuffer relCatBlock(RELCAT_BLOCK);
 Attribute relCatRecord[RELCAT_NO_ATTRS];
 RelCacheEntry *relCacheEntry=nullptr;

 for(int relId=RELCAT_RELID;relId<=ATTRCAT_RELID;relId++){
    relCatBlock.getRecord(relCatRecord,relId);
    relCacheEntry=(RelCacheEntry*)malloc(sizeof(relCacheEntry));
    RelCacheTable::recordToRelCatEntry(relCatRecord,&(relCacheEntry->relCatEntry));
    relCacheEntry->recId.block=RELCAT_BLOCK;
    relCacheEntry->recId.slot=relId;
    relCacheEntry->dirty=false;
    relCacheEntry->searchIndex={-1,-1};
    RelCacheTable::relCache[relId]=relCacheEntry;
 }
 // assign  relcat and attrcat in attrcache
 RecBuffer attrCatBlock(ATTRCAT_BLOCK);
 Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
 AttrCacheEntry *attrCacheEntry = nullptr, *head = nullptr;

 for(int relId=RELCAT_RELID,recordId=0;relId<=ATTRCAT_RELID;relId++){
    int nOAttr = RelCacheTable::relCache[relId]->relCatEntry.numAttrs;
		head = createAttrCacheEntryList(nOAttr);
		attrCacheEntry = head;

        while (nOAttr--)
		{
			attrCatBlock.getRecord(attrCatRecord, recordId);
            AttrCacheTable::recordToAttrCatEntry(attrCatRecord,&(attrCacheEntry->attrCatEntry));
			attrCacheEntry->recId.slot = recordId++;
			attrCacheEntry->recId.block = ATTRCAT_BLOCK;
            attrCacheEntry->dirty=false;
			attrCacheEntry = attrCacheEntry->next;
		}

        AttrCacheTable::attrCache[relId] = head;
    }
//table meta info also assign 
    tableMetaInfo[RELCAT_RELID].free = false; 
	strcpy(tableMetaInfo[RELCAT_RELID].relName, RELCAT_RELNAME);
	tableMetaInfo[ATTRCAT_RELID].free = false; 
	strcpy(tableMetaInfo[ATTRCAT_RELID].relName, ATTRCAT_RELNAME);
 
}

OpenRelTable::~OpenRelTable(){
  //close rel other than relcat and attrcat
  for (int i = 2; i < MAX_OPEN; ++i) {
    if (!tableMetaInfo[i].free) {
      OpenRelTable::closeRel(i); 
    }
  }
 // relcache check  attrcat is dirty if yes then set record 
  if (RelCacheTable::relCache[ATTRCAT_RELID]->dirty == true) {
	    RelCatEntry relCatBuffer;
		RelCacheTable::getRelCatEntry(ATTRCAT_RELID, &relCatBuffer);
		Attribute relCatRecord [RELCAT_NO_ATTRS];
		RelCacheTable::relCatEntryToRecord(&(RelCacheTable::relCache[ATTRCAT_RELID]->relCatEntry), relCatRecord);
		RecId recId = RelCacheTable::relCache[ATTRCAT_RELID]->recId;
		RecBuffer relCatBlock(recId.block);
		relCatBlock.setRecord(relCatRecord,recId.slot);
	}

	free (RelCacheTable::relCache[ATTRCAT_RELID]);
 //relcache check  relcat is dirty if yes then set record 
	if (RelCacheTable::relCache[RELCAT_RELID]->dirty == true) {
	    RelCatEntry relCatBuffer;
		RelCacheTable::getRelCatEntry(RELCAT_RELID, &relCatBuffer);
		Attribute relCatRecord [RELCAT_NO_ATTRS];
		RelCacheTable::relCatEntryToRecord(&(RelCacheTable::relCache[RELCAT_RELID]->relCatEntry), relCatRecord);
		RecId recId = RelCacheTable::relCache[RELCAT_RELID]->recId;
		RecBuffer relCatBlock(recId.block);
		relCatBlock.setRecord(relCatRecord, recId.slot);
	}

	free (RelCacheTable::relCache[RELCAT_RELID]);
 // attr cache similary like above
 for(int relId = ATTRCAT_RELID; relId >= RELCAT_RELID; relId--)
 {
	AttrCacheEntry *head = AttrCacheTable::attrCache[relId];
	AttrCacheEntry *next = head->next;

	while (true) {
		if (head->dirty)
		{
			Attribute attrCatRecord [ATTRCAT_NO_ATTRS];
			AttrCacheTable::attrCatEntryToRecord(&(head->attrCatEntry), attrCatRecord);

			RecBuffer attrCatBlockBuffer (head->recId.block);
			attrCatBlockBuffer.setRecord(attrCatRecord, head->recId.slot);
		}


		free (head);
		head = next;

		if (head == NULL) break;
		next = next->next;
	}
  }
   //freen relcache and attrcache other relcat and attrcat
  for (int i = 2; i < MAX_OPEN; ++i)
    {
        free(RelCacheTable::relCache[i]);
        free(AttrCacheTable::attrCache[i]);
    }

}

int OpenRelTable::getRelId(char relName[ATTR_SIZE]) {
	//check rel in table metainfo
    for (int relId = 0; relId < MAX_OPEN; relId++) 
		if (strcmp(tableMetaInfo[relId].relName, relName) == 0
			&& tableMetaInfo[relId].free == false)
			return relId;

 return E_RELNOTOPEN;
}

int OpenRelTable::getFreeOpenRelTableEntry()
{
  //check is there any free in tablemetainfo
	for (int relId = 0; relId < MAX_OPEN; relId++)
		if (tableMetaInfo[relId].free)
			return relId;

	return E_CACHEFULL;
}

int OpenRelTable::openRel(char relName[ATTR_SIZE]) 
{  // check rel is already open or not if yes return that
	int relId = getRelId(relName);
  	if(relId >= 0){
		return relId;
  	}
    //other wise get freereltableentry
	relId = OpenRelTable::getFreeOpenRelTableEntry();
  	if (relId == E_CACHEFULL) 
	  return E_CACHEFULL;

 //using ls get recid of this rel in relcat
	Attribute attrVal; 
	strcpy(attrVal.sVal, relName);
    char attrname[]=RELCAT_ATTR_RELNAME;

  	RecId relcatRecId = BlockAccess::linearSearch(RELCAT_RELID,attrname, attrVal, EQ);

	if (relcatRecId.block ==-1 || relcatRecId.slot==-1) {
		return E_RELNOTEXIST;
	}
 //assign rel into relcache
	RecBuffer relationBuffer (relcatRecId.block);
	Attribute relationRecord [RELCAT_NO_ATTRS];
	RelCacheEntry *relCacheBuffer = nullptr;

	relationBuffer.getRecord(relationRecord, relcatRecId.slot);
	relCacheBuffer = (RelCacheEntry*) malloc (sizeof(RelCacheEntry));
	RelCacheTable::recordToRelCatEntry(relationRecord, &(relCacheBuffer->relCatEntry));
	relCacheBuffer->recId.block = relcatRecId.block;
	relCacheBuffer->recId.slot = relcatRecId.slot;
	relCacheBuffer->dirty=false;
    relCacheBuffer->searchIndex={-1,-1};
	RelCacheTable::relCache[relId] = relCacheBuffer;	

 //assign rel attr into attrcache
	Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
	AttrCacheEntry *attrCacheEntry = nullptr, *head = nullptr;

	int numberOfAttributes = RelCacheTable::relCache[relId]->relCatEntry.numAttrs;
	head = createAttrCacheEntryList(numberOfAttributes);
	attrCacheEntry = head;

	RelCacheTable::resetSearchIndex(ATTRCAT_RELID);
	while (numberOfAttributes--)
	{  
		char attrname[]=RELCAT_ATTR_RELNAME;

		RecId attrcatRecId = BlockAccess::linearSearch(ATTRCAT_RELID,attrname, attrVal, EQ);
		RecBuffer attrCatBlock(attrcatRecId.block);
		attrCatBlock.getRecord(attrCatRecord, attrcatRecId.slot);
		AttrCacheTable::recordToAttrCatEntry(
			attrCatRecord,
			&(attrCacheEntry->attrCatEntry)
		);
		attrCacheEntry->recId.block = attrcatRecId.block;
		attrCacheEntry->recId.slot = attrcatRecId.slot;
		attrCacheEntry->dirty=false;
		attrCacheEntry = attrCacheEntry->next;
	}

	AttrCacheTable::attrCache[relId] = head;
 // save relname into tablemetainfo
	tableMetaInfo[relId].free = false;
	strcpy(tableMetaInfo[relId].relName, relName);

 return relId;
}

int OpenRelTable::closeRel(int relId) {
  	if (relId == RELCAT_RELID || relId == ATTRCAT_RELID)// chch it si not attrcat or relcat
	return E_NOTPERMITTED;

  	if (0 > relId || relId >= MAX_OPEN)
	 return E_OUTOFBOUND;

  	if (tableMetaInfo[relId].free)//check relopen
	 return E_RELNOTOPEN;
 //relcache check  rel is dirty if yes then set record 
	if (RelCacheTable::relCache[relId]->dirty == true) {
		Attribute relCatBuffer [RELCAT_NO_ATTRS];
		RelCacheTable::relCatEntryToRecord(&(RelCacheTable::relCache[relId]->relCatEntry), relCatBuffer);
		RecId recId = RelCacheTable::relCache[relId]->recId;
		RecBuffer relCatBlock(recId.block);
		relCatBlock.setRecord(relCatBuffer, RelCacheTable::relCache[relId]->recId.slot);
	}

	free (RelCacheTable::relCache[relId]);

 //attrcache check  rel is dirty if yes then set record 
	AttrCacheEntry *head = AttrCacheTable::attrCache[relId];
	AttrCacheEntry *next = head->next;

	while (true) {
		if (head->dirty)
		{
			Attribute attrCatRecord [ATTRCAT_NO_ATTRS];
			AttrCacheTable::attrCatEntryToRecord(&(head->attrCatEntry), attrCatRecord);

			RecBuffer attrCatBlockBuffer (head->recId.block);
			attrCatBlockBuffer.setRecord(attrCatRecord, head->recId.slot);
		}


		free (head);
		head = next;

		if (head == NULL) break;
		next = next->next;
	}
 // set nullpttr to relcache and attrcache ,setfree rablemetainfo
	tableMetaInfo[relId].free = true;
	RelCacheTable::relCache[relId] = nullptr;
	AttrCacheTable::attrCache[relId] = nullptr;

  return SUCCESS;
}

