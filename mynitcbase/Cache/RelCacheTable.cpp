#include "RelCacheTable.h"

#include <cstring>

RelCacheEntry *RelCacheTable::relCache[MAX_OPEN];

int RelCacheTable::getRelCatEntry(int relId, RelCatEntry *relCatBuf){
	if (relId < 0 || relId >= MAX_OPEN)
		return E_OUTOFBOUND;

	if (relCache[relId] == nullptr)
		return E_RELNOTOPEN;

	*relCatBuf = relCache[relId]->relCatEntry;
 return SUCCESS;
}

int RelCacheTable::setRelCatEntry(int relId, RelCatEntry *relCatBuf) {

  if(relId < 0 || relId >= MAX_OPEN) return E_OUTOFBOUND;

  if(RelCacheTable::relCache[relId] == nullptr) return E_RELNOTOPEN;

  memcpy(&(RelCacheTable::relCache[relId]->relCatEntry), relCatBuf, sizeof(RelCatEntry));

  RelCacheTable::relCache[relId]->dirty = true;

 return SUCCESS;
}

void RelCacheTable::recordToRelCatEntry(union Attribute record[RELCAT_NO_ATTRS],RelCatEntry *relCatEntry)
{
	strcpy(relCatEntry->relName, record[RELCAT_REL_NAME_INDEX].sVal);
	relCatEntry->numAttrs = (int)record[RELCAT_NO_ATTRIBUTES_INDEX].nVal;
	relCatEntry->numRecs = (int)record[RELCAT_NO_RECORDS_INDEX].nVal;
	relCatEntry->firstBlk = (int)record[RELCAT_FIRST_BLOCK_INDEX].nVal;
	relCatEntry->lastBlk = (int)record[RELCAT_LAST_BLOCK_INDEX].nVal;
	relCatEntry->numSlotsPerBlk = (int)record[RELCAT_NO_SLOTS_PER_BLOCK_INDEX].nVal;
}

void RelCacheTable::relCatEntryToRecord(RelCatEntry *relCatEntry,union Attribute record[RELCAT_NO_ATTRS])
{
	strcpy(record[RELCAT_REL_NAME_INDEX].sVal, relCatEntry->relName);
	record[RELCAT_NO_ATTRIBUTES_INDEX].nVal = (int)relCatEntry->numAttrs;
	record[RELCAT_NO_RECORDS_INDEX].nVal = (int)relCatEntry->numRecs;
	record[RELCAT_FIRST_BLOCK_INDEX].nVal = (int)relCatEntry->firstBlk;
	record[RELCAT_LAST_BLOCK_INDEX].nVal = (int)relCatEntry->lastBlk;
	record[RELCAT_NO_SLOTS_PER_BLOCK_INDEX].nVal = (int)relCatEntry->numSlotsPerBlk;
}

int RelCacheTable::getSearchIndex(int relId, RecId *searchIndex){

	if (relId < 0 || relId >= MAX_OPEN)
		return E_OUTOFBOUND;

	if (RelCacheTable::relCache[relId] == nullptr)
		return E_RELNOTOPEN;

	*searchIndex = relCache[relId]->searchIndex;
 return SUCCESS;
}

int RelCacheTable::setSearchIndex(int relId, RecId *searchIndex){

	if (relId < 0 || relId >= MAX_OPEN)
		return E_OUTOFBOUND;

	if (RelCacheTable::relCache[relId] == nullptr)
		return E_RELNOTOPEN;

	relCache[relId]->searchIndex = *searchIndex;

 return SUCCESS;
}

int RelCacheTable::resetSearchIndex(int relId){

	if (relId < 0 || relId >= MAX_OPEN)
		return E_OUTOFBOUND;

	if (RelCacheTable::relCache[relId] == nullptr)
		return E_RELNOTOPEN;
		
	RelCacheTable::relCache[relId]->searchIndex = {-1, -1};

 return SUCCESS;
}

