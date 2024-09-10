#include "Schema.h"

#include <cmath>
#include <cstring>


int Schema::openRel(char relName[ATTR_SIZE])
{ // call open rel and check it is success or not
	int ret = OpenRelTable::openRel(relName);

	if (ret >= 0)
		return SUCCESS;

 return ret;
}

int Schema::closeRel(char relName[ATTR_SIZE])
{  //if rel is relcat or attributecat not permitt
	if (strcmp(relName, RELCAT_RELNAME) == 0 || strcmp(relName, ATTRCAT_RELNAME) == 0)
		return E_NOTPERMITTED;
  
  //check rel open or not,if open  then use that relid for closerel
	int relId = OpenRelTable::getRelId(relName);
	 if (relId == E_RELNOTOPEN)
		return E_RELNOTOPEN;
 
 return OpenRelTable::closeRel(relId);
}


int Schema::renameRel(char oldRelName[ATTR_SIZE], char newRelName[ATTR_SIZE]) {
 // check new or old rel names is not relcat or attrcat
 if(strcmp(oldRelName,RELCAT_RELNAME)==0 || strcmp(oldRelName,ATTRCAT_RELNAME)==0)
   return E_NOTPERMITTED;

 if(strcmp(newRelName,RELCAT_RELNAME)==0 || strcmp(newRelName,ATTRCAT_RELNAME)==0)
   return E_NOTPERMITTED;
 //get relid of rel
   int relId=OpenRelTable::getRelId(oldRelName);
   if(relId!=E_RELNOTOPEN)
     return E_RELOPEN;

   return  BlockAccess::renameRelation(oldRelName, newRelName);
}

int Schema::renameAttr(char *relName, char *oldAttrName, char *newAttrName) {
   //check rel is relcat or attr cat
  if(strcmp(relName,RELCAT_RELNAME)==0 || strcmp(relName,ATTRCAT_RELNAME)==0)
   return E_NOTPERMITTED;
 
   //get rel id of rel 
    int relId=OpenRelTable::getRelId(relName);
   if(relId!=E_RELNOTOPEN)
     return E_RELOPEN;

   return BlockAccess::renameAttribute(relName,oldAttrName,newAttrName);
}


int  Schema::createRel(char relName[],int nAttrs, char attrs[][ATTR_SIZE],int attrtype[]){
     //set rel as attribute
    Attribute relNameAsAttribute;
    strcpy((char *)relNameAsAttribute.sVal,(const char*)relName);

    RecId targetRelId;    //reset search index of relcat
    RelCacheTable::resetSearchIndex(RELCAT_RELID);
    
    char attrname[]=RELCAT_ATTR_RELNAME;    //relcat attr relnam as array for linearsearch
   
   //check relname is there relcat usind ls rel as relcatattr
    RecId relcatRecId=BlockAccess::linearSearch(RELCAT_RELID,attrname,relNameAsAttribute,EQ);
    if(relcatRecId.block!=-1 or relcatRecId.slot!=-1){
      return E_RELEXIST;
    }
    
    // is there any attribute duplicate in rel
    for(int i=0;i<nAttrs;i++){
      for(int j=i+1;j<nAttrs;j++){
        if(strcmp(attrs[i],attrs[j])==0)return E_DUPLICATEATTR;

      }
    }
    
    //initialise relcat record for this rel
    Attribute relCatRecord[RELCAT_NO_ATTRS];
    strcpy(relCatRecord[RELCAT_REL_NAME_INDEX].sVal,relName);
    relCatRecord[RELCAT_NO_ATTRIBUTES_INDEX].nVal=nAttrs;
    relCatRecord[RELCAT_NO_RECORDS_INDEX].nVal=0;
    relCatRecord[RELCAT_FIRST_BLOCK_INDEX].nVal=-1;
    relCatRecord[RELCAT_LAST_BLOCK_INDEX].nVal=-1;
    int numofslots=floor(2016*1.00/(16*nAttrs+1));
    relCatRecord[RELCAT_NO_SLOTS_PER_BLOCK_INDEX].nVal=numofslots;

    //insert relcat rcord into relcat using insert function
    int retVal=BlockAccess::insert(RELCAT_RELID,relCatRecord);
    if(retVal!=SUCCESS)return retVal;
   
    //initialise attrcat record for this rel attribute && insert into attrcat by insert function 
   for(int attrIndex=0;attrIndex<nAttrs;attrIndex++)
    {
      Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
      strcpy(attrCatRecord[ATTRCAT_REL_NAME_INDEX].sVal,relName);
      strcpy(attrCatRecord[ATTRCAT_ATTR_NAME_INDEX].sVal,attrs[attrIndex]);
      attrCatRecord[ATTRCAT_ATTR_TYPE_INDEX].nVal=attrtype[attrIndex];
      attrCatRecord[ATTRCAT_PRIMARY_FLAG_INDEX].nVal=-1;
      attrCatRecord[ATTRCAT_ROOT_BLOCK_INDEX].nVal=-1;
      attrCatRecord[ATTRCAT_OFFSET_INDEX].nVal=attrIndex;

        retVal=BlockAccess::insert(ATTRCAT_RELID,attrCatRecord);//attrcatrecord not insert then delete that rel bcz rel is inserted in relcat
        if(retVal!=SUCCESS){
          Schema::deleteRel(relName);
          return E_DISKFULL;
       }
    }
    
 return SUCCESS;
}

int Schema::deleteRel(char *relName) {
 //check rel is relcat or attrcat
    if (strcmp(relName, RELCAT_RELNAME) == 0 || strcmp(relName, ATTRCAT_RELNAME) == 0)
		return E_NOTPERMITTED;

 //get relid for rel is open in cache or not
    int relId=OpenRelTable::getRelId(relName);
    if(relId>0 and relId<MAX_OPEN)return E_RELOPEN;
    
  //delete rel  
    int retVal=BlockAccess::deleteRelation(relName);
    if(retVal!=SUCCESS)
      return retVal;
 return retVal;
}

