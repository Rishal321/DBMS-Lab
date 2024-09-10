#include "Algebra.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <cstring>
#include <iostream>
#include <cstring>

bool isNumber(char *str){
    int len;
    float ignore;
   
    int ret = sscanf(str, "%f %n", &ignore, &len);
    return ret == 1 && len == strlen(str);
}

int Algebra::select(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE], char attr[ATTR_SIZE], int op, char strVal[ATTR_SIZE])
{   //check rel open or not
    int srcRelId = OpenRelTable::getRelId(srcRel); 
    if (srcRelId == E_RELNOTOPEN){
        return E_RELNOTOPEN;
    }
  //  for attrtype that attr
    AttrCatEntry attrCatEntry;
    int ret = AttrCacheTable::getAttrCatEntry(srcRelId, attr, &attrCatEntry);
    if (ret != SUCCESS)
    {
        return ret;
    }

    int type = attrCatEntry.attrType;
    Attribute attrVal;// assign attrval in attrribute form
    if (type == NUMBER)
    {
        if (isNumber(strVal)){
            attrVal.nVal = atof(strVal);
        }
        else
        {
            return E_ATTRTYPEMISMATCH;
        }
    }
    else if (type == STRING)
    {
        strcpy(attrVal.sVal, strVal);
    }

    
    RelCacheTable::resetSearchIndex(srcRelId);

    RelCatEntry relCatEntry;
    RelCacheTable::getRelCatEntry(srcRelId, &relCatEntry);

    printf("|");
    for (int i = 0; i < relCatEntry.numAttrs; ++i){

        AttrCacheTable::getAttrCatEntry(srcRelId, i, &attrCatEntry);
        printf(" %s |", attrCatEntry.attrName);
    }
    printf("\n");

    while (true)
    {
        RecId searchRes = BlockAccess::linearSearch(srcRelId, attr, attrVal, op);

        if (searchRes.block != -1 && searchRes.slot != -1)
        {
            RecBuffer blockbuffer(searchRes.block);
            Attribute rec[relCatEntry.numAttrs];
            blockbuffer.getRecord(rec, searchRes.slot);

            printf("|");
            for (int i = 0; i < relCatEntry.numAttrs; ++i)
            {
                AttrCacheTable::getAttrCatEntry(srcRelId, i, &attrCatEntry);

                if (attrCatEntry.attrType == NUMBER) {
                    printf(" %d |", int(rec[i].nVal));
                }
                else{
                    printf(" %s |", rec[i].sVal);
                }
            }
            printf("\n");
        }
        else
        {
            break;
        }
    }

    return SUCCESS;
}

int Algebra::insert(char relName[ATTR_SIZE], int nAttrs, char record[][ATTR_SIZE]){
    //verify relname is relcat or attrcat

    if(strcmp(relName,RELCAT_RELNAME)==0 || strcmp(relName,ATTRCAT_RELNAME)==0){
      return E_NOTPERMITTED;
    }
    //get relid
    int relId = OpenRelTable::getRelId(relName);
  
   if(relId==E_RELNOTOPEN){
      return E_RELNOTOPEN;
    }
  //check rel attr and given record attribute is same
    RelCatEntry relCatBuf;
    RelCacheTable::getRelCatEntry(relId,&relCatBuf);

    if(relCatBuf.numAttrs!=nAttrs){
        return E_NATTRMISMATCH;
    }

   Attribute recordValues[nAttrs];//store record into attribute recordvalues

    for(int i=0;i<nAttrs;i++){
        AttrCatEntry attrCatEntry;
        AttrCacheTable::getAttrCatEntry(relId,i,&attrCatEntry);

        int type=attrCatEntry.attrType;
        if (type == NUMBER)
        {
            if(isNumber(record[i])){
                   recordValues[i].nVal=atof(record[i]);
            }
            else
            {
                return E_ATTRTYPEMISMATCH;
            }
        }
        else if (type == STRING)
        {
          strcpy(recordValues[i].sVal,record[i]);
        }
    }
   //call insert function
 int retVal=BlockAccess::insert(relId,recordValues);

 return retVal;
}

