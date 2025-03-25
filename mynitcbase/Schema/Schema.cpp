#include "Schema.h"

#include <cmath>
#include <cstring>


int Schema::openRel(char relName[ATTR_SIZE]) {
  int ret = OpenRelTable::openRel(relName);

  // the OpenRelTable::openRel() function returns the rel-id if successful
  // a valid rel-id will be within the range 0 <= relId < MAX_OPEN and any
  // error codes will be negative
  if(ret >= 0){
    return SUCCESS;
  }

  //otherwise it returns an error message
  return ret;
}

int Schema::closeRel(char relName[ATTR_SIZE]) {
  if (strcmp(relName,RELCAT_RELNAME) == 0 || strcmp(relName,ATTRCAT_RELNAME) == 0) {
    return E_NOTPERMITTED;
  }

  // this function returns the rel-id of a relation if it is open or
  // E_RELNOTOPEN if it is not. we will implement this later.
  int relId = OpenRelTable::getRelId(relName);

  if (relId == E_RELNOTOPEN) {
    return E_RELNOTOPEN;
  }

  return OpenRelTable::closeRel(relId);
}


int Schema::renameRel(char oldRelName[ATTR_SIZE], char newRelName[ATTR_SIZE]) {
//checking if were trying to rename the relationcat or the attrcat
    if(strcmp(oldRelName,RELCAT_RELNAME) == 0 || strcmp(oldRelName,ATTRCAT_RELNAME) == 0
    ||strcmp(newRelName,RELCAT_RELNAME)==0 || strcmp(newRelName,ATTRCAT_RELNAME)==0){
        return E_NOTPERMITTED;
    }

//ensure that the relation is closed 
    int relid = OpenRelTable::getRelId(oldRelName);
    if(relid != E_RELNOTOPEN){
        return E_RELOPEN;
    }

    // retVal = BlockAccess::renameRelation(oldRelName, newRelName);
    // return retVal
    int retVal = BlockAccess::renameRelation(oldRelName, newRelName);
    return retVal;
}


int Schema::renameAttr(char *relName, char *oldAttrName, char *newAttrName) {
    // if the relName is either Relation Catalog or Attribute Catalog,
        // return E_NOTPERMITTED
    if(strcmp(relName,RELCAT_RELNAME) == 0 || strcmp(relName,ATTRCAT_RELNAME)== 0){
        return E_NOTPERMITTED;
    }

    // if the relation is open
    int relid = OpenRelTable::getRelId(relName);
    if(relid != E_RELNOTOPEN){
        return E_RELOPEN;
    }

    // Call BlockAccess::renameAttribute with appropriate arguments.
    int retVal = BlockAccess::renameAttribute(relName,oldAttrName,newAttrName);
    return retVal;
}

int Schema::createRel(char relName[],int nAttrs, char attrs[][ATTR_SIZE],int attrtype[]){
//store the relname you want to create 
    Attribute relNameAsAttribute;
    strcpy(relNameAsAttribute.sVal, relName);
    RecId targetRecId;

    // Reset the searchIndex using RelCacheTable::resetSearhIndex()
    // Search the relation catalog (relId given by the constant RELCAT_RELID)
    // for attribute value attribute "RelName" = relNameAsAttribute using
    // BlockAccess::linearSearch() with OP = EQ

    RelCacheTable::resetSearchIndex(RELCAT_RELID);
    char relcatAttrName[ATTR_SIZE];
    strcpy(relcatAttrName, RELCAT_ATTR_RELNAME);

    targetRecId = BlockAccess::linearSearch(RELCAT_RELID, relcatAttrName, relNameAsAttribute, EQ);
    //check if relation with relname already exists
    if(targetRecId.block != -1 and targetRecId.slot != -1){
        return E_RELEXIST;
    }

    // compare every pair of attributes of attrNames[] array
    // if any attribute names have same string value,
    //     return E_DUPLICATEATTR (i.e 2 attributes have same value)

    for(int i = 0 ; i < nAttrs ; i++){
        for(int j = i + 1 ; j < nAttrs ; j++){
            if(strcmp(attrs[i],attrs[j]) == 0){
                return E_DUPLICATEATTR;
            }
        }
    }

//the record which will be inserted in relcat
    Attribute relCatRecord[RELCAT_NO_ATTRS];

    strcpy(relCatRecord[RELCAT_REL_NAME_INDEX].sVal,relName);
    relCatRecord[RELCAT_NO_ATTRIBUTES_INDEX].nVal = nAttrs;
    relCatRecord[RELCAT_NO_RECORDS_INDEX].nVal = 0;
    relCatRecord[RELCAT_FIRST_BLOCK_INDEX].nVal = -1;
    relCatRecord[RELCAT_LAST_BLOCK_INDEX].nVal = -1;
    relCatRecord[RELCAT_NO_SLOTS_PER_BLOCK_INDEX].nVal = floor((2016 / (16 * nAttrs + 1)));
   


    // retVal = BlockAccess::insert(RELCAT_RELID(=0), relCatRecord);
    // if BlockAccess::insert fails return retVal
    // (this call could fail if there is no more space in the relation catalog)

    int retVal = BlockAccess::insert(RELCAT_RELID, relCatRecord);

    if(retVal != SUCCESS){
        return retVal;
    }

    // iterate through 0 to numOfAttributes - 1 :
    for(int i = 0 ; i < nAttrs ; i++){
        
//create the record which will be inserted in attrcat
        Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
        strcpy(attrCatRecord[ATTRCAT_REL_NAME_INDEX].sVal,relName);
        strcpy(attrCatRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, attrs[i]);
        attrCatRecord[ATTRCAT_ATTR_TYPE_INDEX].nVal = attrtype[i];
        attrCatRecord[ATTRCAT_PRIMARY_FLAG_INDEX].nVal = -1;
        attrCatRecord[ATTRCAT_ROOT_BLOCK_INDEX].nVal = -1;
        attrCatRecord[ATTRCAT_OFFSET_INDEX].nVal = i;


       retVal = BlockAccess::insert(ATTRCAT_RELID, attrCatRecord);
       if(retVal != SUCCESS){
        Schema::deleteRel(relName);
        return E_DISKFULL;
       }
    }

    return SUCCESS;
}


int Schema::deleteRel(char *relName) {
 //relcat or attrcat not permitted
   if(strcmp(relName,RELCAT_RELNAME) == 0 || strcmp(relName,ATTRCAT_RELNAME) == 0){
    return E_NOTPERMITTED;
   }
//get the relid
   int relId = OpenRelTable::getRelId(relName);
//cannot delete an open relation
   if(relId >= 0 and relId < MAX_OPEN){
    return E_RELOPEN;
   }

   int ret = BlockAccess::deleteRelation(relName);
   return ret;
}