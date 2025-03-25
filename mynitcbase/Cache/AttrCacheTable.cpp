#include "AttrCacheTable.h"

#include <cstring>
#include <stdio.h>

AttrCacheEntry *AttrCacheTable::attrCache[MAX_OPEN];

/* returns the attrOffset-th attribute for the relation corresponding to relId
NOTE: this function expects the caller to allocate memory for `*attrCatBuf`
*/
int AttrCacheTable::getAttrCatEntry(int relId, int attrOffset,AttrCatEntry *attrCatBuf) {
  // check if 0 <= relId < MAX_OPEN and return E_OUTOFBOUND otherwise
  if (relId < 0 || relId >= MAX_OPEN) {
    return E_OUTOFBOUND;
  }

  // check if attrCache[relId] == nullptr and return E_RELNOTOPEN if true
  if (attrCache[relId] == nullptr) {
    return E_RELNOTOPEN;
  }
  // traverse the linked list of attribute cache entries

  for (AttrCacheEntry *entry = attrCache[relId]; entry != nullptr;
       entry = entry->next) {
    if (entry->attrCatEntry.offset == attrOffset) {
      // copy entry->attrCatEntry to *attrCatBuf and return SUCCESS;
      *attrCatBuf = entry->attrCatEntry;
    }
  }

  // there is no attribute at this offset
  return E_ATTRNOTEXIST;
}

/* Converts a attribute catalog record to AttrCatEntry struct
    We get the record as Attribute[] from the BlockBuffer.getRecord() function.
    This function will convert that to a struct AttrCatEntry type.
*/

/* returns the attribute with name `attrName` for the relation corresponding to
relId NOTE: this function expects the caller to allocate memory for
`*attrCatBuf`
*/
int AttrCacheTable::getAttrCatEntry(int relId, char attrName[ATTR_SIZE],AttrCatEntry *attrCatBuf) {
  // check that relId is valid and corresponds to an open relation
  if (relId < 0 || relId >= MAX_OPEN) {
    return E_OUTOFBOUND;
  }

  // check if attrCache[relId] == nullptr and return E_RELNOTOPEN if true
  if (attrCache[relId] == nullptr) {
    return E_RELNOTOPEN;
  }
  // for (AttrCacheEntry *entry = attrCache[relId]; entry != nullptr;entry = entry->next) {
  //   if (strcmp(entry->attrCatEntry.attrName, attrName) == 0) {
  //     strcpy(attrCatBuf->relName, entry->attrCatEntry.relName);
  //     strcpy(attrCatBuf->attrName, entry->attrCatEntry.attrName);
  //     attrCatBuf->attrType = entry->attrCatEntry.attrType;
  //     attrCatBuf->offset = entry->attrCatEntry.offset;
  //     attrCatBuf->primaryFlag = entry->attrCatEntry.primaryFlag;
  //     attrCatBuf->rootBlock = entry->attrCatEntry.rootBlock;
  //     return SUCCESS;
  //   }
  // }

  for (AttrCacheEntry *entry =attrCache[relId]; entry != nullptr;entry = entry->next) {

    if (strcmp(entry->attrCatEntry.attrName, attrName) == 0) {
      strcpy(attrCatBuf->relName, entry->attrCatEntry.relName);
      strcpy(attrCatBuf->attrName, entry->attrCatEntry.attrName);
      attrCatBuf->attrType = entry->attrCatEntry.attrType;
      attrCatBuf->offset = entry->attrCatEntry.offset;
      attrCatBuf->primaryFlag = entry->attrCatEntry.primaryFlag;
      attrCatBuf->rootBlock = entry->attrCatEntry.rootBlock;
      return SUCCESS;
    }
  }
  // iterate over the entries in the attribute cache and set attrCatBuf to the
  // entry that
  //    matches attrName

  // no attribute with name attrName for the relation
  return E_ATTRNOTEXIST;
}


void AttrCacheTable::recordToAttrCatEntry(
    union Attribute record[ATTRCAT_NO_ATTRS], AttrCatEntry *attrCatEntry) {
  strcpy(attrCatEntry->relName, record[ATTRCAT_REL_NAME_INDEX].sVal);
  strcpy(attrCatEntry->attrName, record[ATTRCAT_ATTR_NAME_INDEX].sVal);
  attrCatEntry->offset = (int)record[ATTRCAT_OFFSET_INDEX].nVal;
  attrCatEntry->attrType = (int)record[ATTRCAT_ATTR_TYPE_INDEX].nVal;
  attrCatEntry->rootBlock = (int)record[ATTRCAT_ROOT_BLOCK_INDEX].nVal;
  attrCatEntry->primaryFlag = (bool)record[ATTRCAT_PRIMARY_FLAG_INDEX].nVal;

  // copy the rest of the fields in the record to the attrCacheEntry struct
}


void AttrCacheTable::attrCatEntryToRecord(AttrCatEntry *attrCatEntry, Attribute record[ATTRCAT_NO_ATTRS])
{
    strcpy(record[ATTRCAT_REL_NAME_INDEX].sVal, attrCatEntry->relName);
    strcpy(record[ATTRCAT_ATTR_NAME_INDEX].sVal, attrCatEntry->attrName);

    record[ATTRCAT_ATTR_TYPE_INDEX].nVal = attrCatEntry->attrType;
    record[ATTRCAT_PRIMARY_FLAG_INDEX].nVal = attrCatEntry->primaryFlag;
    record[ATTRCAT_ROOT_BLOCK_INDEX].nVal = attrCatEntry->rootBlock;
    record[ATTRCAT_OFFSET_INDEX].nVal = attrCatEntry->offset;

    // copy the rest of the fields in the record to the attrCacheEntry struct
}

//get the search index using the attribute name 
//to get the block number and the index(slot) of the index that last matched the search conditions i.e the record that matched the values
int AttrCacheTable::getSearchIndex(int relId,char attrName[ATTR_SIZE],IndexId *searchIndex){
if(relId <0 || relId > MAX_OPEN){
  return E_OUTOFBOUND;
}
//if there are any attributes in the cache i.e if the relation is open or not 
if(AttrCacheTable::attrCache[relId]==nullptr)
  return E_RELNOTOPEN;
//access the first attribute ka attrcacheentry for that relation
AttrCacheEntry *attrcacheentry=AttrCacheTable::attrCache[relId];
//all the attrcache entry of a relation are connected as a linked list so you traverse the ll
while(attrcacheentry){
  //look for the attribute whose searchindex you want to find 
  if(strcmp(attrName,attrcacheentry->attrCatEntry.attrName)==0){
    //get the searchindex(the block number and the index at which the last hit was found)
    *searchIndex=attrcacheentry->searchIndex;
    return SUCCESS;
  }
  attrcacheentry=attrcacheentry->next;
}
return E_ATTRNOTEXIST;
}


//get the search index using the attribute offset value 
int AttrCacheTable::getSearchIndex(int relId,int attrOffset,IndexId *searchIndex){
if(relId <0 || relId > MAX_OPEN){
  return E_OUTOFBOUND;
}
//check if there are attributes related to that relation i.e if the relation is opened in the cache 
if(AttrCacheTable::attrCache[relId]==nullptr)
  return E_RELNOTOPEN;
//access the first attribute ka attrcacheentry for that relation
AttrCacheEntry *attrcacheentry=AttrCacheTable::attrCache[relId];
//all the attrcache entry of a relation are connected as a linked list so you traverse the ll
while(attrcacheentry){
  //look for the attribute whose searchindex you want to find 
  if(attrcacheentry->attrCatEntry.offset==attrOffset){
    //get the searchindex(the block number and the index at which the last hit was found)
    *searchIndex=attrcacheentry->searchIndex;
    return SUCCESS;
  }
  attrcacheentry=attrcacheentry->next;
}
return E_ATTRNOTEXIST;
}


//to set the searchindex of the attribute for that relation using the attributename 
int AttrCacheTable::setSearchIndex(int relId,char attrName[ATTR_SIZE],IndexId *searchIndex){
if(relId <0 || relId > MAX_OPEN){
  return E_OUTOFBOUND;
}
//check if there are attributes of that relation in the cache. i.e if the relation is open
if(AttrCacheTable::attrCache[relId]==nullptr)
  return E_RELNOTOPEN;
//access the first attribute ka attrcacheentry for that relation
AttrCacheEntry *attrcacheentry=AttrCacheTable::attrCache[relId];
//all the attrcache entry of a relation are connected as a linked list so you traverse the ll
while(attrcacheentry){
  //look for the attribute whose searchindex you want to find 
  if(strcmp(attrName,attrcacheentry->attrCatEntry.attrName)==0){
    //get the searchindex(the block number and the index at which the last hit was found)
   attrcacheentry->searchIndex= *searchIndex;
    return SUCCESS;
  }
  attrcacheentry=attrcacheentry->next;
}
return E_ATTRNOTEXIST;
}

//set the search index using the attribute offset value 
int AttrCacheTable::setSearchIndex(int relId,int attrOffset,IndexId *searchIndex){
if(relId <0 || relId > MAX_OPEN){
  return E_OUTOFBOUND;
}
//check if there are attributes related to that relation i.e if the relation is opened in the cache 
if(AttrCacheTable::attrCache[relId]==nullptr)
  return E_RELNOTOPEN;
//access the first attribute ka attrcacheentry for that relation
AttrCacheEntry *attrcacheentry=AttrCacheTable::attrCache[relId];
//all the attrcache entry of a relation are connected as a linked list so you traverse the ll
while(attrcacheentry){
  //look for the attribute whose searchindex you want to find 
  if(attrcacheentry->attrCatEntry.offset==attrOffset){
    //get the searchindex(the block number and the index at which the last hit was found)
   attrcacheentry->searchIndex= *searchIndex;
    return SUCCESS;
  }
  attrcacheentry=attrcacheentry->next;
}
return E_ATTRNOTEXIST;
}

//reset the search index using attrname
int AttrCacheTable::resetSearchIndex(int relId,char attrName[ATTR_SIZE]){
  IndexId searchindex={-1,- 1};
  int ret=setSearchIndex(relId,attrName,&searchindex);
  return ret;
}

//reset the search index using attroffset
int AttrCacheTable::resetSearchIndex(int relId,int attrOffset){
  IndexId searchindex={-1,- 1};
  int ret=setSearchIndex(relId,attrOffset,&searchindex);
  return ret;
}