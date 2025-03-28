#include "Algebra.h"
#include <iostream>
#include <cstring>
#include <cstdio>
#include <cstdlib>

bool isNumber(char *str) {
  int len;
  float ignore;
  /*
    sscanf returns the number of elements read, so if there is no float matching
    the first %f, ret will be 0, else it'll be 1

    %n gets the number of characters read. this scanf sequence will read the
    first float ignoring all the whitespace before and after. and the number of
    characters read that far will be stored in len. if len == strlen(str), then
    the string only contains a float with/without whitespace. else, there's other
    characters.
  */
  int ret = sscanf(str, "%f %n", &ignore, &len);
  return ret == 1 && len == strlen(str);
}




/* used to select all the records that satisfy a condition.
the arguments of the function are
- srcRel - the source relation we want to select from
- targetRel - the relation we want to select into. 
- attr - the attribute that the condition is checking
- op - the operator of the condition
- strVal - the value that we want to compare against (represented as a string)
*/
int Algebra::select(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE], char attr[ATTR_SIZE], int op, char strVal[ATTR_SIZE]) {
  //get the sourcerelid and check if it exists
  int srcRelId = OpenRelTable::getRelId(srcRel);      // we'll implement this later
  if (srcRelId == E_RELNOTOPEN) {
    return E_RELNOTOPEN;
  }

  AttrCatEntry attrCatEntry;
//get the attribute metadata for the attribute we want to check and check if it exists 
  int ret=AttrCacheTable::getAttrCatEntry(srcRelId, attr, &attrCatEntry);
  if(ret== E_ATTRNOTEXIST){
    return E_ATTRNOTEXIST;
  }

//convert the value that we want to check into string or number and enusre that it is compatible
  int type = attrCatEntry.attrType;
  Attribute attrVal;
  if (type == NUMBER) {
    if (isNumber(strVal)) {       // the isNumber() function is implemented below
      attrVal.nVal = atof(strVal);
    } else {
      return E_ATTRTYPEMISMATCH;
    }
  } else if (type == STRING) {
    strcpy(attrVal.sVal, strVal);
  }

/*to create the target relation*/
//get the relation mwetadata of the source relation
  RelCatEntry relCatEntry;
  RelCacheTable::getRelCatEntry(srcRelId, &relCatEntry);

//get the number of attributes
 int src_nAttrs = relCatEntry.numAttrs;
 //create a 2d array that will store the names of the attributes of the relation
 char attr_names[src_nAttrs][ATTR_SIZE];
 //create an array that will store the attr types for each attribute
 int attr_types[src_nAttrs];

 for(int i=0;i<src_nAttrs;i++){
  AttrCatEntry attrcatentry;
  //get the metadata of each attribute 
  AttrCacheTable::getAttrCatEntry(srcRelId,i,&attrcatentry);
  //copy the metadata-attribute names in the new array
  strcpy(attr_names[i],attrcatentry.attrName);
  //get the attribute type of each attribute 
  attr_types[i]=attrcatentry.attrType;
 }

/**create the relation*/
 int retval=Schema::createRel(targetRel,src_nAttrs,attr_names,attr_types);
 if(retval!= SUCCESS){
  Schema::deleteRel(targetRel);
  return retval;
 }

/**open the newly created relation */
int targetrelid = OpenRelTable::openRel(targetRel);
if(targetrelid < 0){
   // Schema::deleteRel(targetRel);
    return targetrelid;
}

Attribute record[src_nAttrs];
/****select and insert the records into the target relation*****/
RelCacheTable::resetSearchIndex(srcRelId);
AttrCacheTable::resetSearchIndex(srcRelId,attr);


while(BlockAccess::search(srcRelId,record,attr,attrVal,op)==SUCCESS){
  int rev=BlockAccess::insert(targetrelid,record);
  if(rev!=SUCCESS){
    Schema::closeRel(targetRel);
    Schema::deleteRel(targetRel);
    return rev;
  }
}
  Schema::closeRel(targetRel);
  return SUCCESS;
}

/*---------PROJECT TO CLONE THE ENTIRE RELATION-------------*/
//make a copy of the source relation and create a new relation called target relation
//basically copy all the records of src relation into target relation
int Algebra::project(char srcRel[ATTR_SIZE],char targetRel[ATTR_SIZE]){
  //access the relation id of the source relation
  int srcRelId=OpenRelTable::getRelId(srcRel);
  //check if the relation is open
  if(srcRelId==E_RELNOTOPEN)
    return E_RELNOTOPEN;
  
  //get the relcat entry of the src relation
  RelCatEntry relcatbuf;
  RelCacheTable::getRelCatEntry(srcRelId,&relcatbuf);

//access the number of attributes in the srcrelation
  int numAttrs=relcatbuf.numAttrs;
  //create a temporary array to store each attributes name and type 
  char attrnames[numAttrs][ATTR_SIZE];
  int attrTypes[numAttrs];

//iterate through the attributes
  for(int i=0;i<numAttrs;i++){
    AttrCatEntry attrcatbuf;
// go over each attribute and copy the content into the arrays
    AttrCacheTable::getAttrCatEntry(srcRelId,i,&attrcatbuf);
    strcpy(attrnames[i],attrcatbuf.attrName);
    attrTypes[i]=attrcatbuf.attrType;
  }

//create the new relation with the structure of the src relation
//basically update the relationcat and attrcat with the relname and the attrname and attrtypes respectively
  int ret=Schema::createRel(targetRel,numAttrs,attrnames,attrTypes);
  if(ret!= SUCCESS)
    return ret;

//open the relation and get the relId 
    int targetrelId=OpenRelTable::openRel(targetRel);
   if(targetrelId < 0){
    Schema::deleteRel(targetRel);
    return targetrelId;
   }

//insert the records in the table
   RelCacheTable::resetSearchIndex(srcRelId);

//create an empty record structure that will take the record
   Attribute record[numAttrs];
   //iterate through all the records in that relation and store it in record
   while(BlockAccess::project(srcRelId,record)==SUCCESS){
    int ret=BlockAccess::insert(targetrelId,record);
    //insert whatever is stored in the record into the targetrelid
  //if it fails then close the relation and delete the relation
    if(ret!=SUCCESS){
      Schema::closeRel(targetRel);
      Schema::deleteRel(targetRel);
      return ret;
    }
   }
  Schema::closeRel(targetRel);
   return SUCCESS;
}

int Algebra::project(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE],int tar_nAttrs, char tar_Attrs[][ATTR_SIZE]){
  int srcRelId = OpenRelTable::getRelId(srcRel);
  if(srcRelId < 0){
   return E_RELNOTOPEN;
  }

  RelCatEntry relcatbuf;
  RelCacheTable::getRelCatEntry(srcRelId,&relcatbuf);
  int src_nattrs=relcatbuf.numAttrs;

//will store the offsets of the attributes ex: attr_offset[0]=4(stores the attribute at 4th index in srcrelation)
  int attr_offset[tar_nAttrs];
  int attr_types[tar_nAttrs];

    for (int i = 0; i < tar_nAttrs; i++) {
       // attr_offset[i] = AttrCacheTable::getAttributeOffset(srcRelId, tar_Attrs[i]);
      //  if (attr_offset[i] < 0) 
       //   return attr_offset[i];

        AttrCatEntry attrCatEntryBuffer;
        AttrCacheTable::getAttrCatEntry(srcRelId, tar_Attrs[i], &attrCatEntryBuffer);
        attr_offset[i]=attrCatEntryBuffer.offset;
        attr_types[i] = attrCatEntryBuffer.attrType;
    }

    int ret=Schema::createRel(targetRel,tar_nAttrs,tar_Attrs,attr_types);
      if(ret!=SUCCESS)
        return ret;
      
      int targetrelid=OpenRelTable::openRel(targetRel);
      if(targetrelid <0){
        Schema::deleteRel(targetRel);
        return targetrelid;
      }

      RelCacheTable::resetSearchIndex(srcRelId);


//record can be thought of as an array and at each index it stores the value of that attribute offset
      Attribute record[src_nattrs];
      //the record has the whole record where rec[0]= name,rec[1]=roll etc
      while(BlockAccess::project(srcRelId,record)==SUCCESS){
        //create a smaller array in which only those record values will be for the attrbiutes needed
        Attribute proj_record[tar_nAttrs];

//
        for(int i=0;i<tar_nAttrs;i++){
          //attr_offset[i] will give you the offset to the first attribute we want to include 
          //record[that value]= attribute value 
          proj_record[i]=record[attr_offset[i]];
        }
        int rev=BlockAccess::insert(targetrelid,proj_record);
        if(rev!=SUCCESS){
          Schema::closeRel(targetRel);
          Schema::deleteRel(targetRel);
          return rev;
        }
      }
      Schema::closeRel(targetRel);
      return SUCCESS;
}


int Algebra::insert(char relName[ATTR_SIZE], int nAttrs, char record[][ATTR_SIZE]){
    // if relName is equal to "RELATIONCAT" or "ATTRIBUTECAT"
    // return E_NOTPERMITTED;

    if(strcmp(relName,RELCAT_RELNAME) == 0 || strcmp(relName,ATTRCAT_RELNAME) == 0){
        return E_NOTPERMITTED;
    }

    // get the relation's rel-id using OpenRelTable::getRelId() method
    int relId = OpenRelTable::getRelId(relName);

    // if relation is not open in open relation table, return E_RELNOTOPEN
    // (check if the value returned from getRelId function call = E_RELNOTOPEN)
    if(relId == E_RELNOTOPEN){
        return E_RELNOTOPEN;
    }

    RelCatEntry relCatEntry;
    RelCacheTable::getRelCatEntry(relId, &relCatEntry);

//check if we have the same number of values
    if(relCatEntry.numAttrs != nAttrs){
        return E_NATTRMISMATCH;
    }

    // let recordValues[numberOfAttributes] be an array of type union Attribute

    Attribute recordValues[nAttrs];

    /*
        Converting 2D char array of record values to Attribute array recordValues
     */
    // iterate through 0 to nAttrs-1: (let i be the iterator)
    for(int i = 0 ; i < nAttrs ; i++){
        // get the attr-cat entry for the i'th attribute from the attr-cache
        // (use AttrCacheTable::getAttrCatEntry())
        AttrCatEntry attrCatEntry;
        AttrCacheTable::getAttrCatEntry(relId, i, &attrCatEntry);
        int type = attrCatEntry.attrType;

        if (type == NUMBER)
        {
            if(isNumber(record[i]) == true)
            {
                recordValues[i].nVal = atof(record[i]);
            }
            else
            {
                return E_ATTRTYPEMISMATCH;
            }
        }
        else if (type == STRING)
        {
            strcpy(recordValues[i].sVal, record[i]);
        }
    }

//prepare the record which will be inserted into the relation
    int retVal = BlockAccess::insert(relId, recordValues);

    return retVal;
}







