#include "Algebra.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <cstring>
#include <iostream>

inline bool isNumber(char *str)
{
    int len;
    float ignore;
    int ret = sscanf(str, "%f %n", &ignore, &len);
    return ret == 1 && len == strlen(str);
}

//we want to get those record values which match the attrVal
int Algebra::select(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE], 
                    char attr[ATTR_SIZE], int op, char strVal[ATTR_SIZE]) 
{
    //get the relId
    int srcRelId = OpenRelTable::getRelId(srcRel); 
    //check if the relation is open
    if (srcRelId == E_RELNOTOPEN) 
        return E_RELNOTOPEN;

    //get the attrcatentry of that attr using the name 
    AttrCatEntry attrCatEntry;
    int ret = AttrCacheTable::getAttrCatEntry(srcRelId, attr, &attrCatEntry);
    //ensure that the attribute exists in the relation
    if (ret == E_ATTRNOTEXIST) 
        return E_ATTRNOTEXIST;

    /*** Convert strVal to an attribute of data type NUMBER or STRING ***/
    //get the type of the attribute
    int type = attrCatEntry.attrType;
    Attribute attrVal;
    if (type == NUMBER)
    {
            //if the value were comparing with can be converted to number 
        if (isNumber(strVal)) 
            //convert and store in attrVal
            attrVal.nVal = atof(strVal);
        else
            return E_ATTRTYPEMISMATCH;
    }
    else if (type == STRING)
        strcpy(attrVal.sVal, strVal);

    /******create the target relation*********/
    //get the relationmetadata of the relation whose records were checking
    RelCatEntry relCatEntryBuffer;
    RelCacheTable::getRelCatEntry(srcRelId, &relCatEntryBuffer);

    //get the number of attributes
    int srcNoAttrs =  relCatEntryBuffer.numAttrs;
    //create a 2d array that will store the attrnames for the target relation
    char srcAttrNames [srcNoAttrs][ATTR_SIZE];
    //create an array that will store the attrtypes of the target relation
    int srcAttrTypes [srcNoAttrs];

    //iterate through the number of attributes in the relation
    //populate the attrnames array and the attrtype array
    for (int i = 0; i < srcNoAttrs; i++) {
        //get the attribute metadata of each attribute in that relation using the offset 
        AttrCatEntry attrCatEntryBuffer;
        AttrCacheTable::getAttrCatEntry(srcRelId, i, &attrCatEntryBuffer);
        //populate the name array
        strcpy (srcAttrNames[i], attrCatEntryBuffer.attrName);
        //populate the attrtype array
        srcAttrTypes[i] = attrCatEntryBuffer.attrType;
    }
    //createrel passing the targetrelname,the number of attributes, the attrnames, the type of attributes 
    ret = Schema::createRel(targetRel, srcNoAttrs, srcAttrNames, srcAttrTypes);
    if (ret != SUCCESS) 
        return ret;
    
    //opening the relation using openrel from openrelcache and get the relid
    int targetRelId = OpenRelTable::openRel(targetRel);
    if (targetRelId < 0 || targetRelId >= MAX_OPEN) 
        return targetRelId;

    /*** Selecting and inserting records into the target relation ***/
    Attribute record[srcNoAttrs];
    //reset the search index for both to ensure ki you start from the first block
    RelCacheTable::resetSearchIndex(srcRelId);
    AttrCacheTable::resetSearchIndex(srcRelId, attr);

    //while there are records that match the searching 
    while (BlockAccess::search(srcRelId, record, attr, attrVal, op) == SUCCESS) 
    {
        //insert into targetrel
        ret = BlockAccess::insert(targetRelId, record);
        if (ret != SUCCESS) 
        {
            Schema::closeRel(targetRel);
            Schema::deleteRel(targetRel);
            return ret;
        }
    }

    // Close the targetRel by calling closeRel()
    Schema::closeRel(targetRel);

    return SUCCESS;
}


int Algebra::project(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE], int tar_nAttrs, char tar_Attrs[][ATTR_SIZE]) 
{
    int srcRelId = OpenRelTable::getRelId(srcRel); // srcRel's rel-id (use OpenRelTable::getRelId() function

    // if srcRel is not open in open relation table, return E_RELNOTOPEN
    if (srcRelId < 0 || srcRelId >= MAX_OPEN) return E_RELNOTOPEN;

    // get RelCatEntry of srcRel using RelCacheTable::getRelCatEntry()
    RelCatEntry relCatEntryBuffer;
    RelCacheTable::getRelCatEntry(srcRelId, &relCatEntryBuffer);

    // get the no. of attributes present in relation from the fetched RelCatEntry.
    int srcNoAttrs = relCatEntryBuffer.numAttrs;
    int attrOffset [tar_nAttrs];
    int attrTypes [tar_nAttrs];

    for (int attrIndex = 0; attrIndex < tar_nAttrs; attrIndex++) {
        attrOffset[attrIndex] = AttrCacheTable::getAttributeOffset(srcRelId, tar_Attrs[attrIndex]);
        if (attrOffset[attrIndex] < 0) return attrOffset[attrIndex];

        AttrCatEntry attrCatEntryBuffer;
        AttrCacheTable::getAttrCatEntry(srcRelId, tar_Attrs[attrIndex], &attrCatEntryBuffer);
        attrTypes[attrIndex] = attrCatEntryBuffer.attrType;
    }

    /*** Creating and opening the target relation ***/

    int ret = Schema::createRel(targetRel, tar_nAttrs, tar_Attrs, attrTypes);
    if (ret != SUCCESS) return ret;

    int targetRelId = OpenRelTable::openRel(targetRel);
    if (targetRelId < 0)
    {
        Schema::deleteRel (targetRel);
        return targetRelId;
    }

    /*** Inserting projected records into the target relation ***/

    // Take care to reset the searchIndex before calling the project function
    // using RelCacheTable::resetSearchIndex()
    RelCacheTable::resetSearchIndex(srcRelId);

    Attribute record[srcNoAttrs];

    while (BlockAccess::project(srcRelId, record) == SUCCESS) {
        // the variable `record` will contain the next record
        Attribute proj_record[tar_nAttrs];

        for (int attrIndex = 0; attrIndex < tar_nAttrs; attrIndex++)
            proj_record[attrIndex] = record[attrOffset[attrIndex]];


        ret = BlockAccess::insert(targetRelId, proj_record);

        if (ret != SUCCESS) {
            // close the targetrel by calling Schema::closeRel()
            // delete targetrel by calling Schema::deleteRel()
            // return ret;

            Schema::closeRel(targetRel);
            Schema::deleteRel(targetRel);

            return ret;
        }
    }

    // Close the targetRel by calling Schema::closeRel()
    Schema::closeRel(targetRel);

    return SUCCESS;
}

int Algebra::project(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE]) 
{
    int srcRelId = OpenRelTable::getRelId(srcRel); // srcRel's rel-id (use OpenRelTable::getRelId() function

    // if srcRel is not open in open relation table, return E_RELNOTOPEN
    if (srcRelId < 0 || srcRelId >= MAX_OPEN) return E_RELNOTOPEN;

    // get RelCatEntry of srcRel using RelCacheTable::getRelCatEntry()
    RelCatEntry relCatEntryBuffer;
    RelCacheTable::getRelCatEntry(srcRelId, &relCatEntryBuffer);

    // get the no. of attributes present in relation from the fetched RelCatEntry.
    int srcNoAttrs = relCatEntryBuffer.numAttrs;

    // attrNames and attrTypes will be used to store the attribute names
    // and types of the source relation respectively
    char attrNames[srcNoAttrs][ATTR_SIZE];
    int attrTypes[srcNoAttrs];

    /*iterate through every attribute of the source relation :
        - get the AttributeCat entry of the attribute with offset.
          (using AttrCacheTable::getAttrCatEntry())
        - fill the arrays `attrNames` and `attrTypes` that we declared earlier
          with the data about each attribute
    */

    for (int attrIndex = 0; attrIndex < srcNoAttrs; attrIndex++) {
        AttrCatEntry attrCatEntryBuffer;
        AttrCacheTable::getAttrCatEntry(srcRelId, attrIndex, &attrCatEntryBuffer);

        strcpy (attrNames[attrIndex], attrCatEntryBuffer.attrName);
        attrTypes[attrIndex] = attrCatEntryBuffer.attrType;
    }

    /*** Creating and opening the target relation ***/

    // Create a relation for target relation by calling Schema::createRel()
    int ret = Schema::createRel(targetRel, srcNoAttrs, attrNames, attrTypes);
    
    // if the createRel returns an error code, then return that value.
    if (ret != SUCCESS) return ret;

    // Open the newly created target relation by calling OpenRelTable::openRel()
    // and get the target relid
    int targetRelId = OpenRelTable::openRel(targetRel);

    // If opening fails, delete the target relation by calling Schema::deleteRel() of
    // return the error value returned from openRel().
    if (targetRelId < 0 || targetRelId >= MAX_OPEN) return targetRelId;

    /*** Inserting projected records into the target relation ***/

    // Take care to reset the searchIndex before calling the project function
    // using RelCacheTable::resetSearchIndex()
    RelCacheTable::resetSearchIndex(srcRelId);

    Attribute record[srcNoAttrs];

    while (BlockAccess::project(srcRelId, record) == SUCCESS)
    {
        // record will contain the next record

        ret = BlockAccess::insert(targetRelId, record);

        if (ret != SUCCESS) {
            // close the targetrel by calling Schema::closeRel()
            // delete targetrel by calling Schema::deleteRel()
            // return ret;

            Schema::closeRel(targetRel);
            Schema::deleteRel(targetRel);
            return ret;
        }
    }

    // Close the targetRel by calling Schema::closeRel()
    Schema::closeRel(targetRel);

    return SUCCESS;
}
int Algebra::insert(char relName[ATTR_SIZE], int nAttrs, char record[][ATTR_SIZE]){
    // if relName is equal to "RELATIONCAT" or "ATTRIBUTECAT"
    if (strcmp(relName, RELCAT_RELNAME) == 0 || strcmp(relName, ATTRCAT_RELNAME) == 0)
    return E_NOTPERMITTED;

    // get the relation's rel-id using OpenRelTable::getRelId() method
    int relId = OpenRelTable::getRelId(relName);

    // if relation is not open in open relation table, return E_RELNOTOPEN
    // (check if the value returned from getRelId function call = E_RELNOTOPEN)
    if (relId < 0 || relId >= MAX_OPEN) return E_RELNOTOPEN;

    // get the relation catalog entry from relation cache
    // (use RelCacheTable::getRelCatEntry() of Cache Layer)
    RelCatEntry relCatBuffer;
    RelCacheTable::getRelCatEntry(relId, &relCatBuffer);

    // if relCatEntry.numAttrs != numberOfAttributes in relation,
    if (relCatBuffer.numAttrs != nAttrs) return E_NATTRMISMATCH;

    // let recordValues[numberOfAttributes] be an array of type union Attribute
    Attribute recordValues[nAttrs];

    // TODO: Converting 2D char array of record values to Attribute array recordValues 
    // iterate through 0 to nAttrs-1: (let i be the iterator)
    for (int attrIndex = 0; attrIndex < nAttrs; attrIndex++)
    {
        // get the attr-cat entry for the i'th attribute from the attr-cache
        // (use AttrCacheTable::getAttrCatEntry())
        AttrCatEntry attrCatEntry;
        AttrCacheTable::getAttrCatEntry(relId, attrIndex, &attrCatEntry);

        int type = attrCatEntry.attrType;
        if (type == NUMBER)
        {
            // if the char array record[i] can be converted to a number
            // (check this using isNumber() function)
            if (isNumber(record[attrIndex]))
            {
                /* convert the char array to numeral and store it
                   at recordValues[i].nVal using atof() */
                recordValues[attrIndex].nVal = atof (record[attrIndex]);
            }
            else
                return E_ATTRTYPEMISMATCH;
        }
        else if (type == STRING)
        {
            // copy record[i] to recordValues[i].sVal
            strcpy((char *) &(recordValues[attrIndex].sVal), record[attrIndex]);
        }
    }

    // insert the record by calling BlockAccess::insert() function
    // let retVal denote the return value of insert call
    int ret = BlockAccess::insert(relId, recordValues);

    return ret;
}

int Algebra::join(char srcRelation1[ATTR_SIZE], char srcRelation2[ATTR_SIZE], 
            char targetRelation[ATTR_SIZE], char attribute1[ATTR_SIZE], 
            char attribute2[ATTR_SIZE]) 
{
    //get the srcrelid of the first relation
    int srcRelId1 = OpenRelTable::getRelId(srcRelation1);

    //get the relid of the second relation
    int srcRelId2 = OpenRelTable::getRelId(srcRelation2);


    if (srcRelId1 == E_RELNOTOPEN || srcRelId2 == E_RELNOTOPEN)
        return E_RELNOTOPEN;

    AttrCatEntry attrCatEntry1, attrCatEntry2;
    int ret = AttrCacheTable::getAttrCatEntry(srcRelId1, attribute1, &attrCatEntry1);
    if (ret != SUCCESS) 
        return E_ATTRNOTEXIST;

    ret = AttrCacheTable::getAttrCatEntry(srcRelId2, attribute2, &attrCatEntry2);
    if (ret != SUCCESS) 
        return E_ATTRNOTEXIST;

    if (attrCatEntry1.attrType != attrCatEntry2.attrType)
        return E_ATTRTYPEMISMATCH;

    RelCatEntry relCatEntryBuf1, relCatEntryBuf2;
    RelCacheTable::getRelCatEntry(srcRelId1, &relCatEntryBuf1);
    RelCacheTable::getRelCatEntry(srcRelId2, &relCatEntryBuf2);

    int numOfAttributes1 = relCatEntryBuf1.numAttrs;
    int numOfAttributes2 = relCatEntryBuf2.numAttrs;

    for (int i = 0; i < numOfAttributes1; i++)
    {
        AttrCatEntry attrCatEntryTemp1;
        AttrCacheTable::getAttrCatEntry(srcRelId1, i, &attrCatEntryTemp1);

        if (strcmp(attrCatEntryTemp1.attrName, attribute1) == 0) 
            continue;
        
        for (int j = 0; j < numOfAttributes2; j++)
        {
            AttrCatEntry attrCatEntryTemp2;
            AttrCacheTable::getAttrCatEntry(srcRelId2, j, &attrCatEntryTemp2);

            if (strcmp (attrCatEntryTemp2.attrName, attribute2) == 0) 
                continue;

            if (strcmp (attrCatEntryTemp1.attrName, attrCatEntryTemp2.attrName) == 0)
                return E_DUPLICATEATTR;
        }
    }

    int rootBlock = attrCatEntry2.rootBlock;
    if (rootBlock == -1)
    {
        ret = BPlusTree::bPlusCreate(srcRelId2, attribute2);
        if (ret == E_DISKFULL) 
            return E_DISKFULL;

        rootBlock = attrCatEntry2.rootBlock;
    }

    int numOfAttributesInTarget = numOfAttributes1 + numOfAttributes2 - 1;
    char targetRelAttrNames[numOfAttributesInTarget][ATTR_SIZE];
    int targetRelAttrTypes[numOfAttributesInTarget];

    for (int i = 0; i < numOfAttributes1; i++)
    {
        AttrCatEntry attrcatentry; 
        AttrCacheTable::getAttrCatEntry(srcRelId1, i, &attrcatentry);
        strcpy(targetRelAttrNames[i], attrcatentry.attrName);
        targetRelAttrTypes[i] = attrcatentry.attrType;
    }

    for (int i= 0, flag = 0; i < numOfAttributes2; i++)
    {
        AttrCatEntry attrcatentry; 
        AttrCacheTable::getAttrCatEntry(srcRelId2,i,&attrcatentry);

        if (strcmp(attribute2, attrcatentry.attrName) == 0)
        {
            flag = 1;
            continue;
        }

        strcpy(targetRelAttrNames[numOfAttributes1 + i-flag], attrcatentry.attrName);
        targetRelAttrTypes[numOfAttributes1 + i-flag] = attrcatentry.attrType;
    }    

    // create the target relation 
    ret = Schema::createRel(targetRelation, numOfAttributesInTarget, 
                                targetRelAttrNames, targetRelAttrTypes);

    if (ret != SUCCESS) 
        return ret;

    // Open the targetRelation
    int targetRelId = OpenRelTable::openRel(targetRelation);
    if (targetRelId < 0)
    {
        Schema::deleteRel(targetRelation);
        return targetRelId;
    }

    Attribute record1[numOfAttributes1];
    Attribute record2[numOfAttributes2];
    Attribute targetRecord[numOfAttributesInTarget];

    RelCacheTable::resetSearchIndex(srcRelId1);

    while (BlockAccess::project(srcRelId1, record1) == SUCCESS) 
    {

        RelCacheTable::resetSearchIndex(srcRelId2);

        // reset the search index of `attribute2` in the attribute cache
        // using AttrCacheTable::resetSearchIndex()
        AttrCacheTable::resetSearchIndex(srcRelId2, attribute2);
        while (BlockAccess::search(srcRelId2, record2, attribute2, 
                                    record1[attrCatEntry1.offset], EQ) == SUCCESS) 
        {

            //copy the first relations record values 
            for (int i = 0; i < numOfAttributes1; i++)
                targetRecord[i] = record1[i];

            //copy the corresponding second relations record values except for the attribute 
            for (int i = 0, flag = 0; i < numOfAttributes2; i++)
            {
                if (i == attrCatEntry2.offset)
                {
                    flag = 1;
                    continue;
                }
                targetRecord[i + numOfAttributes1-flag] = record2[i];
            }


            ret = BlockAccess::insert(targetRelId, targetRecord);

            if (ret == E_DISKFULL)
            {
                ret = OpenRelTable::closeRel(targetRelId);
                ret = Schema::deleteRel(targetRelation);

                return E_DISKFULL;
            }
        }
    }

    return SUCCESS;
}