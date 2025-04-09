#include "BPlusTree.h"

#include <cstring>

RecId BPlusTree::bPlusSearch(int relId, char attrName[ATTR_SIZE], 
                                Attribute attrVal, int op) 
{
    IndexId searchIndex;
    //get the search index for that attr
    int ret = AttrCacheTable::getSearchIndex(relId, attrName, &searchIndex);

    //get the attrcatentry of that attr
    AttrCatEntry attrCatEntry;
    ret = AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);

    // declare variables block and index which will be used during search
    int block = -1, index = -1;

    if (searchIndex.block ==-1 && searchIndex.index ==-1)
    {
        //get the rootblock i.e the first block
        block = attrCatEntry.rootBlock;
        //get the first index
        index = 0;
        //no b+ tree for that attr
        if (block == -1) 
            return RecId{-1, -1};

    } 
    else 
    {
        block = searchIndex.block;
        index = searchIndex.index + 1;  // search is resumed from the next index.

        // load block into leaf using IndLeaf::IndLeaf().
        IndLeaf leaf(block);

        // declare leafHead which will be used to hold the header of leaf.
        HeadInfo leafHead;

        // load header into leafHead using BlockBuffer::getHeader().
        leaf.getHeader(&leafHead);
        
        //check if the index values exceeds the number of indices in the block move to next block
        if (index >= leafHead.numEntries) {
            block = leafHead.rblock;
            index = 0;

            if (block == -1) {
                return RecId{-1, -1};
            }
        }
    }

    while(StaticBuffer::getStaticBlockType(block) == IND_INTERNAL) 
    {  
        // load the block into internalBlk 
        IndInternal internalBlk(block);

        // load the header of internalBlk 
        HeadInfo intHead;
        internalBlk.getHeader(&intHead);
        
        //declare an internalEntry sturcture 
        InternalEntry intEntry;

        if (op == NE || op == LT || op == LE) 
        {
            //if less than we want to start from the first entry in the left most leaf node cause that will be the smallest attrVal
            internalBlk.getEntry(&intEntry, 0);
            block = intEntry.lChild;

        } 
        
        else 
        {
            //iterate through all the indices in the block to find the val just greater than the attrVal
            int entryindex = 0;
            while (entryindex < intHead.numEntries)
            {
                //get each index ka entry in the intEntry structure 
                ret = internalBlk.getEntry(&intEntry, entryindex);
                //compare the attrVal and the entry of that index 
                int cmpVal = compareAttrs(intEntry.attrVal, attrVal, attrCatEntry.attrType);
                if (
                    (op == EQ && cmpVal >= 0) ||
                    (op == GE && cmpVal >= 0) ||
                    (op == GT && cmpVal > 0)
                )
                    break;

                entryindex++;
            }

            // if (/* such an entry is found*/) 
            if (entryindex < intHead.numEntries)
            {
                // move to the left child of that entry
                block = intEntry.lChild; 
            }
            else 
            {
                //move to the right most leaf node which will have all greater values
                block = intEntry.rChild; 
            }
        }
    }

// `block` now has the block number of a leaf index block.
//now that we have the leaf block we need to find the correct index value 
    while (block != -1) {
        // load the block 
        IndLeaf leafBlk(block);

        // load the header.
        HeadInfo leafHead;
        leafBlk.getHeader(&leafHead);

        // declare leafEntry 
        Index leafEntry;
        //iterate through the leafentries 
        while (index < leafHead.numEntries)
        {
            //get the entry of each index in that 
            leafBlk.getEntry(&leafEntry, index);

            // comparison between leafEntry's attribute value and input attrVal using compareAttrs()
            int cmpVal = compareAttrs(leafEntry.attrVal, attrVal, attrCatEntry.attrType); 

            if (
                (op == EQ && cmpVal == 0) ||
                (op == LE && cmpVal <= 0) ||
                (op == GE && cmpVal >= 0) ||
                (op == LT && cmpVal < 0) ||
                (op == GT && cmpVal > 0) ||
                (op == NE && cmpVal != 0)
            ) {
                // set search index to {block, index}
                searchIndex = IndexId{block, index};
                AttrCacheTable::setSearchIndex(relId, attrName, &searchIndex);

                // return the recId {leafEntry.block, leafEntry.slot}.
                return RecId{leafEntry.block, leafEntry.slot};
            } 
            else if ((op == EQ || op == LE || op == LT) && cmpVal > 0) 
            {
                //all the values ahead are hreater than so no point in iterating through the linked list
                return RecId {-1, -1};
            }

            // search next index.
            ++index;
        }
 
        if (op != NE) {
            break;
        }
        block = leafHead.rblock, index = 0;
    }
    return RecId{-1, -1};
}

/******bPlusCreate***********/
int BPlusTree::bPlusCreate(int relId, char attrName[ATTR_SIZE]) 
{
    // if relId is either RELCAT_RELID or ATTRCAT_RELID:
    //     return E_NOTPERMITTED;
    if (relId == RELCAT_RELID || relId == ATTRCAT_RELID)
        return E_NOTPERMITTED;

    // get the attribute catalog entry of attribute `attrName`
    // using AttrCacheTable::getAttrCatEntry()
    AttrCatEntry attrCatEntryBuffer;
    int ret = AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntryBuffer);

    // if getAttrCatEntry fails
    //     return the error code from getAttrCatEntry
    if (ret != SUCCESS) 
        return ret;

    // if (/* an index already exists for the attribute (check rootBlock field) */) 
    if (attrCatEntryBuffer.rootBlock != -1)
        return SUCCESS;

    /******Creating a new B+ Tree ******/

    // get a free leaf block using constructor 1 to allocate a new block
    IndLeaf rootBlockBuf;

    // (if the block could not be allocated, the appropriate error code
    //  will be stored in the blockNum member field of the object)

    // declare rootBlock to store the blockNumber of the new leaf block
    int rootBlock = rootBlockBuf.getBlockNum();

    // if there is no more disk space for creating an index
    if (rootBlock == E_DISKFULL) return E_DISKFULL;

    attrCatEntryBuffer.rootBlock = rootBlock;
    AttrCacheTable::setAttrCatEntry(relId, attrName, &attrCatEntryBuffer);

    RelCatEntry relCatEntryBuffer;

    // load the relation catalog entry into relCatEntry
    // using RelCacheTable::getRelCatEntry().
    RelCacheTable::getRelCatEntry(relId, &relCatEntryBuffer);
    int block = relCatEntryBuffer.firstBlk; // first record block of the relation 

    /***** Traverse all the blocks in the relation and insert them one
           by one into the B+ Tree *****/
    while (block != -1) {
        // declare a RecBuffer object for `block` (using appropriate constructor)
        RecBuffer blockBuffer (block);

        // load the slot map into slotMap using RecBuffer::getSlotMap().
        unsigned char slotmap[relCatEntryBuffer.numSlotsPerBlk];
        blockBuffer.getSlotMap(slotmap);

        for (int slot = 0; slot < relCatEntryBuffer.numSlotsPerBlk; slot++)
        {
            if (slotmap[slot] == SLOT_OCCUPIED)
            {
                Attribute record[relCatEntryBuffer.numAttrs];
                // load the record corresponding to the slot into `record`
                // using RecBuffer::getRecord().
                blockBuffer.getRecord(record, slot);

                // declare recId and store the rec-id of this record in it
                // RecId recId{block, slot};
                RecId recId = RecId{block, slot};


                ret = bPlusInsert(relId, attrName, 
                                    record[attrCatEntryBuffer.offset], recId);

                if (ret == E_DISKFULL) {
                    // (unable to get enough blocks to build the B+ Tree.)
                    return E_DISKFULL;
                }
            }
        }
        
        // get the header of the block using BlockBuffer::getHeader()
        HeadInfo blockHeader;
        blockBuffer.getHeader(&blockHeader);

        // set block = rblock of current block (from the header)
        block = blockHeader.rblock;
    }

    return SUCCESS;
}

int BPlusTree::bPlusInsert(int relId, char attrName[ATTR_SIZE], 
                            Attribute attrVal, RecId recId) {
    // get the attribute cache entry corresponding to attrName
    // using AttrCacheTable::getAttrCatEntry().
    AttrCatEntry attrCatEntryBuffer;
    int ret = AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntryBuffer);

    // if getAttrCatEntry() failed
    //     return the error code
    if (ret != SUCCESS) return ret;

    int rootBlock = attrCatEntryBuffer.rootBlock; // rootBlock of B+ Tree 

    // if (/* there is no index on attribute (rootBlock is -1) */) 
    if (rootBlock == -1) return E_NOINDEX;

    // find the leaf block to which insertion is to be done using the
    // findLeafToInsert() function

    int leafBlkNum = findLeafToInsert(rootBlock, attrVal, attrCatEntryBuffer.attrType); // findLeafToInsert(root block num, attrVal, attribute type) 

    Index indexEntry; 
    indexEntry.attrVal = attrVal, indexEntry.block = recId.block, indexEntry.slot = recId.slot;
    
    // if (/*insertIntoLeaf() returns E_DISKFULL */) 
    if (insertIntoLeaf(relId, attrName, leafBlkNum, indexEntry) == E_DISKFULL)
    {
        // destroy the existing B+ tree by passing the rootBlock to bPlusDestroy().
        BPlusTree::bPlusDestroy(rootBlock);

        // update the rootBlock of attribute catalog cache entry to -1 using
        // AttrCacheTable::setAttrCatEntry().
        attrCatEntryBuffer.rootBlock = -1;
        AttrCacheTable::setAttrCatEntry(relId, attrName, &attrCatEntryBuffer);

        return E_DISKFULL;
    }

    return SUCCESS;
}

int BPlusTree::findLeafToInsert(int rootBlock, Attribute attrVal, int attrType) {
    int blockNum = rootBlock;

    // while (/*block is not of type IND_LEAF */) // use StaticBuffer::getStaticBlockType()
    while (StaticBuffer::getStaticBlockType(blockNum) != IND_LEAF) 
    {  
        // declare an IndInternal object for block using appropriate constructor
        IndInternal internalBlock (blockNum);

        // get header of the block using BlockBuffer::getHeader()
        HeadInfo blockHeader;
        internalBlock.getHeader(&blockHeader);
        int index = 0;
        while (index < blockHeader.numEntries)
        {
            InternalEntry entry;
            internalBlock.getEntry(&entry, index);

            if (compareAttrs(attrVal, entry.attrVal, attrType) <= 0)
                break;

            index++;
        }


        if (index == blockHeader.numEntries) 
        {
            InternalEntry entry;
            internalBlock.getEntry(&entry, blockHeader.numEntries-1);

            blockNum = entry.rChild;
        } 
        else 
        {
            InternalEntry entry;
            internalBlock.getEntry(&entry, index);

            blockNum = entry.lChild;
        }
    }

    return blockNum;
}

int BPlusTree::insertIntoLeaf(int relId, char attrName[ATTR_SIZE], int leafBlockNum, Index indexEntry) 
{
    // get the attribute cache entry corresponding to attrName
    // using AttrCacheTable::getAttrCatEntry().
    AttrCatEntry attrCatEntryBuffer;
    AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntryBuffer);

    // declare an IndLeaf instance for the block using appropriate constructor
    IndLeaf leafBlock (leafBlockNum);

    HeadInfo blockHeader;
    // store the header of the leaf index block into blockHeader
    // using BlockBuffer::getHeader()
    leafBlock.getHeader(&blockHeader);

    // the following variable will be used to store a list of index entries with
    // existing indices + the new index to insert
    Index indices[blockHeader.numEntries + 1];

    bool inserted = false;
    for (int entryindex = 0; entryindex < blockHeader.numEntries; entryindex++)
    {
        Index entry;
        leafBlock.getEntry(&entry, entryindex);

        if (compareAttrs(entry.attrVal, indexEntry.attrVal, attrCatEntryBuffer.attrType) <= 0)
        {
            // the current entry element is lesser than the one to be inserted
            indices[entryindex] = entry;
        }
        else
        {
            // the indexEntry is to be placed here
            indices[entryindex] = indexEntry;
            // insertmarker++;
            inserted = true;

            for (entryindex++; entryindex <= blockHeader.numEntries; entryindex++)
            {
                leafBlock.getEntry(&entry, entryindex-1);
                indices[entryindex] = entry;
            }
            break;
        }
    }

    // adding the last element in indices
    if (!inserted) indices[blockHeader.numEntries] = indexEntry;

    if (blockHeader.numEntries < MAX_KEYS_LEAF) {
        // (leaf block has not reached max limit)

        // increment blockHeader.numEntries and update the header of block
        // using BlockBuffer::setHeader().
        blockHeader.numEntries++;
        leafBlock.setHeader(&blockHeader);

        // iterate through all the entries of the array `indices` and populate the
        // entries of block with them using IndLeaf::setEntry().

        for (int indicesIt = 0; indicesIt < blockHeader.numEntries; indicesIt++)
            leafBlock.setEntry(&indices[indicesIt], indicesIt);

        return SUCCESS;
    }

    int newRightBlk = splitLeaf(leafBlockNum, indices);
    if (newRightBlk == E_DISKFULL) return E_DISKFULL;

    if (blockHeader.pblock != -1)
    {
        InternalEntry middleEntry;
        middleEntry.attrVal = indices[MIDDLE_INDEX_LEAF].attrVal, 
        middleEntry.lChild = leafBlockNum, middleEntry.rChild = newRightBlk;

        // insertIntoInternal(relId, attrName, parent of current block, new internal entry)
        return insertIntoInternal(relId, attrName, blockHeader.pblock, middleEntry);
        // // if (ret == E_DISKFULL) return E_DISKFULL;
    } 
    else 
    {
        if(
            createNewRoot(relId, attrName, indices[MIDDLE_INDEX_LEAF].attrVal, 
                            leafBlockNum, newRightBlk) == E_DISKFULL
        )
            return E_DISKFULL;
    }

    return SUCCESS;
}

int BPlusTree::splitLeaf(int leafBlockNum, Index indices[]) {
    // declare rightBlk, an instance of IndLeaf using constructor 1 to obtain new
    // leaf index block that will be used as the right block in the splitting
    IndLeaf rightBlock;

    // declare leftBlk, an instance of IndLeaf using constructor 2 to read from
    // the existing leaf block
    IndLeaf leftBlock(leafBlockNum);

    int rightBlockNum = rightBlock.getBlockNum();
    int leftBlockNum = leftBlock.getBlockNum();

    if (rightBlockNum == E_DISKFULL) 
    {
        //(failed to obtain a new leaf index block because the disk is full)
        return E_DISKFULL;
    }

    // get the headers of left block and right block using BlockBuffer::getHeader()
    HeadInfo leftBlockHeader, rightBlockHeader;
    leftBlock.getHeader(&leftBlockHeader);
    rightBlock.getHeader(&rightBlockHeader);

    
    rightBlockHeader.blockType = leftBlockHeader.blockType, 
    rightBlockHeader.numEntries = (MAX_KEYS_LEAF+1)/2;
    
    rightBlockHeader.pblock = leftBlockHeader.pblock, 
    rightBlockHeader.lblock = leftBlockNum, 
    rightBlockHeader.rblock = leftBlockHeader.rblock;

    rightBlock.setHeader(&rightBlockHeader);


    leftBlockHeader.numEntries = (MAX_KEYS_LEAF+1)/2;
    leftBlockHeader.rblock = rightBlockNum;

    leftBlock.setHeader(&leftBlockHeader);

    for (int entryindex = 0; entryindex <= MIDDLE_INDEX_LEAF; entryindex++)
    {
        leftBlock.setEntry(&indices[entryindex], entryindex);
        rightBlock.setEntry(&indices[entryindex + MIDDLE_INDEX_LEAF+1], entryindex);
    }

    return rightBlockNum;
}

int BPlusTree::insertIntoInternal(int relId, char attrName[ATTR_SIZE], 
                                    int intBlockNum, InternalEntry intEntry) {
    // get the attribute cache entry corresponding to attrName
    // using AttrCacheTable::getAttrCatEntry().
    AttrCatEntry attrCatEntryBuffer;
    AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntryBuffer);

    // declare intBlk, an instance of IndInternal using constructor 2 for the block
    // corresponding to intBlockNum
    IndInternal internalBlock (intBlockNum);

    HeadInfo blockHeader;
    // load blockHeader with header of intBlk using BlockBuffer::getHeader().
    internalBlock.getHeader(&blockHeader);

    // declare internalEntries to store all existing entries + the new entry
    InternalEntry internalEntries[blockHeader.numEntries + 1];
    // bool inserted = false;
    int insertedIndex = -1;
    for (int entryindex = 0; entryindex < blockHeader.numEntries; entryindex++)
    {
        InternalEntry internalBlockEntry;
        internalBlock.getEntry(&internalBlockEntry, entryindex);

        if (compareAttrs(internalBlockEntry.attrVal, intEntry.attrVal, attrCatEntryBuffer.attrType) <= 0)
        {
            // the new value is lesser (or equal to), hence does not go here
            internalEntries[entryindex] = internalBlockEntry;
        }
        else 
        {
            internalEntries[entryindex] = intEntry;
            insertedIndex = entryindex;
            for (entryindex++; entryindex <= blockHeader.numEntries; entryindex++)
            {
                internalBlock.getEntry(&internalBlockEntry, entryindex-1);
                internalEntries[entryindex] = internalBlockEntry;
            }

            break;
        }
    }

    if (insertedIndex == -1) {
        internalEntries[blockHeader.numEntries] = intEntry;
        insertedIndex = blockHeader.numEntries;
    }

    // setting the previous entry's rChild to lChild of `intEntry`
    if (insertedIndex > 0)
    {
        // InternalEntry entry;
        // internalBlock.getEntry(&entry, insertedIndex-1);
        internalEntries[insertedIndex-1].rChild = intEntry.lChild;
    }

    // setting the following entry's lChild to rChild of `intEntry`
    if (insertedIndex < blockHeader.numEntries)
    {
        // InternalEntry entry;
        // internalBlock.getEntry(&entry, insertedIndex+1);
        internalEntries[insertedIndex+1].lChild = intEntry.rChild;
    }


    if (blockHeader.numEntries < MAX_KEYS_INTERNAL) {
        // (internal index block has not reached max limit)

        // increment blockheader.numEntries and update the header of intBlk
        // using BlockBuffer::setHeader().
        blockHeader.numEntries++;
        internalBlock.setHeader(&blockHeader);

        // iterate through all entries in internalEntries array and populate the
        // entries of intBlk with them using IndInternal::setEntry().
        for (int entryindex = 0; entryindex < blockHeader.numEntries; entryindex++)
            internalBlock.setEntry(&internalEntries[entryindex], entryindex);

        return SUCCESS;
    }

    // If we reached here, the `internalEntries` array has more than entries than
    // can fit in a single internal index block. Therefore, we will need to split
    // the entries in `internalEntries` between two internal index blocks. We do
    // this using the splitInternal() function.
    // This function will return the blockNum of the newly allocated block or
    // E_DISKFULL if there are no more blocks to be allocated.

    int newRightBlk = splitInternal(intBlockNum, internalEntries);

    // if (/* splitInternal() returned E_DISKFULL */) 
    if (newRightBlk == E_DISKFULL)
    {
        // Using bPlusDestroy(), destroy the right subtree, rooted at intEntry.rChild.
        // This corresponds to the tree built up till now that has not yet been
        // connected to the existing B+ Tree

        BPlusTree::bPlusDestroy(intEntry.rChild);

        return E_DISKFULL;
    }

    // if the current block was not the root  
    if (blockHeader.pblock != -1) // (check pblock in header)
    {  
        InternalEntry middleEntry;
        middleEntry.lChild = intBlockNum, middleEntry.rChild = newRightBlk;

        // the middle value will be at index 50 (given by constant MIDDLE_INDEX_INTERNAL)
        middleEntry.attrVal = internalEntries[MIDDLE_INDEX_INTERNAL].attrVal;

        // insertIntoInternal(relId, attrName, parent of current block, new internal entry)
        return insertIntoInternal(relId, attrName, blockHeader.pblock, middleEntry);
        // // if (ret == E_DISKFULL) return E_DISKFULL;
    } 
    else 
    {
        // the current block was the root block and is now split. a new internal index
        // block needs to be allocated and made the root of the tree.
        // To do this, call the createNewRoot() function with the following arguments

        return createNewRoot(relId, attrName, internalEntries[MIDDLE_INDEX_INTERNAL].attrVal, 
                                intBlockNum, newRightBlk);

    }

    // if either of the above calls returned an error (E_DISKFULL), then return that
    // else return SUCCESS
    return SUCCESS;
}

int BPlusTree::splitInternal(int intBlockNum, InternalEntry internalEntries[]) 
{
    // declare rightBlk, an instance of IndInternal using constructor 1 to obtain new
    // internal index block that will be used as the right block in the splitting
    IndInternal rightBlock;

    // declare leftBlk, an instance of IndInternal using constructor 2 to read from
    // the existing internal index block
    IndInternal leftBlock (intBlockNum);

    int leftBlockNum = leftBlock.getBlockNum();
    int rightBlockNum = rightBlock.getBlockNum();

    // if newly allocated block has blockNum E_DISKFULL 
    if (rightBlockNum == E_DISKFULL)
    {
        //(failed to obtain a new internal index block because the disk is full)
        return E_DISKFULL;
    }

    HeadInfo leftBlockHeader, rightBlockHeader;

    // get the headers of left block and right block using BlockBuffer::getHeader()
    leftBlock.getHeader(&leftBlockHeader);
    rightBlock.getHeader(&rightBlockHeader);
    
    rightBlockHeader.numEntries = MAX_KEYS_INTERNAL / 2;
    rightBlockHeader.pblock = leftBlockHeader.pblock;

    rightBlock.setHeader(&rightBlockHeader);
    leftBlockHeader.numEntries = MAX_KEYS_INTERNAL / 2;
    leftBlockHeader.rblock = rightBlockNum;

    leftBlock.setHeader(&leftBlockHeader);



    for (int entryindex = 0; entryindex < MIDDLE_INDEX_INTERNAL; entryindex++)
    {
        leftBlock.setEntry(&internalEntries[entryindex], entryindex);
        rightBlock.setEntry(&internalEntries[entryindex + MIDDLE_INDEX_INTERNAL + 1], entryindex);
    }

    /* block type of a child of any entry of the internalEntries array */;
    //            (use StaticBuffer::getStaticBlockType())
    int type = StaticBuffer::getStaticBlockType(internalEntries[0].lChild);

    // for (/* each child block of the new right block */) 
    BlockBuffer blockbuffer (internalEntries[MIDDLE_INDEX_INTERNAL+1].lChild);

    HeadInfo blockHeader;
    blockbuffer.getHeader(&blockHeader);

    blockHeader.pblock = rightBlockNum;
    blockbuffer.setHeader(&blockHeader);

    for (int entryindex = 0; entryindex < MIDDLE_INDEX_INTERNAL; entryindex++)
    {
        // declare an instance of BlockBuffer to access the child block using
        // constructor 2
        BlockBuffer blockbuffer (internalEntries[entryindex + MIDDLE_INDEX_INTERNAL+1].rChild);

        // update pblock of the block to rightBlkNum using BlockBuffer::getHeader()
        // and BlockBuffer::setHeader().
        // HeadInfo blockHeader;
        blockbuffer.getHeader(&blockHeader);

        blockHeader.pblock = rightBlockNum;
        blockbuffer.setHeader(&blockHeader);
    }

    return rightBlockNum; 
}

int BPlusTree::createNewRoot(int relId, char attrName[ATTR_SIZE], Attribute attrVal, int lChild, int rChild) {
    // get the attribute cache entry corresponding to attrName
    // using AttrCacheTable::getAttrCatEntry().
    AttrCatEntry attrCatEntryBuffer;
    AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntryBuffer);

    // declare newRootBlk, an instance of IndInternal using appropriate constructor
    // to allocate a new internal index block on the disk
    IndInternal newRootBlock;

    int newRootBlkNum = newRootBlock.getBlockNum();

    if (newRootBlkNum == E_DISKFULL) 
    {
        BPlusTree::bPlusDestroy(rChild);

        return E_DISKFULL;
    }

    HeadInfo blockHeader;
    newRootBlock.getHeader(&blockHeader);

    blockHeader.numEntries = 1;
    newRootBlock.setHeader(&blockHeader);

    // create a struct InternalEntry with lChild, attrVal and rChild from the
    // arguments and set it as the first entry in newRootBlk using IndInternal::setEntry()
    InternalEntry internalentry;
    internalentry.lChild = lChild, internalentry.rChild = rChild, internalentry.attrVal = attrVal;

    newRootBlock.setEntry(&internalentry, 0);
    BlockBuffer leftChildBlock (lChild);
    BlockBuffer rightChildBlock (rChild);

    HeadInfo leftChildHeader, rightChildHeader;
    leftChildBlock.getHeader(&leftChildHeader);
    rightChildBlock.getHeader(&rightChildHeader);

    leftChildHeader.pblock = newRootBlkNum;
    rightChildHeader.pblock = newRootBlkNum;

    leftChildBlock.setHeader(&leftChildHeader);
    rightChildBlock.setHeader(&rightChildHeader);

    // update rootBlock = newRootBlkNum for the entry corresponding to `attrName`
    // in the attribute cache using AttrCacheTable::setAttrCatEntry().
    attrCatEntryBuffer.rootBlock = newRootBlkNum;
    AttrCacheTable::setAttrCatEntry(relId, attrName,  &attrCatEntryBuffer);

    return SUCCESS;
}

int BPlusTree::bPlusDestroy(int rootBlockNum) {
    // if rootBlockNum lies outside the valid range [0,DISK_BLOCKS-1]
    if (rootBlockNum < 0 || rootBlockNum >= DISK_BLOCKS)
        return E_OUTOFBOUND;

    int type = StaticBuffer::getStaticBlockType(rootBlockNum); // type of block using StaticBuffer::getStaticBlockType() 

    if (type == IND_LEAF) 
    {
        // declare an instance of IndLeaf for rootBlockNum using appropriate constructor
        IndLeaf leafBlock (rootBlockNum);

        // release the leafblock
        leafBlock.releaseBlock();

        return SUCCESS;

    } 
    else if (type == IND_INTERNAL) 
    {
        // declare an instance of IndInternal for rootBlockNum using appropriate constructor
        IndInternal internalBlock (rootBlockNum);

        // load the header of the block using BlockBuffer::getHeader().
        HeadInfo blockHeader;
        internalBlock.getHeader(&blockHeader);
        InternalEntry blockEntry;
        internalBlock.getEntry (&blockEntry, 0);

        BPlusTree::bPlusDestroy(blockEntry.lChild);

        for (int entry = 0; entry < blockHeader.numEntries; entry++) {
            internalBlock.getEntry (&blockEntry, entry);
            BPlusTree::bPlusDestroy(blockEntry.rChild);
        }

        // release the rootblock
        internalBlock.releaseBlock();

        return SUCCESS;

    } 
    else 
        return E_INVALIDBLOCK;
}

/*
inline bool operator == (RecId lhs, RecId rhs) {
	return (lhs.block == rhs.block && lhs.slot == rhs.slot);
}

inline bool operator != (RecId lhs, RecId rhs) {
	return (lhs.block != rhs.block || lhs.slot != rhs.slot);
}

inline bool operator == (IndexId lhs, IndexId rhs) {
	return (lhs.block == rhs.block && lhs.index == rhs.index);
}

inline bool operator != (IndexId lhs, IndexId rhs) {
	return (lhs.block != rhs.block || lhs.index != rhs.index);
}
*/