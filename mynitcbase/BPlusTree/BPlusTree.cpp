#include "BPlusTree.h"

#include <cstring>

RecId BPlusTree::bPlusSearch(int relId, char attrName[ATTR_SIZE],Attribute attrVal,int op){
    //get the searchindex of the attribute i.e the block and index if at which this attrval is found 
    IndexId searchindex;
    AttrCacheTable::getSearchIndex(relId,attrName,&searchindex);

    AttrCatEntry attrcatbuf;
    //loads the attribute cache entry into the attrcatbuf
    AttrCacheTable::getAttrCatEntry(relId,attrName,&attrcatbuf);

    int block,index;
    //if there is no prev match
    if(searchindex.block==-1 && searchindex.index==-1){
        //start the search from the rootblock
        block=attrcatbuf.rootBlock;
        index=0;
        //if there is no rootblock means that you cant do bplustree search
        if(block==-1)
            return RecId{-1,-1};
    }
    else{
        //if you have a prev match then youre already in that lead block and slot and you go to next slot 
        block=searchindex.block;
        index=searchindex.index+1;
//access the leaflock
        IndLeaf leaf(block);
        //get the info about that block
        struct HeadInfo leafHead;
        leaf.getHeader(&leafHead);
//if we reach the end of the block i.e there are more than 63 entries 
        if(index >=leafHead.numEntries){
            block=leafHead.rblock;
            index=0;
            //if we reach to the last leafblock i.e there are no more entries 
        if(block== -1){
            return RecId{-1,-1};
        }
    }
    
}
//if we are in an internal index block we need to move down to the correct leaf index block
while (StaticBuffer::getStaticBlockType(block)==IND_INTERNAL){
        IndInternal internalBlk(block);
        struct HeadInfo intHead;
        internalBlk.getHeader(&intHead);
//if the op is eq,le,th we want to start with the smallest entry so we go to the leftmost leaf node 
        struct InternalEntry intEntry;
        if(op== NE || op== LT || op ==LE){
            internalBlk.getEntry(&intEntry,0);
            block=intEntry.lChild;
        }
        else{
    //if hreater than or equal to we find the intattrval that is greater than it and move to the left child of it and then keep going right from there 
        index = 0;
      bool flag = false;
      while (index < intHead.numEntries) {
        int ret = internalBlk.getEntry(&intEntry, index);
        int cmpVal = compareAttrs(intEntry.attrVal, attrVal, NUMBER);
        if ((op == EQ and cmpVal == 0) or (op == GE and cmpVal >= 0) or
            (op == GT and cmpVal > 0)) {
          flag = true;
          break;
        }
        index++;
      }

      if (flag) {
        block = intEntry.lChild;
//if attrval is greatest then we move to the right most leaf block
      } else {
        block = intEntry.rChild; 
      }
    }
  }
  //now that we have the leaf block we traverse through all the leaf blocks 
  while(block!=-1){
    IndLeaf leafBlk(block);
    struct HeadInfo leafhead;

    leafBlk.getHeader(&leafhead);
    Index leafentry;
    while(index < leafhead.numEntries){
        //get each entry in the leafblock and check the values 
        int ret=leafBlk.getEntry(&leafentry,index);
        int cmpVal=compareAttrs(leafentry.attrVal,attrVal,attrcatbuf.attrType);
        if(
                (op == EQ && cmpVal == 0) ||
                (op == LE && cmpVal <= 0) ||
                (op == LT && cmpVal < 0) ||
                (op == GT && cmpVal > 0) ||
                (op == GE && cmpVal >= 0) ||
                (op == NE && cmpVal != 0)
    
        )
    {
        searchindex={block,index};
        AttrCacheTable::setSearchIndex(relId,attrName,&searchindex);
        return RecId{leafentry.block,leafentry.slot};
    }
    //if no smaller values are there then no need to proceed 
    else if((op==EQ || op ==LE || op ==LT) && cmpVal >0){
        return RecId{-1,-1};
    }
    ++index;
  }
 
if(op!=NE){
    break;
}
//move to the next block
block=leafhead.rblock;
index=0;
  }
  //if nothing 
  return RecId{-1,-1};
}
