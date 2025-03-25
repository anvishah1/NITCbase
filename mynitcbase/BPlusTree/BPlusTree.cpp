#include "BPlusTree.h"

#include <cstring>

RecId BPlusTree::bPlusSearch(int relId, char attrName[ATTR_SIZE],Attribute attrVal,int op){
    IndexId searchindex;
    AttrCacheTable::getSearchIndex(relId,attrName,&searchindex);

    AttrCatEntry attrcatbuf;
    //loads the attribute cache entry into the attrcatbuf
    AttrCacheTable::getAttrCatEntry(relId,attrName,&attrcatbuf);

    int block,index;
    if(searchindex.block==-1 && searchindex.index==-1){
        block=attrcatbuf.rootBlock;
        index=0;
        if(block==-1)
            return RecId{-1,-1};
    }
    else{
        block=searchindex.block;
        index=searchindex.index+1;

        IndLeaf leaf(block);
        struct HeadInfo leafHead;
        leaf.getHeader(&leafHead);

        if(index >=leafHead.numEntries){
            block=leafHead.rblock;
            index=0;
        if(block== -1){
            return RecId{-1,-1};
        }
    }
    
}
while (StaticBuffer::getStaticBlockType(block)==IND_INTERNAL){
        IndInternal internalBlk(block);
        struct HeadInfo intHead;
        internalBlk.getHeader(&intHead);

        struct InternalEntry intEntry;
        if(op== NE || op== LT || op ==LE){
            internalBlk.getEntry(&intEntry,0);
            block=intEntry.lChild;
        }
        else{
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

      } else {
        block = intEntry.rChild; 
      }
    }
  }
  while(block!=-1){
    IndLeaf leafBlk(block);
    struct HeadInfo leafhead;

    leafBlk.getHeader(&leafhead);
    Index leafentry;
    while(index < leafhead.numEntries){
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
    else if((op==EQ || op ==LE || op ==LT) && cmpVal >0){
        return RecId{-1,-1};
    }
    ++index;
  }
if(op!=NE){
    break;
}
block=leafhead.rblock;
index=0;
  }
  return RecId{-1,-1};
}
