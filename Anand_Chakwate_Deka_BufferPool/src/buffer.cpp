/**
 * @authors - Tarun Anand      9082427247
 *            Rahul Chakwate   9083461260
 *            Debarshi Deka    9083351164
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University
 * of Wisconsin-Madison.
 */

#include "buffer.h"

#include <iostream>
#include <memory>

#include "exceptions/bad_buffer_exception.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/hash_not_found_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"

namespace badgerdb {

constexpr int HASHTABLE_SZ(int bufs) { return ((int)(bufs * 1.2) & -2) + 1; }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(std::uint32_t bufs)
    : numBufs(bufs),
      hashTable(HASHTABLE_SZ(bufs)),
      bufDescTable(bufs),
      bufPool(bufs) {
  for (FrameId i = 0; i < bufs; i++) {
    bufDescTable[i].frameNo = i;
    bufDescTable[i].valid = false;
  }

  clockHand = bufs - 1;
}

void BufMgr::advanceClock() {
// Move the clockhand
clockHand = (clockHand + 1) % numBufs;
}

void BufMgr::allocBuf(FrameId& frame) {
  // To keep track of the original clockhand position
  FrameId original = clockHand;
  // To keep track of how many times a full circle has been completed
  int count = 0;
  // Clock Replacement Algorithm
  while(true)
  {
    advanceClock();
    // If all frames have been checked and all are pinned.
    if(clockHand == original)
    {
      // if count = 0 that means only one circle has been completed
      if(count == 0)
      {
        // In the first circle a page with refbit set may not have been pinned.
        // That page may be accessible now
        count = 1;
      }
      else
      {
        // if count =1 then two circles have been completed and all pages are pinned
        break;
      }
    }
    // Check if valid is set to false
    if(bufDescTable[clockHand].valid == false)
    {
      frame = clockHand;
      return;
    }
    // Check if refbit is set to true
    if(bufDescTable[clockHand].refbit == true)
    {
      bufDescTable[clockHand].refbit = false;
      continue;
    }
    // Check if pinned
    if (bufDescTable[clockHand].pinCnt != 0)
      continue;
    // Check if dirty bit is set
    if(bufDescTable[clockHand].dirty == true)
    {
      // Flush page to disk - obtain page from buffer pool
      bufDescTable[clockHand].file.writePage(bufPool[clockHand]);
    }
    // Remove entry from hash table
    hashTable.remove(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);
    // Initialize for new entry
    bufDescTable[clockHand].clear();
    frame = clockHand;
    return;

  }
  // If all frames are pinned throw exception
  throw BufferExceededException();
}

void BufMgr::readPage(File& file, const PageId pageNo, Page*& page) {
  // FrameId that contains the page and file.
  FrameId fNo;
  try{
    // Looking up entry from hash table
    hashTable.lookup(file, pageNo, fNo);
    // Case 2: Page is in the buffer pool
    bufDescTable[fNo].refbit = true;
    bufDescTable[fNo].pinCnt++;
  }
  catch(const HashNotFoundException &)
  {
    // Case 1: Page is not in the buffer pool
    // Obtain free frame
    allocBuf(fNo);
    // Obtain page from file
    bufPool[fNo] = file.readPage(pageNo);
    // Insert into hash table
    hashTable.insert(file, pageNo, fNo);
    bufDescTable[fNo].Set(file, pageNo);
  }
    // Pointer to frame
    page = &bufPool[fNo];
}

void BufMgr::unPinPage(File& file, const PageId pageNo, const bool dirty) {
  // FrameId that contains the page and file.
  FrameId fNo;
  try{
    // Looking up entry from hash table
    hashTable.lookup(file, pageNo, fNo);
  }
  catch(const HashNotFoundException &)
  {
    // Do nothing if not found
    return;
  }
  // Throw PAGENOTPINNED if pin count is 0
  if(bufDescTable[fNo].pinCnt == 0)
    throw PageNotPinnedException(file.filename(), pageNo, fNo);
  bufDescTable[fNo].pinCnt--;
  if(dirty == true)
    bufDescTable[fNo].dirty = true;
}

void BufMgr::allocPage(File& file, PageId& pageNo, Page*& page) {
  // Empty page being allocated
  Page empty = file.allocatePage();
  pageNo = empty.page_number();
  // FrameId being used
  FrameId fNo;
  // Obtaining frame
  allocBuf(fNo);
  // Adding page to pool
  bufPool[fNo] = empty;
  page = &bufPool[fNo];
  // Inserting in hash table
  hashTable.insert(file, pageNo, fNo);
  // Calling Set
  bufDescTable[fNo].Set(file, pageNo);
}

void BufMgr::flushFile(File& file) {
 // Scan bufTable
 for(FrameId i = 0; i < numBufs; i++)
 {
   // Check if page belongs to file
   if(bufDescTable[i].file == file)
   {
     // If file is invalid
     if(bufDescTable[i].valid == false)
      throw BadBufferException(i, bufDescTable[i].dirty, bufDescTable[i].valid, bufDescTable[i].refbit);
    // If page is pinned
    if(bufDescTable[i].pinCnt > 0)
      throw PagePinnedException(bufDescTable[i].file.filename(), bufDescTable[i].pageNo, i);
    // If page is dirty
    if(bufDescTable[i].dirty == true)
    {
      // Flush page to disk - obtain page from buffer pool
      bufDescTable[i].file.writePage(bufPool[i]);
      bufDescTable[i].dirty = false;
    }
    // Remove page from hashtable
    hashTable.remove(file, bufDescTable[i].pageNo);
    bufDescTable[i].clear();
   }
 }
}

void BufMgr::disposePage(File& file, const PageId PageNo) {
  // Frame id being used
  FrameId fNo;
  // Check if it has been allocated a frame in the buffer pool
  try{
    hashTable.lookup(file, PageNo, fNo);
    // If present delete entry from HashTable
    hashTable.remove(file, PageNo);
    // Clear entry from bufPool
    bufDescTable[fNo].clear();
  }
  catch(const HashNotFoundException &)
  {
    // Do nothing if not found
  }
  // Delete page
  file.deletePage(PageNo);
}

void BufMgr::printSelf(void) {
  int validFrames = 0;

  for (FrameId i = 0; i < numBufs; i++) {
    std::cout << "FrameNo:" << i << " ";
    bufDescTable[i].Print();

    if (bufDescTable[i].valid) validFrames++;
  }

  std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}  // namespace badgerdb
