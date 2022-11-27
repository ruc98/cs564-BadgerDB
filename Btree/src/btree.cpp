/**
 * @authors - Tarun Anand      9082427247
 *            Rahul Chakwate   9083461260
 *            Debarshi Deka    9083351164
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University
 * of Wisconsin-Madison.
 */
#include "btree.h"
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"
#include "exceptions/page_not_pinned_exception.h"

//#define DEBUG

namespace badgerdb
{

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------

BTreeIndex::BTreeIndex(const std::string & relationName,
		std::string & outIndexName,
		BufMgr *bufMgrIn,
		const int attrByteOffset,
		const Datatype attrType)
{
		// Initialize class variables
		this->bufMgr = bufMgrIn;
		this->attributeType = attrType;
		this->attrByteOffset = attrByteOffset;
		this->leafOccupancy = INTARRAYLEAFSIZE;
		this->nodeOccupancy = INTARRAYNONLEAFSIZE;
		this->scanExecuting = false;
		// Constructing the index file name
		std::ostringstream idxStr;
		idxStr << relationName << '.' << attrByteOffset;
		// Returning index file name
		outIndexName = idxStr.str();
		// Check if file exists
		if (! BlobFile::exists(outIndexName))
		{
			// Create index file
			this->file =  new BlobFile(outIndexName,true);
			// Allocate page for metadata
			Page *metaPage,*rootPage;
			this->bufMgr->allocPage(this->file,this->headerPageNum,metaPage);
			// Write into metadata later
			this->bufMgr->unPinPage(this->file,this->headerPageNum,false);
			// Initialize root
			this->isRootLeaf = true;
			// Initialize root as leaf node
			this->bufMgr->allocPage(this->file,this->rootPageNum,rootPage);
			LeafNodeInt *root = (LeafNodeInt *) rootPage;
			// Marking entries filled
			root->entries = 0;
			// Mark right sibling as invalid
			root->rightSibPageNo = Page::INVALID_NUMBER;
			// Write root to disk
			this->bufMgr->unPinPage(this->file,this->rootPageNum,true);
			// Scanning File
			FileScan records = FileScan(relationName,this->bufMgr);
			// Id of each record
			RecordId recId;
			// Record string returned for each RecordId
			std::string recStr;
			// Key value
			void* key;
			// Begin Scanning File
			try
			{
				// Keep scanning till exception is thrown
				while(1)
				{
					// Get next record id
					records.scanNext(recId);
					// Get key
					recStr = records.getRecord();
					key = (int*)(recStr.c_str() + this->attrByteOffset);
					this->insertEntry(key,recId);
				}
			}
			catch(const EndOfFileException &e)
			{
				// End of file scan
			}
			// Bring Metadata page into buffer pool
			this->bufMgr->readPage(this->file,this->headerPageNum,metaPage);
			// Creating Metadata
			IndexMetaInfo *meta = (IndexMetaInfo*)metaPage ;
			strcpy(meta->relationName, relationName.c_str());
			meta->attrByteOffset = attrByteOffset;
			meta->attrType = attrType;
			meta->rootPageNum = this->rootPageNum;
			// Obtain new rootPageNum
			meta->isRootLeaf = this->isRootLeaf;
			// Unpin page
			this->bufMgr->unPinPage(this->file,this->headerPageNum,true);
		}
		else
		{
			// Open file
			this->file = new BlobFile(outIndexName,false);
			// Read metadata
			Page *meta_page_raw;
			this->headerPageNum = 1;
			this->bufMgr->readPage(file,headerPageNum,meta_page_raw);
			IndexMetaInfo *index_meta = (IndexMetaInfo*) meta_page_raw;
			// Check if correct index
			if(index_meta->attrType != this->attributeType || index_meta->attrByteOffset != this->attrByteOffset || index_meta->relationName != relationName )
			{
				//Bad index
				this->bufMgr->unPinPage(this->file,this->headerPageNum,false);
				throw BadIndexInfoException("Wrong index file");
			}
			// Obtain root page no
			this->rootPageNum = index_meta->rootPageNum;
			// Obtain root status
			this->isRootLeaf = index_meta->isRootLeaf;
			// Unpin page
			this->bufMgr->unPinPage(this->file,this->headerPageNum,false);
		}
}


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{
	this->bufMgr->flushFile(this->file);
	delete this->file;
	this->scanExecuting = false;
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

void BTreeIndex::insertEntry(const void *key, const RecordId rid)
{
	// Path of nodes
	std::vector<PageId> path;
	// If root is leaf
	if (this->isRootLeaf)
	{
		// Insert into leaf
		this->insertIntoLeaf(this->rootPageNum,key,rid,path);
	}
	else
	{
		// Since the root is not a leaf
		// We need to traverse the nodes until we find a leaf node to insert into
		this->searchNodes(this->rootPageNum,key,rid,path);
	}
}


const void BTreeIndex::insertIntoLeaf(const PageId pid, const void* key, const RecordId rid, std::vector<PageId> &path)
{
		// Obtain leaf node contents
		Page * leafPage;
		this->bufMgr->readPage(this->file,pid,leafPage);
		LeafNodeInt * leaf = (LeafNodeInt*) leafPage;
		// Check if slots remain
		if(leaf->entries < this->leafOccupancy)
		{
			// It is possible to place the pair in this leaf
			for(int i = 0; i < leaf->entries;i++)
			{
				// Since keyArray stores sorted values
				// if *key < leaf->keyArray[i]
				// then i is the position for <key,rid>
				if(*((int*)key) < leaf->keyArray[i])
				{
					// Shift the rest of the keys to the right
					for(int j = leaf->entries; j>i;j--)
					{
						leaf->keyArray[j] = leaf->keyArray[j-1];
						leaf->ridArray[j] = leaf->ridArray[j-1];
					}
					// insert <key,rid> at i
					leaf->keyArray[i] = *((int*)key);
					leaf->ridArray[i] = rid;
					leaf->entries +=1;
					// Unpin page
					this->bufMgr->unPinPage(this->file,pid,true);
					return;
				}
			}
			// Key is the largest key being inserted into the current node;
			leaf->keyArray[leaf->entries] = *((int*)key);
			leaf->ridArray[leaf->entries] = rid;
			leaf->entries +=1;
			// Unpin page
			this->bufMgr->unPinPage(this->file,pid,true);
		}
		else
		{
			// The node needs to be split into two siblings to continue
			// Unpin page
			this->bufMgr->unPinPage(this->file,pid,true);
			// Split node into two siblings
			this->splitLeaf(pid,key,rid,path);
		}
}


const void BTreeIndex::splitLeaf(const PageId pid, const void* key, const RecordId rid, std::vector<PageId>&path)
{
	// Obtain Page contents
	Page * leafPage;
	this->bufMgr->readPage(this->file,pid,leafPage);
	LeafNodeInt * leaf = (LeafNodeInt*) leafPage;
	// Create a right Sibling Page holding the larger values
	Page * sibPage;
	// Obtain new page id
	PageId sibid ;
	this->bufMgr->allocPage(this->file,sibid,sibPage);
	LeafNodeInt * sib = (LeafNodeInt*) sibPage;
	// Marking entries filled
	sib->entries = 0;
	// Values from the leafoccupancy/2 th index will be copied onto the sibling node
	int j = 0;
	// To ensure even split
	int thresh;
	// To choose the side to insert key
	bool insertRight;
	if(this->leafOccupancy%2 == 0)
	{
			if(leaf->keyArray[this->leafOccupancy/2-1] > *(int*)key )
			{
				// key is inserted to left
				insertRight = false;
				// We prefer right node having the extra value
				thresh = this->leafOccupancy/2 - 1;
			}
			else
			{
				// key is inserted to right
				insertRight = true;
				thresh = this->leafOccupancy/2;
			}
	}
	else
	{
			// To ensure 50% occupancy in both children
			if( leaf->keyArray[this->leafOccupancy/2] > *(int*)key )
			{
				//key goes to left sibling
				insertRight = false;
				// One extra original value goes to the right
				thresh = this->leafOccupancy/2;
			}
			else
			{
				// key goes to right sibling
				insertRight = true;
				// One extra original value goes to the left
				thresh = this->leafOccupancy/2 +1;
			}
	}
	// Split
	for(int i = thresh ; i<this->leafOccupancy;i++ )
	{
		sib->keyArray[j] = leaf->keyArray[i];
		sib->ridArray[j] = leaf->ridArray[i];
		// Increase sibling node's number of entries
		sib->entries ++;
		// Decrease current node's number of entries
		leaf->entries --;
		j++;
	}
	// Set rightSibPageNo
	sib->rightSibPageNo = leaf->rightSibPageNo;
	leaf->rightSibPageNo = sibid;
	// Insert <key,rid>
	// Close both pages
	this->bufMgr->unPinPage(this->file,pid,true);
	this->bufMgr->unPinPage(this->file,sibid,true);
	// Choose where to insert
	if (insertRight)
	{
		// We can safely call insert into leaf since right sibling has less than
		// leaf occupancy values, and it will not recurse infinitely
		this->insertIntoLeaf(sibid,key,rid,path);
	}
	else
	{
		// We can safely call insert into leaf since left sibling has less than
		// leaf occupancy values, and it will not recurse infinitely
		this->insertIntoLeaf(pid,key,rid,path);
	}
	// Repoen right to push up value
	this->bufMgr->readPage(this->file,sibid,sibPage);
	sib = (LeafNodeInt*) sibPage;
	// Right biased tree that will pushup the value of the right sibling
	int upper = sib->keyArray[0];
	// Unpin page
	this->bufMgr->unPinPage(this->file,sibid,true);
	// Push value up
	if(path.size() == 0)
	{
		// Node was the root and now a new root needs to be created
		Page* rootPage;
		PageId rootId ;
		this->bufMgr->allocPage(this->file,rootId,rootPage);
		// Change root index
		this->rootPageNum = rootId;
		// Change type of root
		this->isRootLeaf = false;
		// Adding root page
		NonLeafNodeInt* root = (NonLeafNodeInt*) rootPage;
		// Populating root
		root->keyArray[0] = upper;
		root->pageNoArray[0] = pid;
		root->pageNoArray[1] = sibid;
		root->entries = 1;
		// Since it is directly above a leaf
		root->level = 1;
		// Unpin page
		this->bufMgr->unPinPage(this->file,rootId,true);
	}
	else
	{
		// Pushing up into existing non leaf nodes
		// We are only sending right sibling page id
		// Since the upper level already has the left sibling's page id
		PageId pid = path[path.size()-1];
		path.pop_back();
		this->insertIntoNode(pid,upper,sibid,path);
	}
}



const void BTreeIndex::searchNodes(const PageId pid, const void* key, const RecordId rid, std::vector<PageId>&path)
{
	// Obtain Page contents
	Page * nodePage;
	this->bufMgr->readPage(this->file,pid,nodePage);
	NonLeafNodeInt * node = (NonLeafNodeInt*) nodePage;
	// Find next node
	PageId nextId = Page::INVALID_NUMBER;
	for(int i =0 ; i <node->entries;i++)
	{
		// Since all entries are sorted
		// We need to find the first entry larger than key
		if(*((int*)key) < node->keyArray[i])
		{
			// Found the next node to traverse
			nextId = node->pageNoArray[i];
			break;
		}
	}
	// Case where next node is rightmost child
	if(nextId == Page::INVALID_NUMBER)
	{
		nextId = node->pageNoArray[node->entries];
	}
	this->bufMgr->unPinPage(this->file,pid,false);
	// Add current page id to path
	path.push_back(pid);
	// If child is not leaf continue traversing
	if(node->level == 0)
	{
		this->searchNodes(nextId,key,rid,path);
	}
	else
	{
		this->insertIntoLeaf(nextId,key,rid,path);
	}

}

const void BTreeIndex::insertIntoNode(const PageId pid, int key, const PageId rightId, std::vector<PageId> &path)
{
	// Obtain Page contents
	Page * nodePage;
	this->bufMgr->readPage(this->file,pid,nodePage);
	NonLeafNodeInt * node = (NonLeafNodeInt*) nodePage;
	// Check if key can be placed in current node
	if(node->entries < this->nodeOccupancy)
	{
		// There is space to place key in same node
		for(int i =0 ; i<node->entries;i++)
		{
			// Since all entries in node are sorted, we need to find first location where key is lesser than node value
			if (key < node->keyArray[i])
			{
				// i is the location for key
				// Shift all values to the right
				for(int j = node->entries; j>i;j--)
				{
					node->keyArray[j] = node->keyArray[j-1];
					// Shift pageid values to the right as well
					node->pageNoArray[j+1] = node->pageNoArray[j];
				}
				// insert into i
				node->keyArray[i] = key;
				// insert rightId at i+1 position
				node->pageNoArray[i+1] = rightId;
				// add entry
				node->entries +=1;
				// Unpin page
				this->bufMgr->unPinPage(this->file,pid,true);
				return;
			}
		}
		// key is the largest value in the node
		node->keyArray[node->entries] = key;
		// Insert right id in node->entries + 1 position
		node->pageNoArray[node->entries+1] = rightId;
		// Add entry
		node->entries+=1;
		// Unpin page
		this->bufMgr->unPinPage(this->file,pid,true);
		return;
	}
	else
	{
		// Unpin page
		this->bufMgr->unPinPage(this->file,pid,true);
		// node needs to be split
		this->splitNode(pid,key,rightId,path);
	}
}


const void BTreeIndex::splitNode(const PageId pid,int key, const PageId rightId, std::vector<PageId> &path)
{
	// Obtain Page contents
	Page * nodePage;
	this->bufMgr->readPage(this->file,pid,nodePage);
	NonLeafNodeInt * node = (NonLeafNodeInt*) nodePage;
	// Create a right Page holding the larger values
	Page * newPage;
	// Obtain new page id
	PageId newId ;
	this->bufMgr->allocPage(this->file,newId,newPage);
	NonLeafNodeInt * newNode = (NonLeafNodeInt*) newPage;
	// Marking entries filled
	newNode->entries = 0;
	// set level
	newNode->level = node->level;
	// Half the values from left node will be copied onto right
	int j = 0;
	// To ensure even split
	int thresh;
	// To indicate which node key goes into
	bool insertRight;
	// We prefer having one extra entry in the right node
	if(node->keyArray[this->nodeOccupancy/2-1] > key)
	{
		// key inserted into left
		insertRight = false;
		// We split it such that the right node ends up having two extra values
		// It then pushes up the smallest value, resulting in one extra value overall
		thresh = this->nodeOccupancy/2 -1;
	}
	else
	{
		// Key will be inserted into the right
		thresh = this->nodeOccupancy/2;
		insertRight = true;
	}
	// Copying values into new node
	for(int i = thresh; i < this->leafOccupancy;i++)
	{
		newNode->keyArray[j] = node->keyArray[i];
		newNode->pageNoArray[j] = node->pageNoArray[i];
		// Change entries
		node->entries --;
		newNode->entries ++;
		j++;
	}
	// Copy rightmost pageid
	newNode->pageNoArray[j] = node->pageNoArray[this->leafOccupancy];
	// close both pages
	this->bufMgr->unPinPage(this->file,newId,true);
	this->bufMgr->unPinPage(this->file,pid,true);
	// We need to insert key,rightId
	if(insertRight == true)
	{
		// We can safely call insert into node since size < nodeoccupancy
		this->insertIntoNode(newId,key,rightId,path);
	}
	else
	{
		// We can safely call insert into node since size < nodeoccupancy
		this->insertIntoNode(pid,key,rightId,path);
	}
	// Pop and push up smallest value from right node
	this->bufMgr->readPage(this->file,newId,newPage);
	newNode = (NonLeafNodeInt*) newPage;
	// push up value
	int pushUp = newNode->keyArray[0];
	// Move all values to left
	for(int i = 1; i < newNode->entries ;i++)
	{
		// Move key value to left
		newNode->keyArray[i-1] = newNode->keyArray[i];
		// Move page no value to left
		newNode->pageNoArray[i-1] = newNode->pageNoArray[i];
	}
	// Move rightmost pageNo value to left
	newNode->pageNoArray[newNode->entries-1]= newNode->pageNoArray[newNode->entries];
	// Reduce entries
	newNode->entries --;
	// Unpin page ;
	this->bufMgr->unPinPage(this->file,newId,true);
	// push value up
	if(path.size() == 0 )
	{
		// New root needs to be created
		Page* rootPage;
		PageId rootId;
		this->bufMgr->allocPage(this->file,rootId,rootPage);
		// Change root index
		this->rootPageNum = rootId;
		// Change type of root
		this->isRootLeaf = false;
		// Adding root page
		NonLeafNodeInt* root = (NonLeafNodeInt*) rootPage;
		// Populating root
		root->keyArray[0] = pushUp;
		root->pageNoArray[0] = pid;
		root->pageNoArray[1] = newId;
		root->entries = 1;
		// Since it is not directly above a leaf
		root->level = 0;
		// Unpin page
		this->bufMgr->unPinPage(this->file,rootId,true);
	}
	else
	{
		// push up value to page id from vector
		PageId parentId = path[path.size()-1];
		path.pop_back();
		// Send pushup and newId up
		this->insertIntoNode(parentId,pushUp,newId,path);
	}
}
// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

void BTreeIndex::startScan(const void* lowValParm,
				   const Operator lowOpParm,
				   const void* highValParm,
				   const Operator highOpParm)
{

	// Checking for Operators
	if(lowOpParm != GT && lowOpParm != GTE)
	{
		throw BadOpcodesException();
	}
	if(highOpParm != LT && highOpParm != LTE)
	{
		throw BadOpcodesException();
	}

	// Initialize variable values
	this -> lowValInt = *((int*) lowValParm);
 	this -> highValInt = *((int*) highValParm);
	// Check range of scan
	if(this -> lowValInt > this -> highValInt)
	{
   		 throw BadScanrangeException();
 	 }
	// Begin scan
	if (this->scanExecuting)
	{
	  this->endScan();
	}

	this -> scanExecuting = true;
        this -> lowOp = lowOpParm;
        this -> highOp = highOpParm;
	// Page id that might contain the low value
	PageId pid = Page::INVALID_NUMBER;

	if(this->isRootLeaf)
	{
		// Since root is a leaf, there is no searching to be done
		pid = this->rootPageNum;
	}
	else
	{
		// Search for leaf containing either the key or value just greater than the key
		searchKey(this->rootPageNum,this->lowValInt,pid);
	}

	// Current pagenum = pid
	this->currentPageNum = pid;
	// Open page
	this->bufMgr->readPage(this->file,this->currentPageNum,this->currentPageData);
	LeafNodeInt* leaf = (LeafNodeInt*)this->currentPageData;
	// Find the location to start scanning from
	this->nextEntry=-1;
	// We need to scan the returned leaf, as well as the next leaf
	// To make sure we find the key

	int count = 2;
	while(count > 0)
	{
		for(int i = 0; i < leaf->entries; i++)
		{
			// If op =GTE and values are same
			if(this->lowValInt == leaf->keyArray[i] && this->lowOp == GTE)
			{
				// Check for GTE
				this->nextEntry = i;
				break;
			}
			else if(this->lowValInt < leaf->keyArray[i])
			{
				// Key value is greater than lowval
				this->nextEntry = i;
				break;
			}
		}
		if(this->nextEntry ==-1)
		{
			// While we have exhausted all keys in the current leaf node
			// There is a chance that the keys in the sibling leaf node
			// contain the required key,
			pid = leaf->rightSibPageNo;
			this->bufMgr->unPinPage(this->file,this->currentPageNum,false);
			// Check if sibling exists
			if(pid==Page::INVALID_NUMBER)
				break;
			this->currentPageNum = pid;
			// Open sibling
			this->bufMgr->readPage(this->file,this->currentPageNum,this->currentPageData);
			leaf = (LeafNodeInt*)this->currentPageData;
			count--;
		}
		else
		{
			// Key Found
			break;
		}
	}
	// Check for no key exception
	if(this->nextEntry == -1)
	{
		// Unpin page if not unpinned yet
		if(pid != Page::INVALID_NUMBER)
			this->bufMgr->unPinPage(this->file,this->currentPageNum,false);
		throw NoSuchKeyFoundException();
	}
	// Check if key satisfies the upper bound condition
	if( leaf->keyArray[this->nextEntry] > this->highValInt || (leaf->keyArray[this->nextEntry] == this->highValInt && this->highOp == LT ))
	{
		// Unpin page
		this->bufMgr->unPinPage(this->file,this->currentPageNum,false);
		throw NoSuchKeyFoundException();
	}
	// Leave page open for nextScan;
}


const void BTreeIndex::searchKey(PageId pid,int key, PageId &cid)
{
	// Read page contents
	Page * currPage;
	this->bufMgr->readPage(this->file,pid,currPage);
	NonLeafNodeInt* node = (NonLeafNodeInt*)currPage;
	// Traverse through node
	for(int i =0 ; i < node->entries;i++)
	{
		// Since values in node are sorted, the first larger number we find means that we need to take the left child
		if(node->keyArray[i] > key)
		{
			// If child is a leaf we need to return cid
			if(node->level == 1)
			{
				cid = node->pageNoArray[i];
				// unpin page
				this->bufMgr->unPinPage(this->file,pid,false);
				return;
			}
			// child is not a leaf
			PageId child = node->pageNoArray[i];
			// unpin page
			this->bufMgr->unPinPage(this->file,pid,false);
			// Search continues with child
			this->searchKey(child,key,cid);
			return;
		}
		else if(node->keyArray[i] == key)
		{
			// If key array[i] == key, that means key is present in the right child
			// If child is a leaf we need to return cid
			if(node->level == 1)
			{
				// Right child
				cid = node->pageNoArray[i+1];
				// unpin page
				this->bufMgr->unPinPage(this->file,pid,false);
				return;
			}
			// child is not a leaf
			PageId child = node->pageNoArray[i+1];
			// unpin page
			this->bufMgr->unPinPage(this->file,pid,false);
			// Search continues with child
			this->searchKey(child,key,cid);
			return;
		}
	}
	// Key is present in rightmost child
	// If child is a leaf we need to return cid
	if(node->level == 1)
	{
		cid = node->pageNoArray[node->entries];
		// unpin page
		this->bufMgr->unPinPage(this->file,pid,false);
		return;
	}
	// child is not a leaf
	PageId child = node->pageNoArray[node->entries];
	// unpin page
	this->bufMgr->unPinPage(this->file,pid,false);
	// Search continues with child
	this->searchKey(child,key,cid);
	return;
}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

void BTreeIndex::scanNext(RecordId& outRid)
{
	// Check if scanning
	if(this -> scanExecuting == false)
	{
		throw ScanNotInitializedException();
	}
	// Reached end of all leaves
	if(this->nextEntry == -1)
	{
		// Throw exception
		throw IndexScanCompletedException();
	}
	// Get leaf details
	LeafNodeInt* leaf = (LeafNodeInt*) this->currentPageData;
	// Check if still valid
	if(leaf->keyArray[this->nextEntry] > this->highValInt || (leaf->keyArray[this->nextEntry] == this->highValInt && this->highOp == LT ))
	{
		// Scan is completed
		this->bufMgr->unPinPage(this->file,this->currentPageNum,false);
		// Throw exception
		throw IndexScanCompletedException();
	}
	// Get Rid
	outRid = leaf->ridArray[this->nextEntry];
	// Initialize for next scan
	if(this->nextEntry < leaf->entries-1)
	{
		// Values still remain in current leaf
		this->nextEntry +=1;
	}
	else
	{
		PageId nextid = leaf->rightSibPageNo;
		this->bufMgr->unPinPage(this->file,this->currentPageNum,false);
		// If right sibling exists
		if(nextid != Page::INVALID_NUMBER)
		{
			this->currentPageNum = nextid;
			this->bufMgr->readPage(this->file,this->currentPageNum,this->currentPageData);
			this->nextEntry = 0;
		}
		else
		{
			// No more scans possible
			this->nextEntry = -1;
		}
	}

}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
void BTreeIndex::endScan()
{
	// Check if scan
	if(this->scanExecuting == false)
	{
		throw ScanNotInitializedException();
	}
	// Reset all values
	this->scanExecuting = false;
	this->nextEntry=-1;
	try
	{
		this->bufMgr->unPinPage(this->file,this->currentPageNum,false);
	}
	catch (const PageNotPinnedException &e)
	{
		// Already unpinned
	}
	this->currentPageNum = Page::INVALID_NUMBER;
	this->currentPageData = NULL;
}

}
