
#include "Btree.h"

using namespace MonoDB;

Btree::Btree(const Char* path, Uint32 keySize, Uint32 valueSize, Uint64 space)
	:_path(path), _keySize(keySize), _valueSize(valueSize),
		_final(NULL), _filter(NULL), _compare(NULL) {
	
	/// initialize data instance
	_file = new Data(path, space);

	/// set node configuration 
	_nodeSize = 4 * KB;
	do {
		_maxWidth = (Int32)((_nodeSize - Btree::HeaderSize - Btree::LocSize)
					/ (_keySize + _valueSize + Btree::LocSize));
		if(_maxWidth <= Btree::MinimumWidth) _nodeSize = _nodeSize * 2;
		else break;
	} while(true);
	_nodeSize -= sizeof(Uint32);
	_dummySize = _keySize + _valueSize + Btree::LocSize;
	_headerSize = _nodeSize - _maxWidth * _dummySize - Btree::LocSize;

	/// load recycle
	_recycle = new std::list<Uint64>();
	std::string recyclePath = _path + "recycle";
	FILE* recycleFp = fopen(recyclePath.c_str(), "ab+");
	Uint64 recycleSlot = 0;
	while(fread(&recycleSlot, Btree::LocSize, 1, recycleFp) == Btree::LocSize)
		_recycle->push_back(recycleSlot);
	fclose(recycleFp);
	
	/// initialize request queue
	_request = new Hashmap<Uint64, Byte*>(Btree::RequestHashSize,
					Hashmap<Uint64, Byte*>::DefaultFunction);
	_request->SetFinalizer(Hashmap<Uint64, Byte*>::DefaultClearFinalizer, NULL);

	/// load metadata
	_count = new Hashmap<Uint64, Uint64>(Btree::MetadataHashSize,
			Hashmap<Uint64, Uint64>::DefaultFunction);
	std::string metaPath = _path + "metadata";
	FILE* metaFp = fopen(metaPath.c_str(), "ab+");
	Uint64 metaLocation = 0;
	while(fread(&metaLocation, Btree::LocSize, 1, metaFp) == Btree::LocSize) {
		Uint64 metaWidth = 0;
		for(Btree::Iterator iter = Begin(metaLocation); iter.IsFound(); iter.Next())
			metaWidth++;
		_count->Set(metaLocation, metaWidth);
	}
	if(_count->Count() == 0) CreateTree();
	fclose(metaFp);

	/// initialize mutex
	pthread_mutex_init(&_treeMutex, NULL);

	/// rollback process
	RollbackNodeToFile();
}
Btree::~Btree() {
	/// release resource
	FREE(_file);
	FREE(_count);
	FREE(_request);
	FREE(_recycle);
	
	/// destroy mutex
	pthread_mutex_destroy(&_treeMutex);
}

/// Btree node control
Uint64 Btree::CreateTree() {
	Byte* root = CreateNode();
	Uint64 result = WriteNodeToFile(NotFound, root);
	if(result != NotFound) {
		std::string metaPath = _path + "metadata";
		FILE* metaFp = fopen(metaPath.c_str(), "ab+");
		fwrite(&result, Btree::LocSize, 1, metaFp);
		fclose(metaFp);
		_count->Set(result, 0);
	}
	CLEAR(root);
	return result;
}
NEW Byte* Btree::CreateNode() {
	Byte* node = new Byte[_nodeSize + _dummySize];
	InitializeNode(node);
	return node;
}
void Btree::InitializeNode(OUTPUT Byte* node) {
	memset(node, 0, _nodeSize + _dummySize);
	SetLocation(node, NotFound);
}
Int32 Btree::CompareItem(Byte* left, Byte* right) {
	return _compare ? _compare(left, right, _compareArg) :
		(Int32)memcmp(left, right, _keySize);
}

Bool Btree::IsLeaf(Byte* node) {
	return (GetChildLocation(node, 0) == 0);
}
Bool Btree::IsFull(const Uint64 loc) {
	Byte* node = CreateNode();
	Bool result = false;
	if(ReadNode(loc, node))
		result = (GetWidth(node) >= _maxWidth);
	CLEAR(node);
	return result;
}
Bool Btree::IsUnderflow(const Uint64 loc) {
	Byte* node = CreateNode();
	Bool result = false;
	if(ReadNode(loc, node))
		result = (GetWidth(node) < (Uint32)(_maxWidth / 2) + 1);
	CLEAR(node);
	return result;
}

Uint32 Btree::GetWidth(Byte* node) {
	Uint32 result = 0;
	Block stream(node, _nodeSize + _dummySize);
	stream.Seek(0);
	stream.Read(&result, sizeof(Uint32));
	return result;
}
Uint64 Btree::GetLocation(Byte* node) {
	Uint64 result = 0;
	Block stream(node, _nodeSize + _dummySize);
	stream.Seek(sizeof(Uint32));
	stream.Read(&result, Btree::LocSize);
	return result;
}
Uint64 Btree::GetChildLocation(Byte* node, Uint32 index) {
	Uint64 result = 0;
	Block stream(node, _nodeSize + _dummySize);
	stream.Seek(_headerSize + (_dummySize * index));
	stream.Read(&result, Btree::LocSize);
	return result;
}
Byte* Btree::GetChildItem(Byte* node, Uint32 index) {
	return node + _headerSize + Btree::LocSize + (_dummySize * index);
}
Int32 Btree::GetChildIndex(Byte* node, Uint64 loc) {
	Int32 result = NotFound;
	Uint32 width = GetWidth(node);
	for(Uint32 i = 0; i < width + 1; i++) {
		if(loc == GetChildLocation(node, i)) {
			result = i;
			break;
		}
	}
	return result;
}

void Btree::SetWidth(Byte* node, Uint32 width) {
	Block stream(node, _nodeSize + _dummySize);
	stream.Seek(0);
	stream.Write(&width, sizeof(Uint32));
}
void Btree::SetLocation(Byte* node, Uint64 loc) {
	Block stream(node, _nodeSize + _dummySize);
	stream.Seek(sizeof(Uint32));
	stream.Write(&loc, Btree::LocSize);
}
void Btree::SetChildLocation(Byte* node, Uint32 index, Uint64 loc) {
	Block stream(node, _nodeSize + _dummySize);
	stream.Seek(_headerSize + (_dummySize * index));
	stream.Write(&loc, Btree::LocSize);
}
void Btree::SetChildKey(Byte* node, Uint32 index, Byte* key) {
	Block stream(node, _nodeSize + _dummySize);
	stream.Seek(_headerSize + Btree::LocSize + (_dummySize * index));
	stream.Write(key, _keySize);
}
void Btree::SetChildValue(Byte* node, Uint32 index, Byte* value) {
	Block stream(node, _nodeSize + _dummySize);
	stream.Seek(_headerSize + Btree::LocSize + _keySize + (_dummySize * index));
	stream.Write(value, _valueSize);
}
void Btree::SetChildItem(Byte* node, Uint32 index, Byte* item) {
	Block stream(node, _nodeSize + _dummySize);
	stream.Seek(_headerSize + Btree::LocSize + (_dummySize * index));
	stream.Write(item, _keySize + _valueSize);
}

Bool Btree::InsertItem(Byte* node, Byte* item, Uint64 prevLoc, Uint64 nextLoc) {
	Uint32 width = GetWidth(node);
	Uint32 target = width;

	for(Uint32 i = 0; i < width; i++) {
		Byte* child = GetChildItem(node, i);
		Int32 comp = CompareItem(child, item);
		if(comp > 0) { target = i; break; }
	}
	for(Uint32 i = width; i > target; i--) {
		CopyItem(node, i, node, i - 1);
		CopyLocation(node, i + 1, node, i);
	}

	SetWidth(node, width + 1);
	SetChildItem(node, target, item);
	SetChildLocation(node, target, prevLoc);
	SetChildLocation(node, target + 1, nextLoc);
	return true;
}
Bool Btree::RemoveItem(Byte* node, Uint32 index) {
	Uint32 width = GetWidth(node);
	for(Uint32 i = index; i < width - 1; i++) {
		CopyItem(node, i, node, i + 1);
		CopyLocation(node, i, node, i + 1);
	}
	CopyLocation(node, width - 1, node, width);
	SetWidth(node, width - 1);

	Byte* item = GetChildItem(node, width - 1);
	memset(item, 0, _dummySize);
	return true;
}
void Btree::CopyItem(Byte* dstNode, Uint32 dstIndex, Byte* srcNode, Uint32 srcIndex) {
	Uint32 dstOffset = _headerSize + Btree::LocSize + _dummySize * dstIndex;
	Uint32 srcOffset = _headerSize + Btree::LocSize + _dummySize * srcIndex;
	if(dstNode + dstOffset != srcNode + srcOffset)
		memcpy(dstNode + dstOffset, srcNode + srcOffset, _keySize + _valueSize);
}
void Btree::CopyLocation(Byte* dstNode, Uint32 dstIndex, Byte* srcNode, Uint32 srcIndex) {
	Uint32 dstOffset = _headerSize + _dummySize * dstIndex;
	Uint32 srcOffset = _headerSize + _dummySize * srcIndex;
	if(dstNode + dstOffset != srcNode + srcOffset)
		memcpy(dstNode + dstOffset, srcNode + srcOffset, Btree::LocSize);
}

/// Btree control
Int32 Btree::SearchItem(Uint64 root, Byte* key, OUTPUT std::list<Uint64>& queue, OUTPUT Byte* node) {
	Uint64 loc = root;
	Uint32 width = 0;
	Int32 result = NotFound;
	do {
		if(ReadNode(loc, node)) {
			queue.push_back(loc);
			width = GetWidth(node);

			Int32 nextIdx = NotFound;
			Int32 startIdx = 0;
			Int32 endIdx = width;
			do {
				Int32 centerIdx = (Int32)((startIdx + endIdx) / 2);
				Byte* item = GetChildItem(node, centerIdx);
				Int32 comp = CompareItem(item, key);
				if(comp == 0) {
					result = centerIdx;
					break;
				}
				else if(comp > 0) endIdx = centerIdx;
				else if(comp < 0) startIdx = centerIdx + 1;
			} while(startIdx < endIdx);

			if(result != NotFound) break;
			loc = GetChildLocation(node, endIdx);
		}
		else break;
	} while(loc > 0);
	return result;
}
Uint64 Btree::GetPrevSiblingLocation(Byte* parentNode, Byte* currentNode) {
	Uint64 currentLoc = GetLocation(currentNode);
	Int32 currentIdx = GetChildIndex(parentNode, currentLoc);
	if(currentIdx == NotFound) return NotFound;
	return (currentIdx > 0) ? GetChildLocation(parentNode, currentIdx - 1) : NotFound;
}
Uint64 Btree::GetNextSiblingLocation(Byte* parentNode, Byte* currentNode) {
	Uint64 currentLoc = GetLocation(currentNode);
	Int32 currentIdx = GetChildIndex(parentNode, currentLoc);
	if(currentIdx == NotFound) return NotFound;
	return (currentIdx < _maxWidth) ? GetChildLocation(parentNode, currentIdx + 1) : NotFound;
}

Bool Btree::PassFirstItemToPrevSibling(Byte* parentNode, Byte* currentNode) {
	return PassFirstItemToPrevSibling(parentNode, currentNode, NULL);
}
Bool Btree::PassFirstItemToPrevSibling(Byte* parentNode, Byte* currentNode, Byte* prevNode) {
	Uint64 parentLoc = GetLocation(parentNode);
	Uint64 currentLoc = GetLocation(currentNode);
	if(parentLoc == NotFound) return false;

	Uint64 siblingLoc = prevNode ? GetLocation(prevNode) : GetPrevSiblingLocation(parentNode, currentNode);
	if(siblingLoc > 0 && !IsFull(siblingLoc)) {
		Byte* siblingNode = prevNode ? prevNode : CreateNode();
		Int32 childIdx = GetChildIndex(parentNode, siblingLoc);
		if(childIdx != NotFound) {
			if(prevNode || ReadNode(siblingLoc, siblingNode)) {
				/// move (index)th parent node value to sibling node
				Uint32 siblingWidth = GetWidth(siblingNode);
				Uint64 prevLastLoc = GetChildLocation(siblingNode, siblingWidth);
				Uint64 currFirstLoc = GetChildLocation(currentNode, 0);

				Byte* slot = GetChildItem(parentNode, childIdx);
				InsertItem(siblingNode, slot, prevLastLoc, currFirstLoc);
				WriteNode(siblingLoc, siblingNode);
				if(!prevNode) CLEAR(siblingNode);
				
				/// move first node value to parent
				CopyItem(parentNode, childIdx, currentNode, 0);
				WriteNode(parentLoc, parentNode);
				
				/// remove first node from current node
				RemoveItem(currentNode, 0);
				WriteNode(currentLoc, currentNode);
				return true;
			}
		}
		if(!prevNode) CLEAR(siblingNode);
	}
	return false;
}
Bool Btree::PassLastItemToNextSibling(Byte* parentNode, Byte* currentNode) {
	return PassLastItemToNextSibling(parentNode, currentNode, NULL);
}
Bool Btree::PassLastItemToNextSibling(Byte* parentNode, Byte* currentNode, Byte* nextNode) {
	Uint64 parentLoc = GetLocation(parentNode);
	Uint64 currentLoc = GetLocation(currentNode);
	if(parentLoc == NotFound) return false;

	Uint64 siblingLoc = nextNode ? GetLocation(nextNode) : GetNextSiblingLocation(parentNode, currentNode);
	if(siblingLoc > 0 && !IsFull(siblingLoc)) {
		Byte* siblingNode = nextNode ? nextNode : CreateNode();
		Uint32 childIdx = GetChildIndex(parentNode, currentLoc);
		if(childIdx != NotFound) {
			if(nextNode || ReadNode(siblingLoc, siblingNode)) {
				/// move (index)th parent node value to sibling node
				Uint32 currentWidth = GetWidth(currentNode);
				Uint64 nextFirstLoc = GetChildLocation(siblingNode, 0);
				Uint64 currentLastLoc = GetChildLocation(currentNode, currentWidth);
				
				Byte* slot = GetChildItem(parentNode, childIdx);
				InsertItem(siblingNode, slot, currentLastLoc, nextFirstLoc);
				WriteNode(siblingLoc, siblingNode);
				if(!nextNode) CLEAR(siblingNode);
				
				/// move last node value to parent
				CopyItem(parentNode, childIdx, currentNode, currentWidth - 1);
				WriteNode(parentLoc, parentNode);

				/// remove last node from current node
				memset(slot, 0, _keySize + _valueSize);
				SetChildItem(currentNode, currentWidth - 1, slot);
				SetChildLocation(currentNode, currentWidth, 0);
				SetWidth(currentNode, currentWidth - 1);
				WriteNode(currentLoc, currentNode);
				return true;
			}
		}
		if(!nextNode) CLEAR(siblingNode);
	}
	return false;
}

/// Btree request queue
Bool Btree::ReadNodeFromRequest(const Uint64 loc, OUTPUT Byte* node) {
	Byte* requestNode = NULL;
	Bool result = _request->Get(loc, &requestNode);
	if(result) memcpy(node, requestNode, _nodeSize + _dummySize);
	return result;
}
Uint64 Btree::WriteNodeToRequest(Uint64 loc, Byte* node) {
	/// if the locess is NotFound, create new node in file
	if(loc == NotFound)
		loc = WriteNodeToFile(NotFound, node);

	SetLocation(node, loc);
	Byte* requestNode = CreateNode();
	memcpy(requestNode, node, _nodeSize + _dummySize);

	if(_request->Set(loc, requestNode)) {
		return loc;
	}
	else CLEAR(requestNode);
	return NotFound;
}
Bool Btree::ClearNodeFromRequest(const Uint64 loc) {
	Byte* emptyNode = CreateNode();
	SetLocation(emptyNode, loc);
	if(!_request->Set(loc, emptyNode)) {
		CLEAR(emptyNode);
		return false;
	}
	return true;
}

/// Btree file control
Bool Btree::ReadNodeFromFile(const Uint64 loc, OUTPUT Byte* node) {
	Byte* buffer = NULL;
	Int32 readByte = _file->Read(loc, &buffer);
	if(buffer) {
		memcpy(node, buffer, _nodeSize);
		CLEAR(buffer);
	}
	return (readByte == _nodeSize);
}
Uint64 Btree::WriteNodeToFile(Uint64 loc, Byte* node) {
	if(loc == NotFound) {
		loc = PopNodeFromRecycle();
		if(loc == NotFound) {
			loc = _file->NextLocation(_nodeSize);
			if(loc == NotFound) return NotFound;
		}
	}
	SetLocation(node, loc);
	return _file->Write(loc, node, _nodeSize);
}
Bool Btree::ClearNodeFromFile(const Uint64 loc) {
	if(loc != NotFound) {
		Byte* node = CreateNode();
		SetLocation(node, loc);
		WriteNodeToFile(loc, node);
		CLEAR(node);
		
		/// push the block into recycle recycle
		PushNodeToRecycle(loc);
	}
}
Bool Btree::CommitNodeToFile() {
	/// backup node in request queue
	Bool result = false;
	if(_request->Map(Btree::BackupHandler, this)) {
		/// write node to file
		if(_request->Map(Btree::CommitHandler, this)) {
			/// clear backup file
			std::string backupPath = _path + "backup";
			FILE* backupFp = fopen(backupPath.c_str(), "wb");
			fclose(backupFp);
			result = true;
		}
		else RollbackNodeToFile();
	}
	else RollbackNodeToFile();
	
	_request->Clear();
	return result;
}
void Btree::RollbackNodeToFile() {
	std::string backupPath = _path + "backup";
	FILE* backupFp = fopen(backupPath.c_str(), "rb+");
	if(backupFp) {
		Byte* backupNode = CreateNode();
		fseek(backupFp, 0, SEEK_SET);
		while(fread(backupNode, sizeof(Byte), _nodeSize, backupFp) == _nodeSize) {
			Uint64 backupLoc = GetLocation(backupNode);
			WriteNodeToFile(backupLoc, backupNode);
		}
		CLEAR(backupNode);
		fclose(backupFp);
	}
	
	backupFp = fopen(backupPath.c_str(), "wb");
	fclose(backupFp);
}

/// Btree node recycle
void Btree::PushNodeToRecycle(const Uint64 loc) {
	std::string recyclePath = _path + "recycle";
	FILE* fp = fopen(recyclePath.c_str(), "ab+");
	fwrite(&loc, Btree::RecycleSize, 1, fp);
	_recycle->push_back(loc);
	fclose(fp);
}
Uint64 Btree::PopNodeFromRecycle() {
	Uint64 result = NotFound;
	if(_recycle->size() > 0) {
		std::string recyclePath = _path + "recycle";
		FILE* fp = fopen(recyclePath.c_str(), "rb+");
		if(fp) {
			fseek(fp, 0, SEEK_END);
			ftruncate(fileno(fp), ftell(fp) - Btree::RecycleSize);
			fclose(fp);

			result = _recycle->back();
			_recycle->pop_back();
		}
	}
	return result;
}

/// Btree node interface
Bool Btree::ReadNode(const Uint64 loc, OUTPUT Byte* node) {
	if(ReadNodeFromRequest(loc, node)) return true;
	else if(ReadNodeFromFile(loc, node)) return true;
	return false;
}
Uint64 Btree::WriteNode(const Uint64 loc, Byte* node) {
	return WriteNodeToRequest(loc, node);
}
Bool Btree::CommitNode() {
	return CommitNodeToFile();
}

/// Btree class interface
Bool Btree::Get(Uint64 root, Byte* key, OUTPUT Byte* value) {
	Byte* node = CreateNode();
	std::list<Uint64> queue;
	Bool result = false;
	Int32 index = SearchItem(root, key, queue, node);
	if(index != NotFound) {
		result = true;
		memcpy(value, GetChildItem(node, index) + _keySize, _valueSize);
	}
	CLEAR(node);
	return result;
}
Bool Btree::Insert(Uint64 root, Byte* item) {
	Bool result = false;
	Byte* value = item + _keySize;
	if(_filter && !_filter(item, value, _filterArg))
		return false;

	Byte* currentNode = CreateNode();
	std::list<Uint64> queue;
	Int32 index = SearchItem(root, item, queue, currentNode);

	if(queue.empty()) {
		CLEAR(currentNode);
		return false;
	}
	Uint64 currentLoc = queue.back();
	queue.pop_back();

	if(index != NotFound) {
		SetChildItem(currentNode, index, item);
		result = (WriteNode(currentLoc, currentNode) != NotFound);
	}
	else {
		InsertItem(currentNode, item, 0, 0);
		Uint32 currentWidth = GetWidth(currentNode);
		if(currentWidth <= _maxWidth)
			result = (WriteNode(currentLoc, currentNode) != NotFound);

		Byte* parentNode = CreateNode();
		Byte* slot = CreateItem();
		while(currentWidth > _maxWidth) {
			Uint64 parentLoc = queue.empty() ? NotFound : queue.back();
			if(!queue.empty()) queue.pop_back();

			if(parentLoc != NotFound && ReadNode(parentLoc, parentNode)) {
				if(PassFirstItemToPrevSibling(parentNode, currentNode) ||
					PassLastItemToNextSibling(parentNode, currentNode)) {
					result = true;
					break;
				}
			}

			Uint32 centerIdx = floor(currentWidth / 2);
			memcpy(slot, GetChildItem(currentNode, centerIdx), _keySize + _valueSize);
			
			Byte* leftNode = CreateNode();
			Uint64 leftLoc = (parentLoc == NotFound) ? NotFound : currentLoc;
			SetWidth(leftNode, centerIdx);
			for(Uint32 leftIdx = 0; leftIdx < centerIdx; leftIdx++) {
				CopyItem(leftNode, leftIdx, currentNode, leftIdx);
				CopyLocation(leftNode, leftIdx, currentNode, leftIdx);
			}
			CopyLocation(leftNode, centerIdx, currentNode, centerIdx);
			leftLoc = WriteNode(leftLoc, leftNode);
			CLEAR(leftNode);
			
			Byte* rightNode = CreateNode();
			Uint64 rightLoc = NotFound;
			SetWidth(rightNode, currentWidth - centerIdx - 1);
			for(Uint32 rightIdx = centerIdx + 1; rightIdx < currentWidth; rightIdx++) {
				CopyItem(rightNode, rightIdx - centerIdx - 1, currentNode, rightIdx);
				CopyLocation(rightNode, rightIdx - centerIdx - 1, currentNode, rightIdx);
			}
			CopyLocation(rightNode, currentWidth - centerIdx - 1, currentNode, currentWidth);
			rightLoc = WriteNode(rightLoc, rightNode);
			CLEAR(rightNode);
			
			if(parentLoc == NotFound) {
				InitializeNode(currentNode);
				InsertItem(currentNode, slot, leftLoc, rightLoc);
				if(WriteNode(currentLoc, currentNode) != NotFound) result = true;
				break;
			}
			else {
				memcpy(currentNode, parentNode, _nodeSize + _dummySize);
				InsertItem(currentNode, slot, leftLoc, rightLoc);
				currentLoc = parentLoc;
				currentWidth = GetWidth(currentNode);
				if(currentWidth <= _maxWidth) {
					if(WriteNode(currentLoc, currentNode) != NotFound) result = true;
					break;
				}
			}
		}
		CLEAR(slot);
		CLEAR(parentNode);
	}

	if(result) result = CommitNode();
	else _request->Clear();

	CLEAR(currentNode);
	return result;
}
Bool Btree::Remove(Uint64 root, Byte* key) {
	Byte* currentNode = CreateNode();
	std::list<Uint64> queue;
	Bool result = false;
	Int32 index = SearchItem(root, key, queue, currentNode);
	
	/// if it fails to search tree, it returns
	if(index == NotFound || queue.empty()) {
		CLEAR(currentNode);
		return false;
	}

	/// if the node is not leaf, search for the next element in leaf
	Uint64 currentLoc = queue.back();
	if(!IsLeaf(currentNode)) {
		Byte* leafNode = CreateNode();
		Uint64 leafLoc = GetChildLocation(currentNode, index + 1);
		do {
			queue.push_back(leafLoc);
			if(ReadNode(leafLoc, leafNode)) {
				leafLoc = GetChildLocation(leafNode, 0);
			}
			else {
				CLEAR(leafNode);
				CLEAR(currentNode);
				return false;
			}
		} while(leafLoc > 0);
		
		/// change the nodes (target element and next first element)
		CopyItem(currentNode, index, leafNode, 0);
		WriteNode(currentLoc, currentNode);
		
		/// set the leaf node to current node
		index = 0;
		currentLoc = queue.back();
		queue.pop_back();
		
		memcpy(currentNode, leafNode, _nodeSize + _dummySize);
		CLEAR(leafNode);
	}
	else queue.pop_back();

	// call finalizer and remove item
	if(_final) {
		Byte* item = GetChildItem(currentNode, index);
		_final(item, item + _keySize, _finalArg);
	}
	RemoveItem(currentNode, index);

	/// cover underflow exception
	Byte* parentNode = CreateNode();
	while(!result) {
		Uint64 parentLoc = queue.empty() ? NotFound : queue.back();
		if(!queue.empty()) queue.pop_back();
		
		/// if the node is root, write the result and return
		if(parentLoc == NotFound) {
			if(WriteNode(currentLoc, currentNode) != NotFound)
				result = true;
			break;
		}

		/// the count should be more than a half of node width
		Uint32 currentWidth = GetWidth(currentNode);
		if(currentWidth < (Uint32)(_maxWidth / 2)) {
			if(ReadNode(parentLoc, parentNode)) {
				Byte* siblingNode = CreateNode();
				Uint32 parentWidth = GetWidth(parentNode);
				Int32 locIdx = GetChildIndex(parentNode, currentLoc);
				if(locIdx == NotFound) { CLEAR(siblingNode); break; }

				if(locIdx > 0) {
					Uint64 prevLoc = GetChildLocation(parentNode, locIdx - 1);
					if(!IsUnderflow(prevLoc)) {
						if(ReadNode(prevLoc, siblingNode))
							result = PassLastItemToNextSibling(parentNode, siblingNode, currentNode);
						CLEAR(siblingNode);
						break;
						
					}
				}
				if(locIdx < parentWidth) {
					Uint64 nextLoc = GetChildLocation(parentNode, locIdx + 1);
					if(!IsUnderflow(nextLoc)) {
						if(ReadNode(nextLoc, siblingNode))
							result = PassFirstItemToPrevSibling(parentNode, siblingNode, currentNode);
						CLEAR(siblingNode);
						break;
					}
				}

				/// merge process with sibling node
				Uint64 siblingLoc = GetChildLocation(parentNode, (locIdx > 0) ? locIdx - 1 : locIdx + 1);
				if(ReadNode(siblingLoc, siblingNode)) {
					Uint32 siblingWidth = GetWidth(siblingNode);
					
					/// if sibling is not behind the current node, switch
					if(locIdx > 0) {
						Byte* tempNode = currentNode;
						currentNode = siblingNode;
						siblingNode = tempNode;
						locIdx--;
						
						currentLoc = GetLocation(currentNode);
						currentWidth = GetWidth(currentNode);
						siblingLoc = GetLocation(siblingNode);
						siblingWidth = GetWidth(siblingNode);
					}

					/// merge to current node
					SetWidth(currentNode, currentWidth + siblingWidth + 1);
					CopyItem(currentNode, currentWidth, parentNode, locIdx);
					for(Uint32 nodeIdx = 0; nodeIdx < siblingWidth; nodeIdx++) {
						CopyItem(currentNode, currentWidth + nodeIdx + 1, siblingNode, nodeIdx);
						CopyLocation(currentNode, currentWidth + nodeIdx + 1, siblingNode, nodeIdx);
					}
					CopyLocation(currentNode, currentWidth + siblingWidth + 1, siblingNode, siblingWidth);
					ClearNodeFromRequest(siblingLoc);

					/// remove node item from parent
					RemoveItem(parentNode, locIdx);
					if(GetWidth(parentNode) == 0) {
						memcpy(parentNode, currentNode, _nodeSize + _dummySize);
						SetLocation(parentNode, parentLoc);
						WriteNode(parentLoc, parentNode);
						ClearNodeFromRequest(currentLoc);
						
						CLEAR(siblingNode);
						result = true;						
						break;
					}
					else {
						SetChildLocation(parentNode, locIdx, currentLoc);
						WriteNode(currentLoc, currentNode);
						WriteNode(parentLoc, parentNode);
					}

					/// set current node
					currentLoc = parentLoc;

					/// swap nodes
					Byte* tempNode = currentNode;
					currentNode = parentNode;
					parentNode = tempNode;
				}
				CLEAR(siblingNode);
			}
		}
		else {
			if(WriteNode(currentLoc, currentNode) != NotFound) result = true;
			break;
		}
	}
	CLEAR(parentNode);
	if(result) result = CommitNode();
	else _request->Clear();
	
	CLEAR(currentNode);
	return result;

}
Bool Btree::Clear(Uint64 root) {
	std::list<Uint64> queue;
	Bool result = true;
	Byte* node = CreateNode();
	queue.push_back(root);
	
	while(!queue.empty()) {
		Uint64 currentLoc = queue.front();
		queue.pop_front();
		if(ReadNode(currentLoc, node)) {
			Uint64 width = GetWidth(node);
			for(Uint32 i = 0; i < width + 1; i++)
				queue.push_back(GetChildLocation(node, i));
			
			/// clear node
			ClearNodeFromRequest(currentLoc);
		}
		else result = false;
	}
	if(result) result = CommitNode();
	CLEAR(node);
	return result;
}

Btree::Iterator Btree::Begin(Uint64 root) {
	Iterator result(this);
	result.Begin(root);
	return result;
}
Btree::Iterator Btree::End(Uint64 root) {
	Iterator result(this);
	result.End(root);
	return result;
}

Btree::Iterator Btree::Find(Uint64 root, Byte* key) {
	Iterator result(this);
	Uint64 loc = root;
	Uint32 width = 0;
	Byte* node = CreateNode();
	do {
		if(ReadNode(loc, node)) {
			result.PushNode(node);
			width = GetWidth(node);

			Int32 nextIdx = NotFound;
			Int32 startIdx = 0;
			Int32 endIdx = width;
			while(startIdx < endIdx) {
				Int32 centerIdx = (Int32)((startIdx + endIdx) / 2);
				Byte* item = GetChildItem(node, centerIdx);

				Int32 comp = CompareItem(item, key);
				if(comp == 0) {
					result._found = true;
					result._target = centerIdx;
					break;
				}
				else if(comp > 0) endIdx = centerIdx;
				else if(comp < 0) startIdx = centerIdx + 1;
			};

			if(result._found) break;
			loc = GetChildLocation(node, endIdx);
			if(loc > 0) result.PushIndex(endIdx);
			else result._target = endIdx;
		}
		else break;
	} while(loc > 0);
	CLEAR(node);
	return result;
}
Btree::Iterator Btree::FindBefore(Uint64 root, Byte* key) {
	Iterator result = Find(root, key);
	if(!result._found) {
		Byte* node = result.LastNode();
		if(node) {
			Uint32 width = GetWidth(node);
			if(width > 0) {
				result._found = true;
				if(result._target > 0)
					result._target--;
				else result.Prev();
			}
		}
	}
	else result.Prev();
	return result;
}
Btree::Iterator Btree::FindAfter(Uint64 root, Byte* key) {
	Iterator result = Find(root, key);
	if(!result._found) {
		Byte* node = result.LastNode();
		if(node) {
			Uint32 width = GetWidth(node);
			if(width > 0) {
				result._found = true;
				if(result._target > 0) {
					result._target--;
					result.Next();
				}
			}
		}
	}
	else result.Next();
	return result;
}

/// Btree::Iterator class
Btree::Iterator::Iterator(Btree* parent)
	:_parent(parent), _found(false), _target(NotFound) {}

Btree::Iterator::~Iterator() {
	for(std::list<Byte*>::iterator i = _nodeStack.begin();
		i != _nodeStack.end(); i++)
		CLEAR(*i);

	_found = false;
	_target = NotFound;
	_nodeStack.clear();
	_indexStack.clear();
}

/// Btree iterator control
void Btree::Iterator::PushNode(Byte* node) {
	Byte* newNode = _parent->CreateNode();
	memcpy(newNode, node, _parent->NodeSize());
	_nodeStack.push_back(newNode);
}
void Btree::Iterator::PushIndex(Int32 index) {
	_indexStack.push_back(index);
}
NEW Byte* Btree::Iterator::PopNode() {
	Byte* result = LastNode();
	if(!_nodeStack.empty())
		_nodeStack.pop_back();
	return result;
}
Int32 Btree::Iterator::PopIndex() {
	Int32 result = LastIndex();
	if(!_indexStack.empty())
		_indexStack.pop_back();
	return result;
}
Byte* Btree::Iterator::LastNode() { return _nodeStack.empty() ? NULL : _nodeStack.back(); }
Int32 Btree::Iterator::LastIndex() { return _indexStack.empty() ? NotFound : _indexStack.back(); }

/// Btree iterator traverse
void Btree::Iterator::Begin(Uint64 loc) {
	Uint64 currentLoc = loc;
	Byte* node = _parent->CreateNode();
	do {
		if(_parent->ReadNode(currentLoc, node)) {
			Uint32 width = _parent->GetWidth(node);
			currentLoc = _parent->GetChildLocation(node, 0);
			if(currentLoc == 0) {
				if(width > 0) {
					_found = true;
					_target = 0;
				}
			}
			else PushIndex(0);
			PushNode(node);
		}
		else break;
	} while(currentLoc > 0);
	CLEAR(node);
}
void Btree::Iterator::End(Uint64 loc) {
	Uint64 currentLoc = loc;
	Byte* node = _parent->CreateNode();
	do {
		if(_parent->ReadNode(currentLoc, node)) {
			Uint32 width = _parent->GetWidth(node);
			currentLoc = _parent->GetChildLocation(node, 0);
			if(currentLoc == 0) {
				if(width > 0) {
					_found = true;
					_target = width - 1;
				}
			}
			else PushIndex(width);
			PushNode(node);
		}
		else break;
	} while(currentLoc > 0);
	CLEAR(node);
}

/// Btree iterator class interface
Byte* Btree::Iterator::Get() {
	Byte* node = LastNode();
	Byte* result = NULL;
	if(_found && node) result = _parent->GetChildItem(node, _target);
	return result;
}
void Btree::Iterator::Prev() {
	Byte* node = LastNode();
	if(!_found || !node) return;

	Uint64 loc = _parent->GetChildLocation(node, _target);
	_found = false;
	if(loc != NotFound && loc > 0) {
		PushIndex(_target);
		End(loc);
	}
	else {
		_found = true;
		_target--;
		while(_target < 0) {
			node = PopNode();
			CLEAR(node);

			_target = PopIndex() - 1;
			if(_target < 0) {
				_found = false;
				_target = NotFound;
				break;
			}
		}
	}
}
void Btree::Iterator::Next() {
	Byte* node = LastNode();
	if(!_found || !node) return;

	Uint64 loc = _parent->GetChildLocation(node, _target + 1);
	_found = false;
	if(loc != NotFound && loc > 0) {
		PushIndex(_target + 1);
		Begin(loc);
	}
	else {
		_found = true;
		_target++;

		Uint32 width = _parent->GetWidth(node);
		while(_target >= width) {
			node = PopNode();
			CLEAR(node);

			_target = PopIndex();
			if(_target < 0) {
				_found = false;
				_target = NotFound;
				break;
			}
			width = _parent->GetWidth(LastNode());
		}
	}
}
