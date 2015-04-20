
#ifndef _MONO_BTREE_
#define _MONO_BTREE_

#include "Data.h"

namespace MonoDB {

	THREAD_UNSAFE class Btree {
		private:
			/** Btree node structure

			+------------------------------------------------+
			| width(4) | locess(8) | B-tree node data(4080) |
			+------------------------------------------------+

			Another 4-byte header is attached by Data class.
			The block has the size of a node.
			Thus, the total block size is 4K.

			In data segment, child locess and key-value pair come in turn.
			(It starts and ends with child locess)
			B-tree node data segment looks like below;

			+------------------------------------------------------------------------------+
			| child(8) | key(x) | value(y) | child(8) | ... | key(x) | value(y) | child(8) |
			+------------------------------------------------------------------------------+

			*/
			typedef Bool (*BtreeHandler)(Byte*, Byte*, void*);
			typedef Int32 (*BtreeCompare)(Byte*, Byte*, void*);

			static const Uint32 HeaderSize = sizeof(Uint32) + sizeof(Uint64);
			static const Uint32 LocSize = sizeof(Uint64);
			static const Uint32	MinimumWidth = 8;

			static const Uint32 RequestHashSize = 4;
			static const Uint32 MetadataHashSize = 16;
			static const Uint32 RecycleSize = LocSize;

			/// Btree callback handler
			static Bool BackupHandler(Uint64 key, Byte* node, void* arg) {
				Btree* bt = (Btree*)arg;
				Bool result = false;

				Uint64 loc = bt->GetLocation(node);
				Byte* backup = bt->CreateNode();

				std::string backupPath = bt->Path() + "backup";
				FILE* backupFp = fopen(backupPath.c_str(), "ab+");
				if(backupFp) {
					fseek(backupFp, 0, SEEK_END);
					if(bt->ReadNodeFromFile(loc, backup)) {
						result = (fwrite(backup, sizeof(Byte), bt->NodeSize(), backupFp) == bt->NodeSize());
					}
					fclose(backupFp);
				}
				CLEAR(backup);
				return result;
			}
			static Bool CommitHandler(Uint64 key, Byte* node, void* arg) {
				Btree* bt = (Btree*)arg;
				Uint32 width = bt->GetWidth(node);
				Uint64 loc = bt->GetLocation(node);
				if(width == 0) {
					bt->ClearNodeFromFile(loc);
					return true;
				}
				else return (bt->WriteNodeToFile(loc, node) == loc);
			}

			/// Btree configuration
			std::string		_path;
			Data*			_file;
			pthread_mutex_t	_treeMutex;

			/// Btree node configuration
			Uint32	_maxWidth;
			Uint64	_nodeSize;
			Uint32	_headerSize;
			Uint32	_keySize;
			Uint32	_valueSize;
			Uint32	_dummySize;

			/// Btree members
			Hashmap<Uint64, Uint64>*	_count;
			Hashmap<Uint64, Byte*>*		_request;
			std::list<Uint64>*			_recycle;

			/// Btree handler and argument
			BtreeHandler	_final;
			BtreeHandler	_filter;
			BtreeCompare	_compare;

			void*	_finalArg;
			void*	_filterArg;
			void*	_compareArg;

			/// Btree node control
			Uint64		CreateTree();
			NEW Byte*	CreateNode();
			void		InitializeNode(OUTPUT Byte* node);
			Int32		CompareItem(Byte* left, Byte* right);

			Bool	IsLeaf(Byte* node);
			Bool	IsFull(const Uint64 loc);
			Bool	IsUnderflow(const Uint64 loc);

			Uint32	GetWidth(Byte* node);
			Uint64	GetLocation(Byte* node);
			Uint64	GetChildLocation(Byte* node, Uint32 index);
			Byte*	GetChildItem(Byte* node, Uint32 index);
			Int32	GetChildIndex(Byte* node, Uint64 loc);

			void SetWidth(Byte* node, Uint32 width);
			void SetLocation(Byte* node, Uint64 loc);
			void SetChildLocation(Byte* node, Uint32 index, Uint64 loc);
			void SetChildKey(Byte* node, Uint32 index, Byte* key);
			void SetChildValue(Byte* node, Uint32 index, Byte* value);
			void SetChildItem(Byte* node, Uint32 index, Byte* item);

			Bool InsertItem(Byte* node, Byte* item, Uint64 prevLoc, Uint64 nextLoc);
			Bool RemoveItem(Byte* node, Uint32 index);
			void CopyItem(Byte* dstNode, Uint32 dstIndex, Byte* srcNode, Uint32 srcIndex);
			void CopyLocation(Byte* dstNode, Uint32 dstIndex, Byte* srcNode, Uint32 srcIndex);

			/// Btree control
			Int32 SearchItem(Uint64 root, Byte* key, OUTPUT std::list<Uint64>& queue, OUTPUT Byte* node);
			Uint64 GetPrevSiblingLocation(Byte* parentNode, Byte* currentNode);
			Uint64 GetNextSiblingLocation(Byte* parentNode, Byte* currentNode);

			Bool PassFirstItemToPrevSibling(Byte* parentNode, Byte* currentNode);
			Bool PassFirstItemToPrevSibling(Byte* parentNode, Byte* currentNode, Byte* prevNode);
			Bool PassLastItemToNextSibling(Byte* parentNode, Byte* currentNode);
			Bool PassLastItemToNextSibling(Byte* parentNode, Byte* currentNode, Byte* nextNode);

			/// Btree request queue
			Bool ReadNodeFromRequest(const Uint64 loc, OUTPUT Byte* node);
			Uint64 WriteNodeToRequest(Uint64 loc, Byte* node);
			Bool ClearNodeFromRequest(const Uint64 loc);

			/// Btree file control
			Bool ReadNodeFromFile(const Uint64 loc, OUTPUT Byte* node);
			Uint64 WriteNodeToFile(Uint64 loc, Byte* node);
			Bool ClearNodeFromFile(const Uint64 loc);
			Bool CommitNodeToFile();
			void RollbackNodeToFile();

			/// Btree node recycle
			void PushNodeToRecycle(const Uint64 loc);
			Uint64 PopNodeFromRecycle();

			/// Btree node interface
			Bool ReadNode(const Uint64 loc, OUTPUT Byte* node);
			Uint64 WriteNode(const Uint64 loc, Byte* node);
			Bool CommitNode();

			friend class Iterator;

		public:
			Btree(const Char* path, Uint32 keySize, Uint32 valueSize, Uint64 space);
			~Btree();

			std::string Path() { return _path; }
			Uint32 MaxWidth() { return _maxWidth; }
			Uint64 NodeSize() { return _nodeSize; }
			Uint32 KeySize() { return _keySize; }
			Uint32 ValueSize() { return _valueSize; }

			Uint64 Count(Uint64 root) {
				Uint64 result = 0;
				_count->Get(root, &result);
				return result;
			}
			Uint64 Count() { return Count(0); }

			void Lock() { pthread_mutex_lock(&_treeMutex); }
			void Unlock() { pthread_mutex_unlock(&_treeMutex); }

			void SetCompare(BtreeCompare func, void* arg) { _compare = func; _compareArg = arg; }
			void SetFilter(BtreeHandler func, void* arg) { _filter = func; _filterArg = arg; }
			void SetFinalizer(BtreeHandler func, void* arg) { _final = func; _finalArg = arg; }

			void EnableCache() { _file->EnableCache(); }
			void DisableCache() { _file->DisableCache(); }

			/// Btree class interface
			Uint64 Create() { return CreateTree(); }
			NEW Byte* CreateItem() { return new Byte[_keySize + _valueSize]; }

			Bool Get(Uint64 root, Byte* key, OUTPUT Byte* value);
			Bool Get(Byte* key, OUTPUT Byte* value) { return Get(0, key, value); }
			Bool Insert(Uint64 root, Byte* item);
			Bool Insert(Byte* item) { return Insert(0, item); }
			Bool Remove(Uint64 root, Byte* key);
			Bool Remove(Byte* key) { return Remove(0, key); }
			Bool Clear(Uint64 root);
			Bool Clear() { return Clear(0); }

			class Iterator {
				private:
					/** node-index stack structure
						+----------------------------------------+
						| n1 | // | n2 | // | ... | nk | // | nl |
						+-----------------------------------+----+
						| // | i1 | // | i2 | ... | // | ik | // |
						+-----------------------------------+----+

						NOTICE! index means a location index, not an item index.
						the target item index is assigned to _target.
					 */
					Btree*	_parent;
					Bool	_found;
					Int32	_target;

					std::list<Byte*>	_nodeStack;
					std::list<Int32>	_indexStack;

					/// Btree iterator control
					void PushNode(Byte* node);
					void PushIndex(Int32 index);
					NEW Byte* PopNode();
					Int32 PopIndex();
					Byte* LastNode();
					Int32 LastIndex();

					/// Btree iterator traverse
					void Begin(Uint64 loc);
					void End(Uint64 loc);

					friend class Btree;

				public:
					Iterator(Btree* parent);
					~Iterator();

					// Btree iterator operator
					Iterator& operator=(const Iterator& iter) {
						if(this != &iter) {
							this->~Iterator();
							this->_parent = iter._parent;
							this->_found = iter._found;
							this->_target = iter._target;

							for(std::list<Byte*>::const_iterator i = iter._nodeStack.begin();
								i != iter._nodeStack.end(); i++) {
								Byte* node = this->_parent->CreateNode();
								memcpy(node, *i, this->_parent->_nodeSize + this->_parent->_dummySize);
								this->PushNode(node);
							}
							for(std::list<Int32>::const_iterator i = iter._indexStack.begin();
								i != iter._indexStack.end(); i++)
								this->PushIndex(*i);
						}
						return (*this);
					}
					void operator++(int flag) { this->Next(); }
					void operator--(int flag) { this->Prev(); }

					/// Btree iterator class interface
					Bool IsFound() { return _found; }
					Byte* Get();
					void Prev();
					void Next();
			};
			Btree::Iterator Begin(Uint64 root);
			Btree::Iterator Begin() { return Begin(0); }
			Btree::Iterator End(Uint64 root);
			Btree::Iterator End() { return End(0); }

			Btree::Iterator Find(Uint64 root, Byte* key);
			Btree::Iterator Find(Byte* key) { return Find(0, key); }
			Btree::Iterator FindBefore(Uint64 root, Byte* key);
			Btree::Iterator FindBefore(Byte* key) { return FindBefore(0, key); }
			Btree::Iterator FindAfter(Uint64 root, Byte* key);
			Btree::Iterator FindAfter(Byte* key) { return FindAfter(0, key); }
	};

}

#endif
