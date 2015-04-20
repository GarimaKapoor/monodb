
#ifndef _MONO_CACHE_
#define _MONO_CACHE_

#include "Hashmap.h"

namespace MonoDB {

	THREAD_UNSAFE template <typename _tk, typename _tv>
	class Cache {
		private:
			typedef std::list<_tk> CacheQueue;
			typedef struct {
				Uint32 size;
				_tv value;
				typename CacheQueue::iterator iter;
			} CacheItem;
			typedef Hashmap<_tk, CacheItem*> CacheMap;
			typedef Bool (*CacheMapper)(_tk, _tv, void*);
			typedef void (*CacheFinalizer)(_tk, _tv, void*);

			static const Uint64 DefaultHashSize = 1024;
			static void Finalizer(_tk key, CacheItem* item, void* arg) {
				Cache<_tk, _tv>* ch = (Cache<_tk, _tv>*)arg;
				if(ch->_final) ch->_final(key, item->value, ch->_finalArg);
				
				ch->_cacheUsed -= item->size;
				ch->_queue.erase(item->iter);
				FREE(item);
			}

			CacheQueue		_queue;
			CacheMap*		_cache;
			Uint64			_cacheUsed;
			Uint64			_cacheSize;
			
			CacheFinalizer	_final;
			void*			_finalArg;

		public:
			Cache(Uint64 cacheSize)
				:_cacheUsed(0), _cacheSize(cacheSize), _final(NULL) {
				_cache = new CacheMap(Cache::DefaultHashSize, Hashmap<_tk, CacheItem*>::DefaultFunction);
				_cache->SetFinalizer(Cache::Finalizer, this);
			}
			~Cache() { FREE(_cache); }

			Uint32 Count() { return _queue.size(); }
			Uint64 GetCacheUsed() { return _cacheUsed; }
			Uint64 GetCacheSize() { return _cacheSize; }
			void SetCacheSize(Uint64 size) { _cacheSize = size; }
			void SetFinalizer(CacheFinalizer func, void* arg) {
				_final = func;
				_finalArg = arg;
			}

			Bool IsFull() { return (_cacheUsed >= _cacheSize); }
			Bool Get(_tk key, OUTPUT _tv* value) {
				CacheItem* item = NULL;
				if(_cache->Get(key, &item)) {
					*value = item->value;
					_queue.erase(item->iter);
					_queue.push_front(key);
					item->iter = _queue.begin();
					return true;
				}
				return false;
			}
			Bool Add(_tk key, _tv value, Uint32 size) {
				if(size > _cacheSize) {
					if(_final) _final(key, value, _finalArg);
					return false;
				}
				CacheItem* item = NULL;
				if(_cache->Get(key, &item)) {
					if(_final) _final(key, item->value, _finalArg);
					_queue.erase(item->iter);
					_cacheUsed += size - item->size;
				}
				else {
					item = new CacheItem;
					_cacheUsed += size;
					_cache->Set(key, item);
				}
				_queue.push_front(key);
				item->value = value;
				item->size = size;
				item->iter = _queue.begin();
				
				while(_cacheUsed > _cacheSize)
					if(!Remove(_queue.back())) break;
				return true;
			}
			Bool Remove(_tk key) {
				return _cache->Remove(key);
			}
			Bool Clear(Uint32 count) {
				count = (count > 0) ? count : _queue.size();
				if(count > _queue.size()) count = _queue.size();
				
				for(Uint32 i = 0; i < count; i++)
					if(!Remove(_queue.back())) return false;
				return true;
			}
	};

}

#endif
