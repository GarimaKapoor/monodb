
#ifndef _MONO_HASHMAP_
#define _MONO_HASHMAP_

#include "Define.h"

namespace MonoDB {

	THREAD_UNSAFE template <typename _tk, typename _tv>
	class Hashmap {
		private:
			typedef std::map<_tk, _tv>	HashBundle;
			typedef Uint64 (*HashFunction)(_tk, const Uint64);
			typedef void (*HashFinalizer)(_tk, _tv, void*);
			typedef Bool (*HashFilter)(_tk, _tv, void*);
			typedef Bool (*HashMapper)(_tk, _tv, void*);

			Uint64			_count;
			Uint64			_size;
			HashBundle**	_table;

			HashFunction	_func;
			HashFinalizer	_final;
			HashFilter		_filter;

			void*			_finalArg;
			void*			_filterArg;

		public:
			Hashmap(const Uint32 size, HashFunction func)
				:_count(0), _size(size), _func(func), _final(NULL), _filter(NULL) {
				_table = new HashBundle*[_size];
				for(Uint64 i = 0; i < _size; i++) _table[i] = new HashBundle;
			}
			~Hashmap() {
				Clear();
				for(Uint64 i = 0; i < _size; i++) FREE(_table[i]);
				CLEAR(_table);
			}

			static Uint64 DefaultFunction(_tk key, const Uint64 size) { return key % size; }
			static void DefaultFreeFinalizer(_tk key, _tv value, void* arg) { FREE(value); }
			static void DefaultClearFinalizer(_tk key, _tv value, void* arg) { CLEAR(value); }

			void SetFinalizer(HashFinalizer final, void* arg) { _final = final; _finalArg = arg; }
			void SetFilter(HashFilter filter, void* arg) { _filter = filter; _filterArg = arg; }

			Uint64 Count() { return _count; }
			Bool Get(const _tk key, OUTPUT _tv* value) {
				HashBundle* bundle = _table[_func(key, _size)];
				typename HashBundle::iterator i = bundle->find(key);
				if(i != bundle->end()) {
					*value = i->second;
					return true;
				}
				return false;
			}
			Bool Set(const _tk key, _tv value) {
				if(!_filter || _filter(key, value, _filterArg)) {
					HashBundle* bundle = _table[_func(key, _size)];
					typename HashBundle::iterator i = bundle->find(key);
					if(i != bundle->end()) {
						if(_final) _final(i->first, i->second, _finalArg);
						i->second = value;
						return true;
					}
					else {
						if(bundle->insert(std::pair<_tk, _tv>(key, value)).second) {
							_count++;
							return true;
						}
					}
				}
				return false;
			}
			Bool Remove(const _tk key) {
				HashBundle* bundle = _table[_func(key, _size)];
				typename HashBundle::iterator i = bundle->find(key);
				if(i != bundle->end()) {
					if(_final) _final(i->first, i->second, _finalArg);
					bundle->erase(i);
					_count--;
					return true;
				}
				return false;
			}
			void Clear() {
				for(Uint64 bundleIdx = 0; bundleIdx < _size; bundleIdx++) {
					HashBundle* bundle = _table[bundleIdx];
					if(_final) {
						for(typename HashBundle::iterator i = bundle->begin(); i != bundle->end(); i++)
							_final(i->first, i->second, _finalArg);
					}
					bundle->clear();
				}
				_count = 0;
			}
			Bool Map(HashMapper func, void* arg) {
				for(Uint64 bundleIdx = 0; bundleIdx < _size; bundleIdx++) {
					HashBundle* bundle = _table[bundleIdx];
					for(typename HashBundle::iterator i = bundle->begin(); i != bundle->end(); i++)
						if(!func(i->first, i->second, arg))
							return false;
				}
				return true;
			}
	};

}

#endif
