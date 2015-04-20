
#ifndef _MONO_DATA_
#define _MONO_DATA_

#include "../Block.h"
#include "../Cache.h"

namespace MonoDB {
	THREAD_SAFE class Data {
		private:
			class DataItem {
				private:
					Byte*	_buffer;
					Uint32	_size;
					
				public:
					DataItem(Byte* buffer, Uint32 size):_buffer(NULL) { Set(buffer, size); }
					~DataItem() { CLEAR(_buffer); }
					
					Byte* Get() { return _buffer + Data::DefaultHeaderSize; }
					void Set(Byte* buffer, Uint32 size) {
						CLEAR(_buffer);
						_size = size;
						_buffer = new Byte[Data::DefaultHeaderSize + _size];
						
						Block stream(_buffer, Data::DefaultHeaderSize + _size);
						stream.Write(&_size, sizeof(Uint32));
						stream.Write(buffer, _size);
					}
					Uint32	Size() { return _size; }
			};

			static const Uint32	DefaultHeaderSize = sizeof(Uint32);
			static const Uint64	DefaultCacheSize = 4 * MB;
			static const Uint64	DefaultFileSize = 1 * GB;

			std::string		_path;
			Uint32			_count;

			Uint64			_spaceUsed;
			Uint64			_spaceSize;
			pthread_mutex_t	_dataMutex;

			Cache<Uint64, DataItem*>*	_cache;
			Bool						_cacheEnabled;

			std::string GetPath(const Uint32 file);
			Uint32 ReadFromFile(const Uint64 loc, OUTPUT NEW Byte** buffer);
			Uint64 WriteToFile(Uint64 loc, Byte* buffer, Uint32 size);

		public:
			Data(const Char* path, Uint64 space);
			~Data();

			Uint64 SpaceUsed() { return _spaceUsed; }
			Uint64 SpaceSize() { return _spaceSize; }

			void EnableCache() { _cacheEnabled = true; }
			void DisableCache() { _cacheEnabled = false; }

			Uint32 Read(const Uint64 loc, OUTPUT NEW Byte** buffer);
			Uint64 Write(Byte* buffer, Uint32 size);
			Uint64 Write(Uint64 loc, Byte* buffer, Uint32 size);
			Uint64 NextLocation(Uint32 size);
	};
}

#endif
