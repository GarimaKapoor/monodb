
#ifndef _MONO_BLOCK_
#define _MONO_BLOCK_

#include "Define.h"

namespace MonoDB {

	THREAD_UNSAFE class Block {
		private:
			Byte*	_buffer;
			Uint32	_size;
			Uint32	_cursor;

		public:
			Block(Byte* buffer, Uint32 size)
				:_buffer(buffer), _size(size), _cursor(0) {}
			Block(Byte* buffer, Uint32 size, Uint32 index)
				:_buffer(buffer), _size(size), _cursor(index) {}
			~Block() {}

			Byte* Get() { return _buffer; }
			Uint32 Size() { return _size; }
			Uint32 Tell() { return _cursor; }
			void Seek(Uint32 index) { _cursor = (index < _size) ? index : _size; }

			Uint32 Read(OUTPUT void* buffer, Uint32 size) {
				if(_cursor + size > _size) size = _size - _cursor;
				if(size > 0) {
					memcpy(buffer, _buffer + _cursor, size);
					_cursor += size;
				}
				return size;
			}
			Uint32 Write(void* buffer, Uint32 size) {
				if(_cursor + size > _size) size = _size - _cursor;
				if(size > 0) {
					memcpy(_buffer + _cursor, buffer, size);
					_cursor += size;
				}
				return size;
			}
	};

};

#endif
