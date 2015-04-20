
#ifndef _MONO_LOG_
#define _MONO_LOG_

#include "Define.h"
#include <stdarg.h>

namespace MonoDB {
	enum LogLevel {
		L_NONE = 0,
		L_ERROR = 1,
		L_WARNING = 2,
		L_ALL = 3
	};
	THREAD_SAFE class Log {
		private:
			FILE*		_fp;
			LogLevel	_level;

		public:
			Log():_fp(stdout), _level(L_ALL) {}
			Log(LogLevel level):_fp(stdout), _level(level) {}
			Log(const Char* path, LogLevel level):_level(level) { _fp = fopen(path, "w"); }
			~Log() { if(_fp != stdout) fclose(_fp); }

			void Show(const Char* format, ...) {
				va_list args;
				va_start(args, format);
				vfprintf(_fp, format, args);
				va_end(args);
			}
			void Show(LogLevel level, const Char* format, ...) {
				if(_level >= level) {
					va_list args;
					va_start(args, format);
					vfprintf(_fp, format, args);
					va_end(args);
				}
			}
	};

};

#endif
