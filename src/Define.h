
#ifndef _MONO_DEFINE_
#define _MONO_DEFINE_

#define NEW
#define RELEASE
#define OUTPUT
#define THREAD_SAFE
#define THREAD_UNSAFE

#define FREE(p)		if(p) { delete p; p = NULL; }
#define CLEAR(p)	if(p) { delete [] p; p = NULL; }

/** C runtime library **/
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cmath>
#include <string>
#include <sstream>

/** Standard library **/
#include <list>
#include <map>

/** Linux kernel library **/
#include <unistd.h>
#include <inttypes.h>
#include <dirent.h>
#include <fcntl.h>
#include <signal.h>
#include <errno.h>
#include <pthread.h>
#include <sys/stat.h>
#include <sys/time.h>

namespace MonoDB {

	typedef bool			Bool;
	typedef	int32_t			Int32;
	typedef	int64_t			Int64;
	typedef uint32_t		Uint32;
	typedef uint64_t		Uint64;
	typedef double			Float;
	typedef char			Byte;
	typedef char			Char;
	typedef	wchar_t			Wchar;
	typedef unsigned char	Uchar;
	typedef struct timeval	Time;

	const Int32	Success = 0;
	const Int32 Fail = -1;
	const Int32 NotFound = -1;

	const Uint64 KB = 1024;
	const Uint64 MB = KB * 1024;
	const Uint64 GB = MB * 1024;
	const Uint64 TB = GB * 1024;

};

#endif
