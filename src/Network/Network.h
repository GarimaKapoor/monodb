
#ifndef _MONO_NETWORK_
#define _MONO_NETWORK_

#include "../Define.h"
#include <netdb.h>
#include <arpa/inet.h>
#include <sys/types.h>

#if defined(BSD) || defined(MACOS)
#include <netinet/in.h>
#include <sys/socket.h>
#endif

namespace MonoDB {

	const Int32	NetworkIPLength = 16;
	const Int32	ReadableIPLength = 64;

	typedef Int32	Socket;
	typedef Int32	SocketFamily;
	typedef struct {
		SocketFamily	family;
		Int32			port;
		Byte			ip[NetworkIPLength];
	} SocketAddress;

}

#endif
