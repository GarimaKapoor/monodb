
#ifndef _MONO_CLIENT_
#define _MONO_CLIENT_

#include "Network.h"

namespace MonoDB {
	THREAD_SAFE class Client {
		private:
			static const Uint32 DefaultTimeout = 500;
			static const Uint32 ConnectRetry = 5;
			static const Uint32 SendRetry = 8;
			static const Uint32 RecvRetry = 8;

			Socket			_socket;
			SocketAddress	_addr;
			Time			_timeout;
			Bool			_connected;

		public:
			Client(SocketAddress addr)
				:_socket(Fail), _addr(addr), _connected(false) {
				_timeout.tv_sec = Client::DefaultTimeout;
				_timeout.tv_usec = 0;
			}
			~Client() { Disconnect(); }

			Bool IsConnected() { return _connected; }
			Time GetTimeout() { return _timeout; }
			void SetTimeout(Time timeout) { _timeout = timeout; }

			Bool Connect(SocketAddress serverAddress) {
				_socket = socket(_addr.family, SOCK_STREAM, 0);
				if(_socket == Fail) return false;
				
				struct sockaddr	socketBuffer;
				socklen_t socketLength = 0;
				switch(serverAddress.family) {
					case AF_INET:
						((struct sockaddr_in*)&socketBuffer)->sin_family = AF_INET;
						((struct sockaddr_in*)&socketBuffer)->sin_port = serverAddress.port;
						memcpy(&((struct sockaddr_in*)&socketBuffer)->sin_addr,
								serverAddress.ip, sizeof(NetworkIPLength));
						socketLength = sizeof(struct sockaddr_in);
						break;
						
					case AF_INET6:
						((struct sockaddr_in6*)&socketBuffer)->sin6_family = AF_INET6;
						((struct sockaddr_in6*)&socketBuffer)->sin6_port = serverAddress.port;
						memcpy(&((struct sockaddr_in6*)&socketBuffer)->sin6_addr,
								serverAddress.ip, sizeof(NetworkIPLength));
						socketLength = sizeof(struct sockaddr_in6);
						break;
				}
				setsockopt(_socket, SOL_SOCKET, SO_RCVTIMEO, (char*)&_timeout, sizeof(Time));
				
				if(socketLength > 0) {
					for(Uint32 retry = 0; retry < Client::ConnectRetry; retry++) {
						if(connect(_socket, &socketBuffer, socketLength) == Success) {
							_connected = true;
							break;
						}
					}
				}
				if(!_connected) {
					close(_socket);
					_socket = Fail;
				}
				return _connected;
			}
			void Disconnect() {
				if(_connected) {
					if(_socket != Fail) {
						close(_socket);
						_socket = Fail;
					}
					_connected = false;
				}
			}
			Int32 Send(const Byte* buffer, Int32 size, Int32 flag) {
				Int32 result = 0;
				if(_connected) {
					Uint32 trial = 0;
					while(trial < Client::SendRetry) {
						Int32 sent = send(_socket, buffer + result, size - result, flag);
						if(sent > 0) {
							result += sent;
							trial = 0;
							if(result >= size) break;
						}
						else trial++;
					}
					return result;
				}
				return 0;
			}
			Int32	Recv(OUTPUT Byte* buffer, Int32 size, Int32 flag) {
				Int32 result = 0;
				if(_connected) {
					Uint32 trial = 0;
					while(trial < Client::RecvRetry) {
						Int32 received = recv(_socket, buffer + result, size - result, flag);
						if(received > 0) {
							result += received;
							trial = 0;
							if(result >= received) break;
						}
						else trial++;
					}
					return result;
				}
				return 0;
			}
	};
}

#endif
