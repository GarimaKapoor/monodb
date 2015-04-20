
#ifndef _MONO_SERVER_
#define _MONO_SERVER_

#include "Network.h"

namespace MonoDB {
	THREAD_SAFE class Server {
		private:
			static const Uint32 DefaultTimeout = 500;
			static const Uint32	SendRetry = 8;
			static const Uint32	RecvRetry = 8;

			Socket			_socket;
			SocketAddress	_addr;
			Time			_timeout;
			Bool			_listening;

		public:
			Server(SocketAddress addr)
				:_socket(Fail), _addr(addr), _listening(false) {
				_timeout.tv_sec = Server::DefaultTimeout;
				_timeout.tv_usec = 0;
			}
			~Server() { Terminate(); }

			Bool IsListening() { return _listening; }
			Time GetTimeout() { return _timeout; }
			void SetTimeout(Time timeout) { _timeout = timeout; }

			Bool Listen(Int32 backlog) {
			_socket = socket(_addr.family, SOCK_STREAM, 0);
				if(_socket == Fail) return false;
				
				struct sockaddr socketBuffer;
				socklen_t socketLength = 0;
				
				switch(_addr.family) {
					case AF_INET:
						((struct sockaddr_in*)&socketBuffer)->sin_family = AF_INET;
						((struct sockaddr_in*)&socketBuffer)->sin_port = _addr.port;
						memcpy(&((struct sockaddr_in*)&socketBuffer)->sin_addr,
								_addr.ip, sizeof(NetworkIPLength));
						socketLength = sizeof(struct sockaddr_in);
						break;
						
					case AF_INET6:
						((struct sockaddr_in6*)&socketBuffer)->sin6_family = AF_INET6;
						((struct sockaddr_in6*)&socketBuffer)->sin6_port = _addr.port;
						memcpy(&((struct sockaddr_in6*)&socketBuffer)->sin6_addr,
								_addr.ip, sizeof(NetworkIPLength));
						socketLength = sizeof(struct sockaddr_in6);
						break;
				}
				
				if(socketLength > 0 && (bind(_socket, &socketBuffer, socketLength) == Success)) {
					_listening = (listen(_socket, backlog) == Success);
				}
				else Terminate();
				return _listening;
			}
			void Terminate() {
				if(_socket != Fail) {
					close(_socket);
					_socket = Fail;
				}
				_listening = false;
			}
			Socket Accept(OUTPUT struct sockaddr* addr, OUTPUT socklen_t* length) {
				return (_listening && _socket != Fail) ?
					accept(_socket, addr, length) : Fail;
			}
			Int32 Send(Socket clientSocket, const Byte* buffer, Int32 size, Int32 flag) {
				Int32 result = 0;
				if(_listening) {
					Uint32 trial = 0;
					while(trial < Server::SendRetry) {
						Int32 sent = send(clientSocket, buffer + result, size - result, flag);
						if(sent > 0) {
							result = result + sent;
							trial = 0;
							if(result >= size) break;
						}
						else trial++;
					}
					return result;
				}
				return 0;
			}
			Int32 Recv(Socket clientSocket, OUTPUT Byte* buffer, Int32 size, Int32 flag) {
				Int32 result = 0;
				if(_listening) {
					Uint32 trial = 0;
					while(trial < Server::RecvRetry) {
						Int32 received = recv(clientSocket, buffer + result, size - result, flag);
						if(received > 0) {
							result = result + received;
							trial = 0;
							if(result >= size) break;
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
