
#include	"../src/Network/Client.h"
#include	"../src/Network/Server.h"
#include	"../src/Log.h"

namespace MonoDB {

	const Uint64 number = 2000;
	const SocketFamily family = AF_INET;
	const Int32	serverPort = 6793;
	const Int32	clientPort = 6953;
	const Char* ip = "127.0.0.1";
	SocketAddress serverAddr, clientAddr;

	static void* ServerTestHandler(OUTPUT void* result) {
		Log l("test/log/Server.log", L_ALL);
		Bool success = true;

		Server* s = new Server(serverAddr);
		if(s->Listen(0)) {
			struct sockaddr clientSocketAddress;
			socklen_t clientSocketLength = 0;
			memset(&clientSocketAddress, 0, sizeof(struct sockaddr));

			Int32 acceptCount = 0;
			while(acceptCount < number) {
				Socket clientSocket = s->Accept(&clientSocketAddress, &clientSocketLength);
				if(clientSocket != Fail) {
					Int32 testValue = (acceptCount + 1) * 3 + (number - acceptCount - 1) * 2;
					Int32 acceptValue = 0;
					
					Int32 received = s->Recv(clientSocket, (Byte*)&acceptValue, sizeof(Int32), 0);
					if(received == sizeof(Int32) && testValue == acceptValue) {
						Int32 sent = s->Send(clientSocket, (Byte*)&acceptValue, sizeof(Int32), 0);
						close(clientSocket);
						
						if(sent != sizeof(Int32)) { // send fail
							success = false;
							l.Show("[ERROR] server send fail \n");
							break;
						}
						else {
							acceptCount++;
							l.Show("[REPORT] server recv and send %d \n", acceptValue);
						}
					}
					else { // recv fail
						success = false;
						close(clientSocket);
						l.Show("[ERROR] server recv fail \n");
						break;
					}
				}
				else { // accept fail
					success = false;
					l.Show("[ERROR] server accept fail \n");
					break;
				}
			}
		}
		else l.Show("[ERROR] server listen fail \n");
		FREE(s);
		memcpy(result, &success, sizeof(Bool));
	}
	static void* ClientTestHandler(OUTPUT void* result) {
		Log l("test/log/Client.log", L_ALL);
		Bool success = true;

		Client* c = new Client(clientAddr);
		for(Int32 i = 0; i < number; i++) {
			if(c->Connect(serverAddr)) {
				Int32 testValue = (i + 1) * 3 + (number - i - 1) * 2;
				Int32 sent = c->Send((Byte*)&testValue, sizeof(Int32), 0);
				if(sent == sizeof(Int32)) {
					Int32 returnValue = 0;
					Int32 received = c->Recv((Byte*)&returnValue, sizeof(Int32), 0);
					c->Disconnect();
					
					if(received != sizeof(Int32) || testValue != returnValue) { // recv fail
						success = false;
						l.Show("[ERROR] client recv \n");
						break;
					}
					else l.Show("[REPORT] client send and recv %d \n", returnValue);
				}
				else { // send fail
					success = false;
					c->Disconnect();
					l.Show("[ERROR] client send \n");
					break;
				}
			}
			else { // connect fail
				success = false;
				l.Show("[ERROR] client connect \n");
				break;
			}
		}
		FREE(c);
		memcpy(result, &success, sizeof(Bool));
	}

	Bool NetworkTestHandler() {
		Bool result = true;

		serverAddr.family = family;
		serverAddr.port = serverPort;
		inet_pton(family, ip, serverAddr.ip);

		clientAddr.family = family;
		clientAddr.port = clientPort;
		inet_pton(family, ip, clientAddr.ip);

		pthread_t server, client;		
		pthread_create(&server, NULL, ServerTestHandler, (Byte*)&result);
		sleep(1);
		pthread_create(&client, NULL, ClientTestHandler, (Byte*)&result);
		pthread_join(client, NULL);
		pthread_join(server, NULL);

		return result;
	}

}
