
#pragma once

#include <nstd/Thread.h>

#include "Tools/Socket.h"
#include "InternalProtocol.h"

class WorkerJob;
class TableFile;

class WorkerThread
{
public:
  WorkerThread(Socket& socket);

  bool_t start() {return thread.start(main, this);}

private:
  Socket socket;
  Thread thread;
  WorkerJob* currentWorkerJob;

private:
  static uint_t main(void_t* param);

private:
  void_t handleMessage(const ClientProtocol::Header& header);
  void_t handleLogin(const ClientProtocol::Header& header);
  void_t handleAdd(const ClientProtocol::AddRequest& add);
  void_t handleQuery(ClientProtocol::QueryRequest& query);
  void_t handleSubscribe(ClientProtocol::SubscribeRequest& subscribe);
  void_t handleQueryOrSubscribe(ClientProtocol::QueryRequest& query, ClientProtocol::MessageType responseType);

  void_t sendErrorResponse(uint32_t requestId, ClientProtocol::Error error);
};
