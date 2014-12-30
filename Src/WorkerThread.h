
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
  void_t handleMessage(const DataProtocol::Header& header);
  void_t handleLogin(const DataProtocol::Header& header);
  void_t handleCreateTable(const DataProtocol::Header& header);
  void_t handleAdd(const DataProtocol::AddRequest& add);
  void_t handleQuery(const DataProtocol::QueryRequest& query);

  void_t sendErrorResponse(uint32_t requestId, DataProtocol::Error error);
};
