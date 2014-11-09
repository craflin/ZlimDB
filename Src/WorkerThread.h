
#pragma once

#include <nstd/Thread.h>

#include "Tools/Socket.h"
#include "DataProtocol.h"

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

private:
  static uint_t main(void_t* param);

private:
  void_t handleMessage(TableFile& tableFile, const DataProtocol::Header& header);
};
