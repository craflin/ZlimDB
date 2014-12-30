
#include <nstd/Time.h>
#include <nstd/Math.h>

#include "WorkerHandler.h"
#include "ServerHandler.h"
#include "ClientHandler.h"
#include "DataProtocol.h"
#include "Table.h"

WorkerHandler::~WorkerHandler()
{
  while(!openWorkerJobs.isEmpty())
    serverHandler.removeWorkerJob(*openWorkerJobs.front());
}

void_t WorkerHandler::addWorkerJob(WorkerJob& workerJob)
{
  openWorkerJobs.append(&workerJob);
  WorkerJob* buffer = &workerJob;
  client.send((const byte_t*)&buffer, sizeof(buffer));
}

void_t WorkerHandler::continueWorkerJob(WorkerJob& workerJob)
{
  WorkerJob* buffer = &workerJob;
  client.send((const byte_t*)&buffer, sizeof(buffer));
}

size_t WorkerHandler::handle(byte_t* data, size_t size)
{
  byte_t* pos = data;
  while(size > 0)
  {
    if(size < sizeof(WorkerJob*))
      break;
    WorkerJob* workerJob = *(WorkerJob**)pos;
    handleWorkerJob(*workerJob);
    pos += sizeof(WorkerJob*);
    size -= sizeof(WorkerJob*);
  }
  return pos - data;
}

void_t WorkerHandler::handleWorkerJob(WorkerJob& workerJob)
{
  if(workerJob.isValid())
  {
    bool finished = (((const DataProtocol::Header*)(const byte_t*)workerJob.getResponseData())->flags & DataProtocol::Header::fragmented) == 0;
    ClientHandler& clientHandler = workerJob.getClientHandler();
    clientHandler.handleWorkerJob(workerJob);
    if(finished)
      serverHandler.removeWorkerJob(workerJob);
    else if(!clientHandler.isSuspended())
      workerJob.getTable().getWorkerHandler()->continueWorkerJob(workerJob);
    else
      clientHandler.suspendWorkerJob(workerJob);
  }
  else
    serverHandler.removeWorkerJob(workerJob);
}
