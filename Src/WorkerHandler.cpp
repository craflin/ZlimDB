
#include <nstd/Time.h>
#include <nstd/Math.h>

#include "WorkerHandler.h"
#include "ServerHandler.h"
#include "ClientHandler.h"
#include "ClientProtocol.h"
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
  ClientHandler* clientHandler = workerJob.getClientHandler();
  if(clientHandler)
  {
    clientHandler->handleWorkerJob(workerJob);
    bool finished = (((const ClientProtocol::Header*)(const byte_t*)workerJob.getResponseData())->flags & ClientProtocol::HeaderFlag::fragmented) == 0;
    if(finished)
      serverHandler.removeWorkerJob(workerJob);
    else if(!clientHandler->isSuspended())
      workerJob.getTable().getWorkerHandler()->continueWorkerJob(workerJob);
    else
      clientHandler->suspendWorkerJob(workerJob);
  }
  else
    serverHandler.removeWorkerJob(workerJob);
}
