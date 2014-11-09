
#include <nstd/Time.h>
#include <nstd/Math.h>

#include "WorkerHandler.h"
#include "ServerHandler.h"
#include "ClientHandler.h"
#include "DataProtocol.h"
#include "Table.h"

WorkerHandler::~WorkerHandler()
{
  for(HashSet<WorkerJob*>::Iterator i = openWorkerJobs.begin(), end = openWorkerJobs.end(); i != end; ++i)
    handleAbortedWorkerJob(**i);
}

void_t WorkerHandler::enqueueJob(WorkerJob& workerJob)
{
  openWorkerJobs.append(&workerJob);
}

size_t WorkerHandler::handle(byte_t* data, size_t size)
{
  byte_t* pos = data;
  while(size > 0)
  {
    if(size < sizeof(WorkerJob*))
      break;
    WorkerJob* workerJob = (WorkerJob*)pos;
    handleFinishedWorkerJob(*workerJob);
    pos += sizeof(WorkerJob*);
    size -= sizeof(WorkerJob*);
  }
  return pos - data;
}

void_t WorkerHandler::write() 
{
}

void_t WorkerHandler::handleFinishedWorkerJob(WorkerJob& workerJob)
{
  if(workerJob.isValid())
    workerJob.getClientHandler().handleFinishedWorkerJob(workerJob);
  Table& table = workerJob.getTable();
  table.removeWorkerJob(workerJob);
  if(table.isValid() && table.getLoad() == 0)
    serverHandler.removeTable(table);
}

void_t WorkerHandler::handleAbortedWorkerJob(WorkerJob& workerJob)
{
  if(workerJob.isValid())
    workerJob.getClientHandler().handleAbortedWorkerJob(workerJob);
  Table& table = workerJob.getTable();
  table.removeWorkerJob(workerJob);
  if(table.isValid() && table.getLoad() == 0)
    serverHandler.removeTable(table);
}
