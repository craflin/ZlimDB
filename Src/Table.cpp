
#include "Table.h"
#include "ServerHandler.h"
#include "WorkerHandler.h"

bool_t Table::open()
{
  return tableFile.open(name);
}

uint32_t Table::getEntitySize() const
{
  return sizeof(DataProtocol::Table) + name.length();
}

void_t Table::getEntity(DataProtocol::Table& entity) const
{
  entity.size = sizeof(DataProtocol::Table) + name.length();
  entity.id = id;
  entity.time = time;
  entity.flags = 0;
  entity.nameSize = name.length();
}

WorkerJob& Table::createWorkerJob(ClientHandler& clientHandler, const void* data, size_t size)
{
  WorkerJob* workerJob = new WorkerJob(clientHandler, *this, data, size, tableFile);
  openWorkerJobs.append(workerJob);
  if(!workerHandler)
  {
    workerHandler = &serverHandler.getWorkerHandler();
    workerHandler->enqueueJob(*workerJob);
  }
  else
  {
    workerHandler->enqueueJob(*workerJob);
    serverHandler.increaseWorkerHandlerRank(*workerHandler);
  }
  return *workerJob;
}

void_t Table::removeWorkerJob(WorkerJob& workerJob)
{
  openWorkerJobs.remove(&workerJob);
  serverHandler.decreaseWorkerHandlerRank(*workerHandler);
  if(openWorkerJobs.isEmpty())
    workerHandler = 0;
  delete &workerJob;
}
