
#pragma once

#include <nstd/String.h>
#include <nstd/List.h>
#include <nstd/HashSet.h>

#include "DataProtocol.h"
#include "WorkerJob.h"
#include "Tools/TableFile.h"

class ServerHandler;
class ClientHandler;
class WorkerHandler;

class Table
{
public:
  Table(ServerHandler& serverHandler, uint32_t id, timestamp_t time, const String& name) : serverHandler(serverHandler), id(id), valid(true), time(time), name(name), workerHandler(0) {}

  uint32_t getId() const {return id;}
  bool_t isValid() const {return valid;}

  bool_t open();

  uint32_t getEntitySize() const;
  void_t getEntity(DataProtocol::Table& entity) const;

  WorkerJob& createWorkerJob(ClientHandler& clientHandler, const void* data, size_t size);
  void_t removeWorkerJob(WorkerJob& workerJob);
  size_t getLoad() const {return openWorkerJobs.size();}

private:
  ServerHandler& serverHandler;
  uint32_t id;
  bool valid;
  timestamp_t time;
  String name;
  HashSet<WorkerJob*> openWorkerJobs;
  WorkerHandler* workerHandler;
  TableFile tableFile;
};
