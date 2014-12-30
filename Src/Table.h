
#pragma once

#include <nstd/String.h>
#include <nstd/List.h>
#include <nstd/HashSet.h>

#include "Tools/TableFile.h"

#include "DataProtocol.h"
#include "WorkerJob.h"

class WorkerHandler;

class Table
{
public:
  Table(uint32_t id, timestamp_t time, const String& name) : id(id), valid(true), time(time), name(name), workerHandler(0) {}

  uint32_t getId() const {return id;}
  const String& getName() const {return name;}
  bool_t isValid() const {return valid;}

  bool_t open();
  bool_t create(const DataProtocol::Entity* entity);
  TableFile& getTableFile() {return tableFile;}

  uint32_t getEntitySize() const;
  void_t getEntity(DataProtocol::Table& entity) const;

  size_t getLoad() const {return openWorkerJobs.size();}
  WorkerHandler* getWorkerHandler() {return workerHandler;}
  void_t setWorkerHandler(WorkerHandler* workerHandler) {this->workerHandler = workerHandler;}
  void_t addWorkerJob(WorkerJob& workerJob) {openWorkerJobs.append(&workerJob);}
  void_t removeWorkerJob(WorkerJob& workerJob) {openWorkerJobs.remove(&workerJob);}

private:
  uint32_t id;
  bool valid;
  timestamp_t time;
  String name;
  HashSet<WorkerJob*> openWorkerJobs;
  WorkerHandler* workerHandler;
  TableFile tableFile;
};
