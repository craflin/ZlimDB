
#pragma once

#include <nstd/String.h>
#include <nstd/List.h>
#include <nstd/HashSet.h>

#include "Tools/TableFile.h"
#include "Tools/ClientProtocol.h"

class WorkerJob;
class WorkerHandler;
class Subscription;

class Table
{
public:
  Table(uint32_t id, int64_t time, const String& name) : id(id), valid(true), time(time), name(name), workerHandler(0), tableFile(name.isEmpty() ? 0 : new TableFile(id, name)), lastEntityId(0), lastEntityTimestamp(0), minTimeOffset(0x7fffffffffffffffLL) {}
  ~Table() {delete tableFile;}

  uint32_t getId() const {return id;}
  const String& getName() const {return name;}
  bool_t isValid() const {return valid;}
  void_t invalidate() {valid = false;}

  bool_t open();
  bool_t create(const zlimdb_entity* entity);
  TableFile* getTableFile() {return tableFile;}
  const TableFile* getTableFile() const {return tableFile;}

  uint64_t getLastEntityId() const {return lastEntityId;}
  uint64_t getLastEntityTimestamp() const {return lastEntityTimestamp;}
  void_t setLastEntityId(uint64_t id, uint64_t timestamp) {lastEntityId = id; lastEntityTimestamp = timestamp;}

  uint32_t getEntitySize() const;
  void_t getEntity(zlimdb_table_entity& entity) const;

  size_t getLoad() const {return openWorkerJobs.size();}
  WorkerHandler* getWorkerHandler() {return workerHandler;}
  void_t setWorkerHandler(WorkerHandler* workerHandler) {this->workerHandler = workerHandler;}
  void_t addWorkerJob(WorkerJob& workerJob) {openWorkerJobs.append(&workerJob);}
  void_t removeWorkerJob(WorkerJob& workerJob) {openWorkerJobs.remove(&workerJob);}

  void_t addSubscription(Subscription& subscription) {subscriptions.append(&subscription);}
  void_t removeSubscription(Subscription& subscription) {subscriptions.remove(&subscription);}
  HashSet<Subscription*>& getSubscriptions() {return subscriptions;}

  int64_t updateTimeOffset(int64_t timeOffset);
  int64_t getTimeOffset() const {return minTimeOffset == 0x7fffffffffffffffLL ? 0 : minTimeOffset;}

private:
  uint32_t id;
  bool valid;
  int64_t time;
  String name;
  HashSet<WorkerJob*> openWorkerJobs;
  WorkerHandler* workerHandler;
  TableFile* tableFile;
  uint64_t lastEntityId;
  uint64_t lastEntityTimestamp;
  HashSet<Subscription*> subscriptions;
  List<int64_t> timeOffsets;
  int64_t minTimeOffset;
};
