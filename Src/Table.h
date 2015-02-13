
#pragma once

#include <nstd/String.h>
#include <nstd/List.h>
#include <nstd/HashSet.h>

#include "Tools/TableFile.h"

#include "ClientProtocol.h"

class WorkerJob;
class WorkerHandler;
class Subscription;

class Table
{
public:
  Table(uint32_t id, timestamp_t time, const String& name) : id(id), valid(true), time(time), name(name), workerHandler(0), tableFile(id), lastEntityId(0), minTimeOffset(0x7fffffffffffffffLL) {}

  uint32_t getId() const {return id;}
  const String& getName() const {return name;}
  bool_t isValid() const {return valid;}
  void_t invalidate() {valid = false;}

  bool_t open();
  bool_t create(const ClientProtocol::Entity* entity);
  TableFile& getTableFile() {return tableFile;}

  uint64_t getLastEntityId() const {return lastEntityId;}
  void_t setLastEntityId(uint64_t entityId) {lastEntityId = entityId;}

  uint32_t getEntitySize() const;
  void_t getEntity(ClientProtocol::Table& entity) const;

  size_t getLoad() const {return openWorkerJobs.size();}
  WorkerHandler* getWorkerHandler() {return workerHandler;}
  void_t setWorkerHandler(WorkerHandler* workerHandler) {this->workerHandler = workerHandler;}
  void_t addWorkerJob(WorkerJob& workerJob) {openWorkerJobs.append(&workerJob);}
  void_t removeWorkerJob(WorkerJob& workerJob) {openWorkerJobs.remove(&workerJob);}

  void_t addSubscription(Subscription& subscription) {subscriptions.append(&subscription);}
  void_t removeSubscription(Subscription& subscription) {subscriptions.remove(&subscription);}
  HashSet<Subscription*>& getSubscriptions() {return subscriptions;}

  timestamp_t updateTimeOffset(timestamp_t timeOffset);
  timestamp_t getTimeOffset() const {return minTimeOffset == 0x7fffffffffffffffLL ? 0 : minTimeOffset;}

private:
  uint32_t id;
  bool valid;
  timestamp_t time;
  String name;
  HashSet<WorkerJob*> openWorkerJobs;
  WorkerHandler* workerHandler;
  TableFile tableFile;
  uint64_t lastEntityId;
  HashSet<Subscription*> subscriptions;
  List<timestamp_t> timeOffsets;
  timestamp_t minTimeOffset;
};
