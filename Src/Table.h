
#pragma once

#include <nstd/String.h>
#include <nstd/List.h>
#include <nstd/HashSet.h>

#include "Tools/TableFile.h"
#include "Tools/ClientProtocol.h"

class WorkerJob;
class ControlJob;
class WorkerHandler;
class Subscription;
class ClientHandler;
class ServerHandler;

class Table
{
public:
  Table(ServerHandler& serverHandler, uint32_t id, int64_t time, const String& name) : serverHandler(serverHandler), id(id), valid(true), available(true), time(time), name(name), workerHandler(0), tableFile(name.isEmpty() ? 0 : new TableFile(id, name)), lastEntityId(0), lastEntityTimestamp(0), minTimeOffset(0x7fffffffffffffffLL), responder(0) {}
  ~Table();

  uint32_t getId() const {return id;}
  const String& getName() const {return name;}
  bool_t isValid() const {return valid;}
  bool_t isAvailable() const {return available;}
  void_t setAvailable(bool_t available) {this->available = available;}
  void_t invalidate() {valid = available = false;}

  bool_t open(); // todo: rename openFile
  bool_t create(const zlimdb_entity* entity); // todo: rename createFile
  TableFile* getTableFile() {return tableFile;}
  const TableFile* getTableFile() const {return tableFile;}

  uint64_t getLastEntityId() const {return lastEntityId;}
  uint64_t getLastEntityTimestamp() const {return lastEntityTimestamp;}
  void_t setLastEntityId(uint64_t id, uint64_t timestamp) {lastEntityId = id; lastEntityTimestamp = timestamp;}

  bool_t copyEntity(zlimdb_table_entity& entity, size_t maxSize) const;

  size_t getLoad() const {return openWorkerJobs.size();}
  WorkerHandler* getWorkerHandler() {return workerHandler;}
  void_t setWorkerHandler(WorkerHandler* workerHandler) {this->workerHandler = workerHandler;}
  void_t addWorkerJob(WorkerJob& workerJob) {openWorkerJobs.append(&workerJob);}
  void_t removeWorkerJob(WorkerJob& workerJob) {openWorkerJobs.remove(&workerJob);}

  void_t addControlJob(ControlJob& controlJob) {openControlJobs.append(&controlJob);}
  void_t removeControlJob(ControlJob& controlJob) {openControlJobs.remove(&controlJob);}

  void_t addSubscription(Subscription& subscription) {subscriptions.append(&subscription);}
  void_t removeSubscription(Subscription& subscription);
  HashSet<Subscription*>& getSubscriptions() {return subscriptions;}

  int64_t updateTimeOffset(int64_t timeOffset);
  int64_t getTimeOffset() const {return minTimeOffset == 0x7fffffffffffffffLL ? 0 : minTimeOffset;}

  void_t setResponder(ClientHandler& responder) {this->responder = &responder;}
  ClientHandler* getResponder() {return responder;}

private:
  ServerHandler& serverHandler;
  uint32_t id;
  bool valid; // the table is not about to be remove
  bool available; // table can be used
  int64_t time;
  String name;
  HashSet<WorkerJob*> openWorkerJobs;
  HashSet<ControlJob*> openControlJobs;
  WorkerHandler* workerHandler;
  TableFile* tableFile;
  uint64_t lastEntityId;
  uint64_t lastEntityTimestamp;
  HashSet<Subscription*> subscriptions;
  List<int64_t> timeOffsets;
  int64_t minTimeOffset;
  ClientHandler* responder;
};
