
#pragma once

#include <nstd/HashMap.h>
#include <nstd/MultiMap.h>
#include <nstd/Array.h>

#include "Tools/Server.h"

class ClientHandler;
class WorkerHandler;
class WorkerThread;
class Table;
class User;
class Subscription;
class WorkerJob;
class ControlJob;

class ServerHandler : public Server::Listener
{
public:
  ServerHandler(Server& server);
  ~ServerHandler();

  bool_t loadTables(const String& path = String());

  bool_t createWorkerThread();

  Table& createTable(const String& name);
  void_t removeTable(Table& table);
  const HashMap<uint32_t, Table*>& getTables() const {return tables;}
  Table* findTable(uint32_t id) const;
  Table* findTable(const String& name) const;

  WorkerJob& createWorkerJob(ClientHandler& clientHandler, Table& table, const void* data, size_t size, uint64_t param1);
  void_t removeWorkerJob(WorkerJob& workerJob);

  ControlJob& createControlJob(ClientHandler& clientHandler, Table& table, const void* data, size_t size);
  void_t removeControlJob(ControlJob& controlJob);
  ControlJob* findControlRequest(uint32_t id) {return *controlJobs.find(id);}

  Subscription& createSubscription(ClientHandler& clientHandler, Table& table);
  void_t removeSubscription(Subscription& subscription);

private:
  Server& server;
  uint32_t nextTableId;
  uint32_t nextControlRequestId;
  HashMap<uint32_t, Table*> tables;
  HashMap<String, Table*> tablesByName;
  HashSet<ClientHandler*> clientHandlers;
  HashMap<WorkerHandler*, WorkerThread*> workerThreads;
  HashSet<WorkerHandler*> workerHandlers;
  HashMap<uint32_t, ControlJob*> controlJobs;

private:
  void_t decreaseWorkerHandlerRank(WorkerHandler& workerHandler);
  void_t increaseWorkerHandlerRank(WorkerHandler& workerHandler);

  bool_t loadDirectory(const String& path);
  bool_t loadTable(const String& path);

  void_t removeClient(ClientHandler& clientHandler);

private: // Server::Listener
  virtual void_t acceptedClient(Server::Client& client, uint16_t localPort);
  virtual void_t closedClient(Server::Client& client);
};
