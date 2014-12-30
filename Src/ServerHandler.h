
#pragma once

#include <nstd/HashMap.h>
#include <nstd/MultiMap.h>
#include <nstd/Array.h>

#include "Tools/Server.h"
#include "WorkerJob.h"

class ClientHandler;
class WorkerHandler;
class WorkerThread;
class Table;
class User;

class ServerHandler : public Server::Listener
{
public:
  ServerHandler(Server& server);
  ~ServerHandler();

  bool_t loadTables(const String& path = String());
  bool_t loadTable(const String& path);

  bool_t createWorkerThread();

  Table& createTable(const String& name);
  void_t removeTable(Table& table);
  const HashMap<uint32_t, Table*>& getTables() const {return tables;}
  Table* findTable(uint32_t id) const;
  Table* findTable(const String& name) const;

  WorkerJob& createWorkerJob(ClientHandler& clientHandler, Table& table, const void* data, size_t size);
  void_t removeWorkerJob(WorkerJob& workerJob);

private:
  void_t decreaseWorkerHandlerRank(WorkerHandler& workerHandler);
  void_t increaseWorkerHandlerRank(WorkerHandler& workerHandler);

private:
  Server& server;
  uint32_t nextTableId;
  HashMap<uint32_t, Table*> tables;
  HashMap<String, Table*> tablesByName;
  HashSet<ClientHandler*> clientHandlers;
  HashMap<WorkerHandler*, WorkerThread*> workerThreads;
  HashSet<WorkerHandler*> workerHandlers;

private: // Server::Listener
  virtual void_t acceptedClient(Server::Client& client, uint16_t localPort);
  virtual void_t closedClient(Server::Client& client);
};
