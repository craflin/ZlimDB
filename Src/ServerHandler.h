
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

class ServerHandler : public Server::Listener
{
public:
  ServerHandler(Server& server);
  ~ServerHandler();

  bool_t loadTables(const String& path = String());
  bool_t loadTable(const String& path);

  bool_t createWorkerThread();

  Table* createTable(uint32_t id, const String& name);
  void_t removeTable(Table& table);
  const HashMap<uint32_t, Table*>& getTables() const {return tables;}
  Table* findTable(uint32_t id) const;

  uint64_t findUser(const String& name) const;

  WorkerHandler& getWorkerHandler() {return *workerHandlers.front();}
  void_t decreaseWorkerHandlerRank(WorkerHandler& workerHandler);
  void_t increaseWorkerHandlerRank(WorkerHandler& workerHandler);

private:
  Server& server;
  uint32_t nextTableId;
  HashMap<uint32_t, Table*> tables;
  HashSet<ClientHandler*> clientHandlers;
  HashMap<WorkerHandler*, WorkerThread*> workerThreads;
  HashSet<WorkerHandler*> workerHandlers;
  HashMap<String, uint32_t> users;

private: // Server::Listener
  virtual void_t acceptedClient(Server::Client& client, uint32_t addr, uint16_t port);
  virtual void_t closedClient(Server::Client& client);
};
