
#pragma once

#include <nstd/HashSet.h>

#include "Tools/Server.h"

class ServerHandler;
class WorkerJob;

class WorkerHandler : public Server::Client::Listener
{
public:
  WorkerHandler(ServerHandler& serverHandler, Server::Client& client) : serverHandler(serverHandler), client(client){}
  ~WorkerHandler();

  void_t enqueueJob(WorkerJob& workerJob);
  size_t getLoad() const {return openWorkerJobs.size();}

private:
  ServerHandler& serverHandler;
  Server::Client& client;
  HashSet<WorkerJob*> openWorkerJobs;

private: // Server::Client::Listener
  virtual size_t handle(byte_t* data, size_t size);
  virtual void_t write();

private:
  void_t handleFinishedWorkerJob(WorkerJob& workerJob);
  void_t handleAbortedWorkerJob(WorkerJob& workerJob);
};
