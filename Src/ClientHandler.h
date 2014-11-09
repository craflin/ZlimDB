
#pragma once

#include <nstd/HashSet.h>
#include <nstd/Buffer.h>

#include "Tools/Server.h"
#include "Channel.h"
#include "DataProtocol.h"

class ServerHandler;
class WorkerJob;

class ClientHandler : public Server::Client::Listener
{
public:
  ClientHandler(ServerHandler& serverHandler, Server::Client& client) : serverHandler(serverHandler), client(client) {}
  ~ClientHandler();

  void_t handleFinishedWorkerJob(WorkerJob& workerJob);
  void_t handleAbortedWorkerJob(WorkerJob& workerJob);

private:
  static Buffer buffer;

private:
  ServerHandler& serverHandler;
  Server::Client& client;
  HashSet<WorkerJob*> openWorkerJobs;

private: // Server::Client::Listener
  virtual size_t handle(byte_t* data, size_t size);

private: 
  void_t handleMessage(const DataProtocol::Header& header);
  void_t handleLogin(const DataProtocol::LoginRequest& login);
  void_t handleAuth(const DataProtocol::AuthRequest& auth);
  void_t handleAdd(const DataProtocol::AddRequest& add);
  void_t handleUpdate(const DataProtocol::UpdateRequest& update);
  void_t handleRemove(const DataProtocol::RemoveRequest& remove);
  void_t handleSubscribe(const DataProtocol::SubscribeRequest& subscribe);
  void_t handleUnsubscribe(const DataProtocol::UnsubscribeRequest& unsubscribe);
  void_t handleQuery(const DataProtocol::QueryRequest& query);

  void_t sendErrorResponse(uint32_t requestId, DataProtocol::Error error);
};
