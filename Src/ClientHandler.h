
#pragma once

#include <nstd/HashSet.h>
#include <nstd/Buffer.h>

#include "Tools/Server.h"

#include "DataProtocol.h"
#include "InternalProtocol.h"

class ServerHandler;
class WorkerJob;

class ClientHandler : public Server::Client::Listener
{
public:
  ClientHandler(ServerHandler& serverHandler, Server::Client& client) : serverHandler(serverHandler), client(client), suspended(false) {}
  ~ClientHandler();

  void_t addWorkerJob(WorkerJob& workerJob) {openWorkerJobs.append(&workerJob);}
  void_t suspendWorkerJob(WorkerJob& workerJob) {suspendedWorkerJobs.append(&workerJob);}
  void_t removeWorkerJob(WorkerJob& workerJob) {openWorkerJobs.remove(&workerJob); suspendedWorkerJobs.remove(&workerJob);}

  void_t handleWorkerJob(WorkerJob& workerJob);

  bool_t isSuspended() const {return suspended;}
  void_t suspend();
  void_t resume();

private:
  static Buffer buffer;
  byte_t signature[32];

private:
  ServerHandler& serverHandler;
  Server::Client& client;
  HashSet<WorkerJob*> openWorkerJobs;
  HashSet<WorkerJob*> suspendedWorkerJobs;
  bool_t suspended;

private: // Server::Client::Listener
  virtual size_t handle(byte_t* data, size_t size);
  virtual void_t write() {resume();}

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

  void_t handleInternalLoginResponse(const InternalProtocol::LoginResponse& loginResponse);

  void_t sendErrorResponse(uint32_t requestId, DataProtocol::Error error);
  void_t sendResponse(DataProtocol::Header& header);
};
