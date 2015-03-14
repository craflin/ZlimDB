
#pragma once

#include <nstd/HashSet.h>
#include <nstd/HashMap.h>
#include <nstd/Buffer.h>

#include "Tools/Server.h"

#include "ClientProtocol.h"

class ServerHandler;
class WorkerJob;
class Subscription;
class Table;

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

  void_t addSubscription(Subscription& subscription);
  void_t removeSubscription(Subscription& subscription);

private:
  static Buffer buffer;
  byte_t signature[32];

private:
  ServerHandler& serverHandler;
  Server::Client& client;
  HashSet<WorkerJob*> openWorkerJobs;
  HashSet<WorkerJob*> suspendedWorkerJobs;
  bool_t suspended;
  HashMap<Table*, Subscription*> subscriptions;

private: // Server::Client::Listener
  virtual size_t handle(byte_t* data, size_t size);
  virtual void_t write() {resume();}

private: 
  void_t handleMessage(ClientProtocol::Header& header);
  void_t handleLogin(const ClientProtocol::LoginRequest& login);
  void_t handleAuth(const ClientProtocol::AuthRequest& auth);
  void_t handleAdd(ClientProtocol::AddRequest& add);
  void_t handleUpdate(const ClientProtocol::UpdateRequest& update);
  void_t handleRemove(const ClientProtocol::RemoveRequest& remove);
  void_t handleSubscribe(const ClientProtocol::SubscribeRequest& subscribe);
  void_t handleUnsubscribe(const ClientProtocol::UnsubscribeRequest& unsubscribe);
  void_t handleQuery(const ClientProtocol::QueryRequest& query);
  void_t handleSync(const ClientProtocol::SyncRequest& sync);

  void_t handleMetaQuery(const ClientProtocol::QueryRequest& query, ClientProtocol::MessageType responseType);

  void_t handleInternalLoginResponse(const ClientProtocol::LoginResponse& loginResponse);
  void_t handleInternalSubscribeResponse(WorkerJob& workerJob, ClientProtocol::Header& subscribeResponse);
  void_t handleInternalErrorResponse(WorkerJob& workerJob, const ClientProtocol::ErrorResponse& errorResponse);

  void_t sendErrorResponse(uint32_t requestId, ClientProtocol::Error error);
  void_t sendOkResponse(ClientProtocol::MessageType type,uint32_t requestId);
  void_t sendResponse(ClientProtocol::Header& header);
};
