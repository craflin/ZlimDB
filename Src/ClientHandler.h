
#pragma once

#include <nstd/HashSet.h>
#include <nstd/HashMap.h>
#include <nstd/Buffer.h>

#include "Tools/Server.h"
#include "Tools/ClientProtocol.h"

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
  void_t handleMessage(zlimdb_header& header);
  void_t handleLogin(const zlimdb_login_request& login);
  void_t handleAuth(const zlimdb_auth_request& auth);
  void_t handleAdd(zlimdb_add_request& add);
  void_t handleUpdate(zlimdb_update_request& update);
  void_t handleRemove(zlimdb_remove_request& remove);
  void_t handleSubscribe(const zlimdb_subscribe_request& subscribe);
  void_t handleUnsubscribe(const zlimdb_unsubscribe_request& unsubscribe);
  void_t handleQuery(const zlimdb_query_request& query);
  void_t handleSync(const zlimdb_sync_request& sync);
  void_t handleClear(zlimdb_clear_request& clear);
  void_t handleFind(zlimdb_find_request& find);
  void_t handleCopy(const zlimdb_copy_request& copy);
  void_t handleControl(zlimdb_control_request& control);

  void_t handleMetaQuery(const zlimdb_query_request& query, zlimdb_message_type responseType);

  void_t handleInternalLoginResponse(const zlimdb_login_response& loginResponse);
  void_t handleInternalAddResponse(WorkerJob& workerJob, const zlimdb_add_response& addResponse);
  void_t handleInternalUpdateResponse(WorkerJob& workerJob, const zlimdb_header& updateResponse);
  void_t handleInternalRemoveResponse(WorkerJob& workerJob, const zlimdb_header& removeResponse);
  void_t handleInternalClearResponse(WorkerJob& workerJob, const zlimdb_header& clearResponse);
  void_t handleInternalSubscribeResponse(WorkerJob& workerJob, zlimdb_header& subscribeResponse);
  void_t handleInternalCopyResponse(WorkerJob& workerJob, zlimdb_header& copyResponse);
  void_t handleInternalErrorResponse(WorkerJob& workerJob, const zlimdb_error_response& errorResponse);

  void_t sendErrorResponse(uint32_t requestId, zlimdb_message_error error);
  void_t sendOkResponse(zlimdb_message_type type,uint32_t requestId);
  void_t sendResponse(zlimdb_header& header);
};
