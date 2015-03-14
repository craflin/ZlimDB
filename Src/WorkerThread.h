
#pragma once

#include <nstd/Thread.h>

#include "Tools/Socket.h"
#include "Tools/ClientProtocol.h"

class WorkerJob;
class TableFile;

class WorkerThread
{
public:
  struct LoginResponse : public zlimdb_login_response
  {
    byte_t signature[32];
  };

public:
  WorkerThread(Socket& socket);

  bool_t start() {return thread.start(main, this);}

private:
  Socket socket;
  Thread thread;
  WorkerJob* currentWorkerJob;

private:
  static uint_t main(void_t* param);

private:
  void_t handleMessage(const zlimdb_header& header);
  void_t handleLogin(const zlimdb_header& header);
  void_t handleAdd(const zlimdb_add_request& add);
  void_t handleUpdate(const zlimdb_update_request& update);
  void_t handleRemove(const zlimdb_remove_request& remove);
  void_t handleQuery(zlimdb_query_request& query);
  void_t handleSubscribe(zlimdb_subscribe_request& subscribe);
  void_t handleQueryOrSubscribe(zlimdb_query_request& query, zlimdb_message_type responseType);

  void_t sendErrorResponse(uint32_t requestId, zlimdb_message_error error);
};
