
#include <nstd/Console.h>
#include <nstd/Debug.h>
#include <nstd/Math.h>
#include <nstd/Directory.h>

#include "Tools/TableFile.h"
#include "Tools/Sha256.h"

#include "WorkerThread.h"
#include "WorkerJob.h"

WorkerThread::WorkerThread(Socket& socket)
{
  this->socket.swap(socket);
}

uint_t WorkerThread::main(void_t* param)
{
  WorkerThread* thread = (WorkerThread*)param;
  Socket& socket = thread->socket;
  
  for(;;)
  {
    size_t read = 0;
    WorkerJob* workerJob;
    do
    {
      size_t i = socket.recv((byte_t*)&workerJob + read, sizeof(workerJob) - read);
      if(i <= 0)
      {
        if(i == 0)
          return 0;
        Console::errorf("error: Could not receive from worker socket: %s", (const char_t*)socket.getErrorString());
        return 1;
      }
      read += i;
    } while(read < sizeof(workerJob));

    thread->currentWorkerJob = workerJob;
    const zlimdb_header* header = (const zlimdb_header*)(const byte_t*)workerJob->getRequestData();
    thread->handleMessage(*header);

    size_t sent = 0;
    do
    {
      size_t i = socket.send((const byte_t*)&workerJob, sizeof(workerJob));
      if(i <= 0)
      {
        if(i == 0)
          return 0;
        Console::errorf("error: Could not send to worker socket: %s", (const char_t*)socket.getErrorString());
        return 1;
      }
      sent += i;
    } while(sent < sizeof(workerJob));
  }

  return 0;
}

void_t WorkerThread::handleMessage(const zlimdb_header& header)
{
  switch(header.message_type)
  {
  case zlimdb_message_login_request:
    handleLogin(header);
    break;
  case zlimdb_message_add_request:
    handleAdd((const zlimdb_add_request&)header);
    break;
  case zlimdb_message_update_request:
    handleUpdate((const zlimdb_update_request&)header);
    break;
  case zlimdb_message_remove_request:
    handleRemove((const zlimdb_remove_request&)header);
    break;
  case zlimdb_message_query_request:
    handleQuery((zlimdb_query_request&)header);
    break;
  case zlimdb_message_subscribe_request:
    handleSubscribe((zlimdb_subscribe_request&)header);
    break;
  default:
    ASSERT(false);
    break;
  }
}

void_t WorkerThread::handleLogin(const zlimdb_header& header)
{
  TableFile& tableFile = currentWorkerJob->getTableFile();
  Buffer userBuffer;
  if(!tableFile.get(1, userBuffer, 0) || userBuffer.size() < sizeof(zlimdb_user_entity))
    return sendErrorResponse(header.request_id, zlimdb_error_invalid_login);
  const zlimdb_user_entity* user = (const zlimdb_user_entity*)(const byte_t*)userBuffer;

  Buffer& responseBuffer = currentWorkerJob->getResponseData();
  responseBuffer.resize(sizeof(LoginResponse));
  LoginResponse* response = (LoginResponse*)(byte_t*)responseBuffer;
  ClientProtocol::setHeader(response->header, zlimdb_message_login_response, sizeof(*response), header.request_id);
  Memory::copy(&response->pw_salt, &user->pw_salt, sizeof(user->pw_salt));
  for(uint16_t* i = (uint16_t*)response->auth_salt, * end = (uint16_t*)(response->auth_salt + sizeof(response->auth_salt)); i < end; ++i)
      *i = Math::random();
  Sha256::hmac(response->auth_salt, sizeof(response->auth_salt), user->pw_hash, sizeof(user->pw_hash), response->signature);
}

void_t WorkerThread::handleAdd(const zlimdb_add_request& add)
{
  TableFile& tableFile = currentWorkerJob->getTableFile();
  switch(add.table_id)
  {
  case zlimdb_table_tables:
    {
      const zlimdb_table_entity* tableEntity = (const zlimdb_table_entity*)(&add + 1);
      String tableName;
      if(!ClientProtocol::getString(add.header, tableEntity->entity, sizeof(*tableEntity), tableEntity->name_size, tableName))
        return sendErrorResponse(add.header.request_id, zlimdb_error_invalid_message_data);

      String dir = File::dirname(tableName);
      if(dir != ".")
        Directory::create(dir);

      if(!tableFile.create(tableName))
        return sendErrorResponse(add.header.request_id, zlimdb_error_open_file);

      Buffer& responseBuffer = currentWorkerJob->getResponseData();
      responseBuffer.resize(sizeof(zlimdb_add_response));
      zlimdb_add_response* response = (zlimdb_add_response*)(byte_t*)responseBuffer;
      ClientProtocol::setHeader(response->header, zlimdb_message_add_response, sizeof(*response), add.header.request_id);
      response->id = tableFile.getTableId();
      break;
    }
  default:
    {
      const TableFile::DataHeader* data = (const TableFile::DataHeader*)(&add + 1);
      if(data->size != add.header.size - sizeof(add))
        return sendErrorResponse(add.header.request_id, zlimdb_error_invalid_message_data);
      if(!tableFile.add(*data, currentWorkerJob->getTimeOffset()))
        return sendErrorResponse(add.header.request_id, tableFile.getLastError() == TableFile::argumentError ? zlimdb_error_entity_id : zlimdb_error_write_file);

      Buffer& responseBuffer = currentWorkerJob->getResponseData();
      responseBuffer.resize(sizeof(zlimdb_add_response));
      zlimdb_add_response* response = (zlimdb_add_response*)(byte_t*)responseBuffer;
      ClientProtocol::setHeader(response->header, zlimdb_message_add_response, sizeof(*response), add.header.request_id);
      response->id = data->id;
      break;
    }
  }
}

void_t WorkerThread::handleUpdate(const zlimdb_update_request& update)
{
  TableFile& tableFile = currentWorkerJob->getTableFile();
  switch(update.table_id)
  {
  case zlimdb_table_tables:
    {
      // todo
      break;
    }
  default:
    {
      const TableFile::DataHeader* data = (const TableFile::DataHeader*)(&update + 1);
      if(data->size != update.header.size - sizeof(update))
        return sendErrorResponse(update.header.request_id, zlimdb_error_invalid_message_data);

      if(!tableFile.update(*data))
        return sendErrorResponse(update.header.request_id, tableFile.getLastError() ==TableFile::notFoundError ? zlimdb_error_entity_not_found : zlimdb_error_write_file);

      Buffer& responseBuffer = currentWorkerJob->getResponseData();
      responseBuffer.resize(sizeof(zlimdb_header));
      zlimdb_header* response = (zlimdb_header*)(byte_t*)responseBuffer;
      ClientProtocol::setHeader(*response, zlimdb_message_update_response, sizeof(*response), update.header.request_id);
      break;
    }
  }
}

void_t WorkerThread::handleRemove(const zlimdb_remove_request& remove)
{
  TableFile& tableFile = currentWorkerJob->getTableFile();
  switch(remove.table_id)
  {
  case zlimdb_table_tables:
    {
      // todo
      break;
    }
  default:
    {
      if(!tableFile.remove(remove.id))
        return sendErrorResponse(remove.header.request_id, tableFile.getLastError() ==TableFile::notFoundError ? zlimdb_error_entity_not_found : zlimdb_error_write_file);

      Buffer& responseBuffer = currentWorkerJob->getResponseData();
      responseBuffer.resize(sizeof(zlimdb_header));
      zlimdb_header* response = (zlimdb_header*)(byte_t*)responseBuffer;
      ClientProtocol::setHeader(*response, zlimdb_message_remove_response, sizeof(*response), remove.header.request_id);
      break;
    }
  }
}

void_t WorkerThread::handleQuery(zlimdb_query_request& query)
{
  return handleQueryOrSubscribe(query, zlimdb_message_query_response);
}

void_t WorkerThread::handleSubscribe(zlimdb_subscribe_request& subscribe)
{
  return handleQueryOrSubscribe(subscribe, zlimdb_message_subscribe_response);
}

void_t WorkerThread::handleQueryOrSubscribe(zlimdb_query_request& query, zlimdb_message_type responseType)
{
  TableFile& tableFile = currentWorkerJob->getTableFile();
  Buffer& responseBuffer = currentWorkerJob->getResponseData();
  switch(query.type)
  {
  case zlimdb_query_type_all:
    {
      uint64_t blockId;
      if(query.param == 0)
      {
        if(!tableFile.getFirstCompressedBlock(blockId, responseBuffer, sizeof(zlimdb_header)))
          return sendErrorResponse(query.header.request_id, tableFile.getLastError() == TableFile::notFoundError ? zlimdb_error_entity_not_found : zlimdb_error_read_file);
      }
      else if(!tableFile.getNextCompressedBlock(query.param, blockId, responseBuffer, sizeof(zlimdb_header)))
        return sendErrorResponse(query.header.request_id, tableFile.getLastError() == TableFile::notFoundError ? zlimdb_error_entity_not_found : zlimdb_error_read_file);
      zlimdb_header* response = (zlimdb_header*)(byte_t*)responseBuffer;
      response->flags = zlimdb_header_flag_compressed;
      if(tableFile.hasNextCompressedBlock(blockId))
      {
        response->flags |= zlimdb_header_flag_fragmented;
        query.param = blockId;
      }
      else
        query.param = tableFile.getLastId();
      response->message_type = responseType;
      response->request_id = query.header.request_id;
      response->size = responseBuffer.size();
      break;
    }
  case zlimdb_query_type_by_id:
    {
      if(!tableFile.get(query.param, responseBuffer, sizeof(zlimdb_header)))
        return sendErrorResponse(query.header.request_id, tableFile.getLastError() == TableFile::notFoundError ? zlimdb_error_entity_not_found : zlimdb_error_read_file);
      zlimdb_header* response = (zlimdb_header*)(byte_t*)responseBuffer;
      response->flags = 0;
      response->message_type = responseType;
      response->request_id = query.header.request_id;
      response->size = responseBuffer.size();
      break;
    }
  case zlimdb_query_type_since_id:
    {
      uint64_t blockId;
      if(!tableFile.getCompressedBlock(query.param, blockId, responseBuffer, sizeof(zlimdb_header)))
        return sendErrorResponse(query.header.request_id, tableFile.getLastError() == TableFile::notFoundError ? zlimdb_error_entity_not_found : zlimdb_error_read_file);
      zlimdb_header* response = (zlimdb_header*)(byte_t*)responseBuffer;
      response->flags = zlimdb_header_flag_compressed;
      if(tableFile.hasNextCompressedBlock(blockId))
      {
        response->flags |= zlimdb_header_flag_fragmented;
        query.param = blockId;
      }
      else
        query.param = tableFile.getLastId();
      query.type = zlimdb_query_type_all;
      response->message_type = responseType;
      response->request_id = query.header.request_id;
      response->size = responseBuffer.size();
      break;
    }
  case zlimdb_query_type_since_time:
    {
      uint64_t blockId;
      if(!tableFile.getCompressedBlockByTime(query.param, blockId, responseBuffer, sizeof(zlimdb_header)))
        return sendErrorResponse(query.header.request_id, tableFile.getLastError() == TableFile::notFoundError ? zlimdb_error_entity_not_found : zlimdb_error_read_file);
      zlimdb_header* response = (zlimdb_header*)(byte_t*)responseBuffer;
      response->flags = zlimdb_header_flag_compressed;
      if(tableFile.hasNextCompressedBlock(blockId))
      {
        response->flags |= zlimdb_header_flag_fragmented;
        query.param = blockId;
      }
      else
        query.param = tableFile.getLastId();
      query.type = zlimdb_query_type_all;
      response->message_type = responseType;
      response->request_id = query.header.request_id;
      response->size = responseBuffer.size();
      break;
    }
    break;
  }
}

void_t WorkerThread::sendErrorResponse(uint32_t requestId, zlimdb_message_error error)
{
  Buffer& responseBuffer = currentWorkerJob->getResponseData();
  responseBuffer.resize(sizeof(zlimdb_error_response));
  zlimdb_error_response* response = (zlimdb_error_response*)(byte_t*)responseBuffer;
  ClientProtocol::setHeader(response->header, zlimdb_message_error_response, sizeof(*response), requestId);
  response->error = error;
}
