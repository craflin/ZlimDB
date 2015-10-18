
#include <nstd/Console.h>
#include <nstd/Debug.h>
#include <nstd/Math.h>
#include <nstd/Directory.h>

#include "Tools/TableFile.h"
#include "Tools/Sha256.h"

#include "WorkerThread.h"
#include "WorkerJob.h"

Mutex WorkerThread::directoryMutex;

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
  case zlimdb_message_clear_request:
    handleClear((const zlimdb_clear_request&)header);
    break;
  case zlimdb_message_copy_request:
    handleCopy((const zlimdb_copy_request&)header);
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
      const String& tableName = tableFile.getFileName();
      String dir = File::dirname(tableName);
      bool createResult;
      if(dir != ".")
      {
        directoryMutex.lock();
        Directory::create(dir);
        createResult = tableFile.create();
        directoryMutex.unlock();
      }
      else
        createResult = tableFile.create();

      if(!createResult)
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
      int64_t timeOffset = (int64_t)currentWorkerJob->getParam1();
      if(!tableFile.add(*data, timeOffset))
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
  case zlimdb_table_clients:
  case zlimdb_table_tables:
    {
      // todo: isn't this filtered in clientHandler?
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
      tableFile.close();
      if(!File::unlink(tableFile.getFileName()))
        return sendErrorResponse(remove.header.request_id, zlimdb_error_write_file);

      directoryMutex.lock();
      Directory::purge(File::dirname(tableFile.getFileName()));
      directoryMutex.unlock();

      Buffer& responseBuffer = currentWorkerJob->getResponseData();
      responseBuffer.resize(sizeof(zlimdb_header));
      zlimdb_header* response = (zlimdb_header*)(byte_t*)responseBuffer;
      ClientProtocol::setHeader(*response, zlimdb_message_remove_response, sizeof(*response), remove.header.request_id);
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

void_t WorkerThread::handleQuery(const zlimdb_query_request& query)
{
  return handleQueryOrSubscribe(query, zlimdb_message_query_response);
}

void_t WorkerThread::handleSubscribe(const zlimdb_subscribe_request& subscribe)
{
  return handleQueryOrSubscribe(subscribe, zlimdb_message_subscribe_response);
}

void_t WorkerThread::handleQueryOrSubscribe(const zlimdb_query_request& query, zlimdb_message_type responseType)
{
  TableFile& tableFile = currentWorkerJob->getTableFile();
  Buffer& responseBuffer = currentWorkerJob->getResponseData();
  switch(query.type)
  {
  case zlimdb_query_type_all:
    {
      uint64_t nextBlockId;
      if(currentWorkerJob->getParam1() == 0 ? 
         !tableFile.getFirstCompressedBlock2(nextBlockId, responseBuffer, sizeof(zlimdb_header)) : 
         !tableFile.getCompressedBlock2(currentWorkerJob->getParam1() + 1, nextBlockId, responseBuffer, sizeof(zlimdb_header)))
      {
        if(tableFile.getLastError() != TableFile::notFoundError)
          return sendErrorResponse(query.header.request_id, zlimdb_error_read_file);
        tableFile.getEmptyCompressedBlock2(nextBlockId, responseBuffer, sizeof(zlimdb_header));
      }
      zlimdb_header* response = (zlimdb_header*)(byte_t*)responseBuffer;
      response->flags = zlimdb_header_flag_compressed;
      if(nextBlockId != 0)
      {
        response->flags |= zlimdb_header_flag_fragmented;
        currentWorkerJob->setParam1(nextBlockId - 1);
      }
      else
        currentWorkerJob->setParam1(tableFile.getLastId());
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
      uint64_t nextBlockId;
      if(!tableFile.getCompressedBlock2(currentWorkerJob->getParam1() == 0 ? query.param : (currentWorkerJob->getParam1() + 1), nextBlockId, responseBuffer, sizeof(zlimdb_header)))
        return sendErrorResponse(query.header.request_id, tableFile.getLastError() == TableFile::notFoundError ? zlimdb_error_entity_not_found : zlimdb_error_read_file);
      zlimdb_header* response = (zlimdb_header*)(byte_t*)responseBuffer;
      response->flags = zlimdb_header_flag_compressed;
      if(nextBlockId != 0)
      {
        response->flags |= zlimdb_header_flag_fragmented;
        currentWorkerJob->setParam1(nextBlockId - 1);
      }
      else
        currentWorkerJob->setParam1(tableFile.getLastId());
      response->message_type = responseType;
      response->request_id = query.header.request_id;
      response->size = responseBuffer.size();
      break;
    }
  case zlimdb_query_type_since_time:
    {
      uint64_t nextBlockId;
      if(currentWorkerJob->getParam1() == 0 ?
         !tableFile.getCompressedBlockByTime2(query.param, nextBlockId, responseBuffer, sizeof(zlimdb_header)) :
         !tableFile.getCompressedBlock2(currentWorkerJob->getParam1() + 1, nextBlockId, responseBuffer, sizeof(zlimdb_header)))
      {
        if(tableFile.getLastError() != TableFile::notFoundError)
          return sendErrorResponse(query.header.request_id, zlimdb_error_read_file);
        tableFile.getEmptyCompressedBlock2(nextBlockId, responseBuffer, sizeof(zlimdb_header));
      }
      zlimdb_header* response = (zlimdb_header*)(byte_t*)responseBuffer;
      response->flags = zlimdb_header_flag_compressed;
      if(nextBlockId !=  0)
      {
        response->flags |= zlimdb_header_flag_fragmented;
        currentWorkerJob->setParam1(nextBlockId - 1);
      }
      else
        currentWorkerJob->setParam1(tableFile.getLastId());
      response->message_type = responseType;
      response->request_id = query.header.request_id;
      response->size = responseBuffer.size();
      break;
    }
    break;
  }
}

void_t WorkerThread::handleClear(const zlimdb_clear_request& clear)
{
  TableFile& tableFile = currentWorkerJob->getTableFile();
  if(!tableFile.clear())
    return sendErrorResponse(clear.header.request_id, zlimdb_error_write_file);

  Buffer& responseBuffer = currentWorkerJob->getResponseData();
  responseBuffer.resize(sizeof(zlimdb_header));
  zlimdb_header* response = (zlimdb_header*)(byte_t*)responseBuffer;
  ClientProtocol::setHeader(*response, zlimdb_message_clear_response, sizeof(*response), clear.header.request_id);
}

void_t WorkerThread::handleCopy(const zlimdb_copy_request& copy)
{
  TableFile& tableFile = currentWorkerJob->getTableFile();
  if(currentWorkerJob->getParam1() == 0)
  {
    const zlimdb_table_entity* tableEntity = (const zlimdb_table_entity*)(&copy + 1);

    String copyFileName;
    if(!ClientProtocol::getString(tableEntity->entity, sizeof(zlimdb_table_entity), tableEntity->name_size, copyFileName))
      return sendErrorResponse(copy.header.request_id, zlimdb_error_invalid_message_data);

    const String& fileName = tableFile.getFileName();
    const String tempFileName = File::dirname(fileName) + "/." + File::basename(fileName) + ".tmp";
    if(!tableFile.copy(tempFileName) || !File::rename(tempFileName, copyFileName))
      return sendErrorResponse(copy.header.request_id, zlimdb_error_write_file);

    Buffer& responseBuffer = currentWorkerJob->getResponseData();
    responseBuffer.resize(sizeof(zlimdb_header));
    zlimdb_header* response = (zlimdb_header*)(byte_t*)responseBuffer;
    ClientProtocol::setHeader(*response, zlimdb_message_copy_response, sizeof(*response), copy.header.request_id);
  }
  else
  {
    if(!tableFile.open())
      return sendErrorResponse(copy.header.request_id, zlimdb_error_open_file);

    Buffer& responseBuffer = currentWorkerJob->getResponseData();
    responseBuffer.resize(sizeof(zlimdb_copy_response));
    zlimdb_copy_response* response = (zlimdb_copy_response*)(byte_t*)responseBuffer;
    ClientProtocol::setHeader(response->header, zlimdb_message_copy_response, sizeof(*response), copy.header.request_id);
    response->id = tableFile.getTableId();
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
