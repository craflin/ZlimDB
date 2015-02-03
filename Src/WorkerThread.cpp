
#include <nstd/Console.h>
#include <nstd/Debug.h>
#include <nstd/Math.h>

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
    const DataProtocol::Header* header = (const DataProtocol::Header*)(const byte_t*)workerJob->getRequestData();
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

void_t WorkerThread::handleMessage(const DataProtocol::Header& header)
{
  switch((DataProtocol::MessageType)header.message_type)
  {
  case DataProtocol::loginRequest:
    handleLogin(header);
    break;
  case DataProtocol::addRequest:
    handleAdd((DataProtocol::AddRequest&)header);
    break;
  case DataProtocol::queryRequest:
    handleQuery((DataProtocol::QueryRequest&)header);
    break;
  default:
    ASSERT(false);
    break;
  }
}

void_t WorkerThread::handleLogin(const DataProtocol::Header& header)
{
  TableFile& tableFile = currentWorkerJob->getTableFile();
  Buffer userBuffer;
  if(!tableFile.get(1, userBuffer, 0) || userBuffer.size() < sizeof(InternalProtocol::User))
    return sendErrorResponse(header.request_id, DataProtocol::invalidLogin);
  const InternalProtocol::User* user = (const InternalProtocol::User*)(const byte_t*)userBuffer;

  Buffer& responseBuffer = currentWorkerJob->getResponseData();
  responseBuffer.resize(sizeof(InternalProtocol::LoginResponse));
  InternalProtocol::LoginResponse* response = (InternalProtocol::LoginResponse*)(byte_t*)responseBuffer;
  response->header.flags = 0;
  response->header.size = sizeof(*response);
  response->header.message_type = DataProtocol::loginResponse;
  response->header.request_id = header.request_id;
  Memory::copy(&response->pw_salt, &user->pwSalt, sizeof(user->pwSalt));
  for(uint16_t* i = (uint16_t*)response->auth_salt, * end = (uint16_t*)(response->auth_salt + sizeof(response->auth_salt)); i < end; ++i)
      *i = Math::random();
  Sha256::hmac(response->auth_salt, sizeof(response->auth_salt), user->pwHash, sizeof(user->pwHash), response->signature);
}

void_t WorkerThread::handleAdd(const DataProtocol::AddRequest& add)
{
  TableFile& tableFile = currentWorkerJob->getTableFile();
  switch(add.table_id)
  {
  case DataProtocol::tablesTable:
    {
      const DataProtocol::Table* tableEntity = (const DataProtocol::Table*)(&add + 1);
      String tableName;
      if(!DataProtocol::getString(add.header, tableEntity->entity, sizeof(*tableEntity), tableEntity->name_size, tableName))
        return sendErrorResponse(add.header.request_id, DataProtocol::Error::invalidData);

      if(!tableFile.create(tableName))
        return sendErrorResponse(add.header.request_id, DataProtocol::Error::couldNotOpenFile);

      Buffer& responseBuffer = currentWorkerJob->getResponseData();
      responseBuffer.resize(sizeof(DataProtocol::Header));
      DataProtocol::Header* response = (DataProtocol::Header*)(byte_t*)responseBuffer;
      DataProtocol::setHeader(*response, DataProtocol::addResponse, sizeof(*response), add.header.request_id);
      break;
    }
  default:
    {
      const TableFile::DataHeader* data = (const TableFile::DataHeader*)(&add + 1);
      if(data->size != add.header.size - sizeof(add))
        return sendErrorResponse(add.header.request_id, DataProtocol::Error::invalidData);
      if(!tableFile.add(*data))
        return sendErrorResponse(add.header.request_id, DataProtocol::Error::couldNotWriteFile);

      Buffer& responseBuffer = currentWorkerJob->getResponseData();
      responseBuffer.resize(sizeof(DataProtocol::Header));
      DataProtocol::Header* response = (DataProtocol::Header*)(byte_t*)responseBuffer;
      response->flags = 0;
      response->size = sizeof(*response);
      response->message_type = DataProtocol::addResponse;
      response->request_id = add.header.request_id;
      break;
    }
  }
}

void_t WorkerThread::handleQuery(DataProtocol::QueryRequest& query)
{
  return handleQueryOrSubscribe(query, DataProtocol::queryResponse);
}

void_t WorkerThread::handleSubscribe(DataProtocol::SubscribeRequest& subscribe)
{
  return handleQueryOrSubscribe(subscribe, DataProtocol::subscribeResponse);
}

void_t WorkerThread::handleQueryOrSubscribe(DataProtocol::QueryRequest& query, DataProtocol::MessageType responseType)
{
  TableFile& tableFile = currentWorkerJob->getTableFile();
  Buffer& responseBuffer = currentWorkerJob->getResponseData();
  switch(query.type)
  {
  case DataProtocol::QueryType::all:
    {
      uint64_t blockId;
      if(query.param == 0)
      {
        if(!tableFile.getFirstCompressedBlock(blockId, responseBuffer, sizeof(DataProtocol::Header)))
          return sendErrorResponse(query.header.request_id, tableFile.getLastError() == TableFile::notFoundError ? DataProtocol::Error::entityNotFound : DataProtocol::Error::couldNotReadFile);
      }
      else if(!tableFile.getNextCompressedBlock(query.param, blockId, responseBuffer, sizeof(DataProtocol::Header)))
        return sendErrorResponse(query.header.request_id, tableFile.getLastError() == TableFile::notFoundError ? DataProtocol::Error::entityNotFound : DataProtocol::Error::couldNotReadFile);
      DataProtocol::Header* response = (DataProtocol::Header*)(byte_t*)responseBuffer;
      response->flags = DataProtocol::HeaderFlag::compressed;
      if(tableFile.hasNextCompressedBlock(blockId))
        response->flags |= DataProtocol::HeaderFlag::fragmented;
      query.param = blockId;
      response->message_type = responseType;
      response->request_id = query.header.request_id;
      response->size = responseBuffer.size();
      break;
    }
  case DataProtocol::QueryType::byId:
    {
      if(!tableFile.get(query.param, responseBuffer, sizeof(DataProtocol::Header)))
        // todo: distinguish between "not found" and "file io error"
        return sendErrorResponse(query.header.request_id, tableFile.getLastError() == TableFile::notFoundError ? DataProtocol::Error::entityNotFound : DataProtocol::Error::couldNotReadFile);
      DataProtocol::Header* response = (DataProtocol::Header*)(byte_t*)responseBuffer;
      response->flags = 0;
      response->message_type = responseType;
      response->request_id = query.header.request_id;
      response->size = responseBuffer.size();
      break;
    }
  case DataProtocol::QueryType::sinceId:
    {
      uint64_t blockId;
      if(!tableFile.getCompressedBlock(query.param, blockId, responseBuffer, sizeof(DataProtocol::Header)))
        return sendErrorResponse(query.header.request_id, tableFile.getLastError() == TableFile::notFoundError ? DataProtocol::Error::entityNotFound : DataProtocol::Error::couldNotReadFile);
      DataProtocol::Header* response = (DataProtocol::Header*)(byte_t*)responseBuffer;
      response->flags = DataProtocol::HeaderFlag::compressed;
      if(tableFile.hasNextCompressedBlock(blockId))
        response->flags |= DataProtocol::HeaderFlag::fragmented;
      query.type = DataProtocol::QueryType::all;
      query.param = blockId;
      response->message_type = responseType;
      response->request_id = query.header.request_id;
      response->size = responseBuffer.size();
      break;
    }
  case DataProtocol::QueryType::sinceTime:
    {
      uint64_t blockId;
      if(!tableFile.getCompressedBlockByTime(query.param, blockId, responseBuffer, sizeof(DataProtocol::Header)))
        return sendErrorResponse(query.header.request_id, tableFile.getLastError() == TableFile::notFoundError ? DataProtocol::Error::entityNotFound : DataProtocol::Error::couldNotReadFile);
      DataProtocol::Header* response = (DataProtocol::Header*)(byte_t*)responseBuffer;
      response->flags = DataProtocol::HeaderFlag::compressed;
      if(tableFile.hasNextCompressedBlock(blockId))
        response->flags |= DataProtocol::HeaderFlag::fragmented;
      query.type = DataProtocol::QueryType::all;
      query.param = blockId;
      response->message_type = responseType;
      response->request_id = query.header.request_id;
      response->size = responseBuffer.size();
      break;
    }
    break;
  }
}

void_t WorkerThread::sendErrorResponse(uint32_t requestId, DataProtocol::Error error)
{
  Buffer& responseBuffer = currentWorkerJob->getResponseData();
  responseBuffer.resize(sizeof(DataProtocol::ErrorResponse));
  DataProtocol::ErrorResponse* response = (DataProtocol::ErrorResponse*)(byte_t*)responseBuffer;
  response->header.size = sizeof(*response);
  response->header.flags = 0;
  response->header.message_type = DataProtocol::errorResponse;
  response->header.request_id = requestId;
  response->error = error;
}
