
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
    const ClientProtocol::Header* header = (const ClientProtocol::Header*)(const byte_t*)workerJob->getRequestData();
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

void_t WorkerThread::handleMessage(const ClientProtocol::Header& header)
{
  switch((ClientProtocol::MessageType)header.message_type)
  {
  case ClientProtocol::loginRequest:
    handleLogin(header);
    break;
  case ClientProtocol::addRequest:
    handleAdd((ClientProtocol::AddRequest&)header);
    break;
  case ClientProtocol::queryRequest:
    handleQuery((ClientProtocol::QueryRequest&)header);
    break;
  default:
    ASSERT(false);
    break;
  }
}

void_t WorkerThread::handleLogin(const ClientProtocol::Header& header)
{
  TableFile& tableFile = currentWorkerJob->getTableFile();
  Buffer userBuffer;
  if(!tableFile.get(1, userBuffer, 0) || userBuffer.size() < sizeof(WorkerProtocol::User))
    return sendErrorResponse(header.request_id, ClientProtocol::invalidLogin);
  const WorkerProtocol::User* user = (const WorkerProtocol::User*)(const byte_t*)userBuffer;

  Buffer& responseBuffer = currentWorkerJob->getResponseData();
  responseBuffer.resize(sizeof(WorkerProtocol::LoginResponse));
  WorkerProtocol::LoginResponse* response = (WorkerProtocol::LoginResponse*)(byte_t*)responseBuffer;
  response->header.flags = 0;
  response->header.size = sizeof(*response);
  response->header.message_type = ClientProtocol::loginResponse;
  response->header.request_id = header.request_id;
  Memory::copy(&response->pw_salt, &user->pwSalt, sizeof(user->pwSalt));
  for(uint16_t* i = (uint16_t*)response->auth_salt, * end = (uint16_t*)(response->auth_salt + sizeof(response->auth_salt)); i < end; ++i)
      *i = Math::random();
  Sha256::hmac(response->auth_salt, sizeof(response->auth_salt), user->pwHash, sizeof(user->pwHash), response->signature);
}

void_t WorkerThread::handleAdd(const ClientProtocol::AddRequest& add)
{
  TableFile& tableFile = currentWorkerJob->getTableFile();
  switch(add.table_id)
  {
  case ClientProtocol::tablesTable:
    {
      const ClientProtocol::Table* tableEntity = (const ClientProtocol::Table*)(&add + 1);
      String tableName;
      if(!ClientProtocol::getString(add.header, tableEntity->entity, sizeof(*tableEntity), tableEntity->name_size, tableName))
        return sendErrorResponse(add.header.request_id, ClientProtocol::Error::invalidMessageData);

      if(!tableFile.create(tableName))
        return sendErrorResponse(add.header.request_id, ClientProtocol::Error::openFile);

      Buffer& responseBuffer = currentWorkerJob->getResponseData();
      responseBuffer.resize(sizeof(ClientProtocol::Header));
      ClientProtocol::Header* response = (ClientProtocol::Header*)(byte_t*)responseBuffer;
      ClientProtocol::setHeader(*response, ClientProtocol::addResponse, sizeof(*response), add.header.request_id);
      break;
    }
  default:
    {
      const TableFile::DataHeader* data = (const TableFile::DataHeader*)(&add + 1);
      if(data->size != add.header.size - sizeof(add))
        return sendErrorResponse(add.header.request_id, ClientProtocol::Error::invalidMessageData);
      if(!tableFile.add(*data))
        return sendErrorResponse(add.header.request_id, ClientProtocol::Error::writeFile);

      Buffer& responseBuffer = currentWorkerJob->getResponseData();
      responseBuffer.resize(sizeof(ClientProtocol::Header));
      ClientProtocol::Header* response = (ClientProtocol::Header*)(byte_t*)responseBuffer;
      response->flags = 0;
      response->size = sizeof(*response);
      response->message_type = ClientProtocol::addResponse;
      response->request_id = add.header.request_id;
      break;
    }
  }
}

void_t WorkerThread::handleQuery(ClientProtocol::QueryRequest& query)
{
  return handleQueryOrSubscribe(query, ClientProtocol::queryResponse);
}

void_t WorkerThread::handleSubscribe(ClientProtocol::SubscribeRequest& subscribe)
{
  return handleQueryOrSubscribe(subscribe, ClientProtocol::subscribeResponse);
}

void_t WorkerThread::handleQueryOrSubscribe(ClientProtocol::QueryRequest& query, ClientProtocol::MessageType responseType)
{
  TableFile& tableFile = currentWorkerJob->getTableFile();
  Buffer& responseBuffer = currentWorkerJob->getResponseData();
  switch(query.type)
  {
  case ClientProtocol::QueryType::all:
    {
      uint64_t blockId;
      if(query.param == 0)
      {
        if(!tableFile.getFirstCompressedBlock(blockId, responseBuffer, sizeof(ClientProtocol::Header)))
          return sendErrorResponse(query.header.request_id, tableFile.getLastError() == TableFile::notFoundError ? ClientProtocol::Error::entityNotFound : ClientProtocol::Error::readFile);
      }
      else if(!tableFile.getNextCompressedBlock(query.param, blockId, responseBuffer, sizeof(ClientProtocol::Header)))
        return sendErrorResponse(query.header.request_id, tableFile.getLastError() == TableFile::notFoundError ? ClientProtocol::Error::entityNotFound : ClientProtocol::Error::readFile);
      ClientProtocol::Header* response = (ClientProtocol::Header*)(byte_t*)responseBuffer;
      response->flags = ClientProtocol::HeaderFlag::compressed;
      if(tableFile.hasNextCompressedBlock(blockId))
        response->flags |= ClientProtocol::HeaderFlag::fragmented;
      query.param = blockId;
      response->message_type = responseType;
      response->request_id = query.header.request_id;
      response->size = responseBuffer.size();
      break;
    }
  case ClientProtocol::QueryType::byId:
    {
      if(!tableFile.get(query.param, responseBuffer, sizeof(ClientProtocol::Header)))
        // todo: distinguish between "not found" and "file io error"
        return sendErrorResponse(query.header.request_id, tableFile.getLastError() == TableFile::notFoundError ? ClientProtocol::Error::entityNotFound : ClientProtocol::Error::readFile);
      ClientProtocol::Header* response = (ClientProtocol::Header*)(byte_t*)responseBuffer;
      response->flags = 0;
      response->message_type = responseType;
      response->request_id = query.header.request_id;
      response->size = responseBuffer.size();
      break;
    }
  case ClientProtocol::QueryType::sinceId:
    {
      uint64_t blockId;
      if(!tableFile.getCompressedBlock(query.param, blockId, responseBuffer, sizeof(ClientProtocol::Header)))
        return sendErrorResponse(query.header.request_id, tableFile.getLastError() == TableFile::notFoundError ? ClientProtocol::Error::entityNotFound : ClientProtocol::Error::readFile);
      ClientProtocol::Header* response = (ClientProtocol::Header*)(byte_t*)responseBuffer;
      response->flags = ClientProtocol::HeaderFlag::compressed;
      if(tableFile.hasNextCompressedBlock(blockId))
        response->flags |= ClientProtocol::HeaderFlag::fragmented;
      query.type = ClientProtocol::QueryType::all;
      query.param = blockId;
      response->message_type = responseType;
      response->request_id = query.header.request_id;
      response->size = responseBuffer.size();
      break;
    }
  case ClientProtocol::QueryType::sinceTime:
    {
      uint64_t blockId;
      if(!tableFile.getCompressedBlockByTime(query.param, blockId, responseBuffer, sizeof(ClientProtocol::Header)))
        return sendErrorResponse(query.header.request_id, tableFile.getLastError() == TableFile::notFoundError ? ClientProtocol::Error::entityNotFound : ClientProtocol::Error::readFile);
      ClientProtocol::Header* response = (ClientProtocol::Header*)(byte_t*)responseBuffer;
      response->flags = ClientProtocol::HeaderFlag::compressed;
      if(tableFile.hasNextCompressedBlock(blockId))
        response->flags |= ClientProtocol::HeaderFlag::fragmented;
      query.type = ClientProtocol::QueryType::all;
      query.param = blockId;
      response->message_type = responseType;
      response->request_id = query.header.request_id;
      response->size = responseBuffer.size();
      break;
    }
    break;
  }
}

void_t WorkerThread::sendErrorResponse(uint32_t requestId, ClientProtocol::Error error)
{
  Buffer& responseBuffer = currentWorkerJob->getResponseData();
  responseBuffer.resize(sizeof(ClientProtocol::ErrorResponse));
  ClientProtocol::ErrorResponse* response = (ClientProtocol::ErrorResponse*)(byte_t*)responseBuffer;
  response->header.size = sizeof(*response);
  response->header.flags = 0;
  response->header.message_type = ClientProtocol::errorResponse;
  response->header.request_id = requestId;
  response->error = error;
}
