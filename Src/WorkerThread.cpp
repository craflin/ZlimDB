
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
  case ClientProtocol::updateRequest:
    handleUpdate((ClientProtocol::UpdateRequest&)header);
    break;
  case ClientProtocol::removeRequest:
    handleRemove((ClientProtocol::RemoveRequest&)header);
    break;
  case ClientProtocol::queryRequest:
    handleQuery((ClientProtocol::QueryRequest&)header);
    break;
  case ClientProtocol::subscribeRequest:
    handleSubscribe((ClientProtocol::SubscribeRequest&)header);
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
  if(!tableFile.get(1, userBuffer, 0) || userBuffer.size() < sizeof(ClientProtocol::User))
    return sendErrorResponse(header.request_id, ClientProtocol::invalidLogin);
  const ClientProtocol::User* user = (const ClientProtocol::User*)(const byte_t*)userBuffer;

  Buffer& responseBuffer = currentWorkerJob->getResponseData();
  responseBuffer.resize(sizeof(LoginResponse));
  LoginResponse* response = (LoginResponse*)(byte_t*)responseBuffer;
  ClientProtocol::setHeader(response->header, ClientProtocol::loginResponse, sizeof(*response), header.request_id);
  Memory::copy(&response->pw_salt, &user->pw_salt, sizeof(user->pw_salt));
  for(uint16_t* i = (uint16_t*)response->auth_salt, * end = (uint16_t*)(response->auth_salt + sizeof(response->auth_salt)); i < end; ++i)
      *i = Math::random();
  Sha256::hmac(response->auth_salt, sizeof(response->auth_salt), user->pw_hash, sizeof(user->pw_hash), response->signature);
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
        return sendErrorResponse(add.header.request_id, ClientProtocol::invalidMessageData);

      String dir = File::dirname(tableName);
      if(dir != ".")
        Directory::create(dir);

      if(!tableFile.create(tableName))
        return sendErrorResponse(add.header.request_id, ClientProtocol::openFile);

      Buffer& responseBuffer = currentWorkerJob->getResponseData();
      responseBuffer.resize(sizeof(ClientProtocol::AddResponse));
      ClientProtocol::AddResponse* response = (ClientProtocol::AddResponse*)(byte_t*)responseBuffer;
      ClientProtocol::setHeader(response->header, ClientProtocol::addResponse, sizeof(*response), add.header.request_id);
      response->id = tableFile.getTableId();
      break;
    }
  default:
    {
      const TableFile::DataHeader* data = (const TableFile::DataHeader*)(&add + 1);
      if(data->size != add.header.size - sizeof(add))
        return sendErrorResponse(add.header.request_id, ClientProtocol::invalidMessageData);
      if(!tableFile.add(*data, currentWorkerJob->getTimeOffset()))
        return sendErrorResponse(add.header.request_id, tableFile.getLastError() ==TableFile::argumentError ? ClientProtocol::entityId : ClientProtocol::writeFile);

      Buffer& responseBuffer = currentWorkerJob->getResponseData();
      responseBuffer.resize(sizeof(ClientProtocol::AddResponse));
      ClientProtocol::AddResponse* response = (ClientProtocol::AddResponse*)(byte_t*)responseBuffer;
      ClientProtocol::setHeader(response->header, ClientProtocol::addResponse, sizeof(*response), add.header.request_id);
      response->id = data->id;
      break;
    }
  }
}

void_t WorkerThread::handleUpdate(const ClientProtocol::UpdateRequest& update)
{
  TableFile& tableFile = currentWorkerJob->getTableFile();
  switch(update.table_id)
  {
  case ClientProtocol::tablesTable:
    {
      // todo
      break;
    }
  default:
    {
      const TableFile::DataHeader* data = (const TableFile::DataHeader*)(&update + 1);
      if(data->size != update.header.size - sizeof(update))
        return sendErrorResponse(update.header.request_id, ClientProtocol::invalidMessageData);

      if(!tableFile.update(*data))
        return sendErrorResponse(update.header.request_id, tableFile.getLastError() ==TableFile::notFoundError ? ClientProtocol::entityNotFound : ClientProtocol::writeFile);

      Buffer& responseBuffer = currentWorkerJob->getResponseData();
      responseBuffer.resize(sizeof(ClientProtocol::Header));
      ClientProtocol::Header* response = (ClientProtocol::Header*)(byte_t*)responseBuffer;
      ClientProtocol::setHeader(*response, ClientProtocol::updateResponse, sizeof(*response), update.header.request_id);
      break;
    }
  }
}

void_t WorkerThread::handleRemove(const ClientProtocol::RemoveRequest& remove)
{
  TableFile& tableFile = currentWorkerJob->getTableFile();
  switch(remove.table_id)
  {
  case ClientProtocol::tablesTable:
    {
      // todo
      break;
    }
  default:
    {
      if(!tableFile.remove(remove.id))
        return sendErrorResponse(remove.header.request_id, tableFile.getLastError() ==TableFile::notFoundError ? ClientProtocol::entityNotFound : ClientProtocol::writeFile);

      Buffer& responseBuffer = currentWorkerJob->getResponseData();
      responseBuffer.resize(sizeof(ClientProtocol::Header));
      ClientProtocol::Header* response = (ClientProtocol::Header*)(byte_t*)responseBuffer;
      ClientProtocol::setHeader(*response, ClientProtocol::removeResponse, sizeof(*response), remove.header.request_id);
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
          return sendErrorResponse(query.header.request_id, tableFile.getLastError() == TableFile::notFoundError ? ClientProtocol::entityNotFound : ClientProtocol::readFile);
      }
      else if(!tableFile.getNextCompressedBlock(query.param, blockId, responseBuffer, sizeof(ClientProtocol::Header)))
        return sendErrorResponse(query.header.request_id, tableFile.getLastError() == TableFile::notFoundError ? ClientProtocol::entityNotFound : ClientProtocol::readFile);
      ClientProtocol::Header* response = (ClientProtocol::Header*)(byte_t*)responseBuffer;
      response->flags = ClientProtocol::HeaderFlag::compressed;
      if(tableFile.hasNextCompressedBlock(blockId))
      {
        response->flags |= ClientProtocol::HeaderFlag::fragmented;
        query.param = blockId;
      }
      else
        query.param = tableFile.getLastId();
      response->message_type = responseType;
      response->request_id = query.header.request_id;
      response->size = responseBuffer.size();
      break;
    }
  case ClientProtocol::QueryType::byId:
    {
      if(!tableFile.get(query.param, responseBuffer, sizeof(ClientProtocol::Header)))
        return sendErrorResponse(query.header.request_id, tableFile.getLastError() == TableFile::notFoundError ? ClientProtocol::entityNotFound : ClientProtocol::readFile);
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
        return sendErrorResponse(query.header.request_id, tableFile.getLastError() == TableFile::notFoundError ? ClientProtocol::entityNotFound : ClientProtocol::readFile);
      ClientProtocol::Header* response = (ClientProtocol::Header*)(byte_t*)responseBuffer;
      response->flags = ClientProtocol::HeaderFlag::compressed;
      if(tableFile.hasNextCompressedBlock(blockId))
      {
        response->flags |= ClientProtocol::HeaderFlag::fragmented;
        query.param = blockId;
      }
      else
        query.param = tableFile.getLastId();
      query.type = ClientProtocol::QueryType::all;
      response->message_type = responseType;
      response->request_id = query.header.request_id;
      response->size = responseBuffer.size();
      break;
    }
  case ClientProtocol::QueryType::sinceTime:
    {
      uint64_t blockId;
      if(!tableFile.getCompressedBlockByTime(query.param, blockId, responseBuffer, sizeof(ClientProtocol::Header)))
        return sendErrorResponse(query.header.request_id, tableFile.getLastError() == TableFile::notFoundError ? ClientProtocol::entityNotFound : ClientProtocol::readFile);
      ClientProtocol::Header* response = (ClientProtocol::Header*)(byte_t*)responseBuffer;
      response->flags = ClientProtocol::HeaderFlag::compressed;
      if(tableFile.hasNextCompressedBlock(blockId))
      {
        response->flags |= ClientProtocol::HeaderFlag::fragmented;
        query.param = blockId;
      }
      else
        query.param = tableFile.getLastId();
      query.type = ClientProtocol::QueryType::all;
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
