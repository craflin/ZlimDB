
#include <nstd/Time.h>
#include <nstd/Math.h>

#include "ClientHandler.h"
#include "ServerHandler.h"
#include "WorkerJob.h"
#include "DataProtocol.h"
#include "Table.h"

Buffer ClientHandler::buffer;

ClientHandler::~ClientHandler()
{
  for(HashSet<WorkerJob*>::Iterator i = openWorkerJobs.begin(), end = openWorkerJobs.end(); i != end; ++i)
    (*i)->invalidate();
}

size_t ClientHandler::handle(byte_t* data, size_t size)
{
  byte_t* pos = data;
  while(size > 0)
  {
    if(size < sizeof(DataProtocol::Header))
      break;
    DataProtocol::Header* header = (DataProtocol::Header*)pos;
    if(header->size < sizeof(DataProtocol::Header) || header->size >= 5000)
    {
      client.close();
      return 0;
    }
    if(size < header->size)
      break;
    handleMessage(*header);
    pos += header->size;
    size -= header->size;
  }
  return pos - data;
}

void_t ClientHandler::handleMessage(const DataProtocol::Header& header)
{
  switch((DataProtocol::MessageType)header.messageType)
  {
  case DataProtocol::loginRequest:
    if(header.size >= sizeof(DataProtocol::LoginRequest))
      handleLogin((const DataProtocol::LoginRequest&)header);
    else
      sendErrorResponse(header.requestId, DataProtocol::invalidMessageSize);
    break;
  case DataProtocol::authRequest:
    if(header.size >= sizeof(DataProtocol::AuthRequest))
      handleAuth((const DataProtocol::AuthRequest&)header);
    else
      sendErrorResponse(header.requestId, DataProtocol::invalidMessageSize);
    break;
  case DataProtocol::addRequest:
    if(header.size >= sizeof(DataProtocol::AddRequest))
      handleAdd((const DataProtocol::AddRequest&)header);
    else
      sendErrorResponse(header.requestId, DataProtocol::invalidMessageSize);
    break;
  case DataProtocol::updateRequest:
    if(header.size >= sizeof(DataProtocol::UpdateRequest))
      handleUpdate((const DataProtocol::UpdateRequest&)header);
    else
      sendErrorResponse(header.requestId, DataProtocol::invalidMessageSize);
    break;
  case DataProtocol::removeRequest:
    if(header.size >= sizeof(DataProtocol::RemoveRequest))
      handleRemove((const DataProtocol::RemoveRequest&)header);
    else
      sendErrorResponse(header.requestId, DataProtocol::invalidMessageSize);
    break;
  case DataProtocol::subscribeRequest:
    if(header.size >= sizeof(DataProtocol::SubscribeRequest))
      handleSubscribe((const DataProtocol::SubscribeRequest&)header);
    else
      sendErrorResponse(header.requestId, DataProtocol::invalidMessageSize);
    break;
  case DataProtocol::unsubscribeRequest:
    if(header.size >= sizeof(DataProtocol::UnsubscribeRequest))
      handleUnsubscribe((const DataProtocol::UnsubscribeRequest&)header);
    else
      sendErrorResponse(header.requestId, DataProtocol::invalidMessageSize);
    break;
  case DataProtocol::queryRequest:
    if(header.size >= sizeof(DataProtocol::QueryRequest))
      handleQuery((const DataProtocol::QueryRequest&)header);
    else
      sendErrorResponse(header.requestId, DataProtocol::invalidMessageSize);
    break;
  default:
    sendErrorResponse(header.requestId, DataProtocol::invalidMessageType);
    break;
  }
}

void_t ClientHandler::handleLogin(const DataProtocol::LoginRequest& login)
{
  String userName;
  if(!DataProtocol::getString(login, sizeof(DataProtocol::LoginRequest), login.userNameSize, userName))
  {
    sendErrorResponse(login.requestId, DataProtocol::invalidMessageSize);
    return;
  }
  uint64_t userId = serverHandler.findUser(userName);
  if(!userId)
  {
    sendErrorResponse(login.requestId, DataProtocol::invalidLogin);
    return;
  }
  ??
}

void_t ClientHandler::handleAuth(const DataProtocol::AuthRequest& auth)
{
  ??
}

void_t ClientHandler::handleAdd(const DataProtocol::AddRequest& add)
{
  switch(add.tableId)
  {
  case DataProtocol::clientsTable:
  case DataProtocol::timeTable:
    return sendErrorResponse(add.requestId, DataProtocol::invalidRequest);
  case DataProtocol::tablesTable:
  case DataProtocol::usersTable:
  default:
    {
      Table* table = serverHandler.findTable(add.tableId);
      if(!table)
        return sendErrorResponse(add.requestId, DataProtocol::tableNotFound);

      WorkerJob& workerJob = table->createWorkerJob(*this, &add, add.size);
      openWorkerJobs.append(&workerJob);
    }
    break;
  }
}

void_t ClientHandler::handleUpdate(const DataProtocol::UpdateRequest& update)
{
  ??
}

void_t ClientHandler::handleRemove(const DataProtocol::RemoveRequest& remove)
{
  ??
}

void_t ClientHandler::handleSubscribe(const DataProtocol::SubscribeRequest& subscribe)
{
  ??
}

void_t ClientHandler::handleUnsubscribe(const DataProtocol::UnsubscribeRequest& unsubscribe)
{
  ??
}

void_t ClientHandler::handleQuery(const DataProtocol::QueryRequest& query)
{
  switch(query.tableId)
  {
  case DataProtocol::clientsTable:
    return sendErrorResponse(query.requestId, DataProtocol::notImplemented);
  case DataProtocol::tablesTable:
    switch(query.type)
    {
    case DataProtocol::QueryRequest::all:
      {
        const HashMap<uint32_t, Table*>& tables = serverHandler.getTables();
        buffer.resize(4096);
        DataProtocol::Header* response = (DataProtocol::Header*)(byte_t*)buffer;
        response->messageType = DataProtocol::queryResponse;
        response->requestId = query.requestId;
        byte_t* start;
        byte_t* pos = start = (byte_t*)response + sizeof(DataProtocol::Header);
        for(HashMap<uint32_t, Table*>::Iterator i = tables.begin(), end = tables.end(); i != end; ++i)
        {
          const Table* table = *i;
          uint32_t entitySize = table->getEntitySize();
          uint32_t reqBufferSize = pos + entitySize - start;
          if(reqBufferSize > buffer.size())
          {
            response->size = pos - start;
            response->flags = DataProtocol::Header::partial;
            client.send(buffer, response->size);
            pos = start;
          }
          table->getEntity(*(DataProtocol::Table*)pos);
          pos += entitySize;
        }
        response->size = pos - start;
        response->flags = 0;
        client.send(buffer, response->size);
      }
      break;
    case DataProtocol::QueryRequest::byId:
      {
        const Table* table = serverHandler.findTable((uint32_t)query.param);
        if(!table)
          return sendErrorResponse(query.requestId, DataProtocol::entityNotFound);
        buffer.resize(sizeof(DataProtocol::Header) + table->getEntitySize());
        DataProtocol::Header* response = (DataProtocol::Header*)(byte_t*)buffer;
        DataProtocol::Table* tableBuf = (DataProtocol::Table*)((byte_t*)response + sizeof(DataProtocol::Header));
        response->size = sizeof(buffer);
        response->flags = 0;
        response->messageType = DataProtocol::queryResponse;
        response->requestId = query.requestId;
        table->getEntity(*tableBuf);
        client.send(buffer, sizeof(buffer));
      }
      break;
    case DataProtocol::QueryRequest::sinceTime:
      return sendErrorResponse(query.requestId, DataProtocol::notImplemented);
    case DataProtocol::QueryRequest::sinceId:
      return sendErrorResponse(query.requestId, DataProtocol::notImplemented);
    default:
      return sendErrorResponse(query.requestId, DataProtocol::invalidRequest);
    }
    break;
  case DataProtocol::timeTable:
    return sendErrorResponse(query.requestId, DataProtocol::notImplemented);
  case DataProtocol::usersTable:
    return sendErrorResponse(query.requestId, DataProtocol::notImplemented);
  default:
    {
      Table* table = serverHandler.findTable(query.tableId);
      if(!table)
        return sendErrorResponse(query.requestId, DataProtocol::tableNotFound);

      WorkerJob& workerJob = table->createWorkerJob(*this, &query, sizeof(query));
      openWorkerJobs.append(&workerJob);
    }
    break;
  }
}

void_t ClientHandler::sendErrorResponse(uint32_t requestId, DataProtocol::Error error)
{
  DataProtocol::ErrorResponse response;
  response.size = sizeof(response);
  response.messageType = DataProtocol::errorResponse;
  response.requestId = requestId;
  response.error = error;
  client.send((const byte_t*)&response, sizeof(response));
}

void_t ClientHandler::handleFinishedWorkerJob(WorkerJob& workerJob)
{
}

void_t ClientHandler::handleAbortedWorkerJob(WorkerJob& workerJob)
{
}

