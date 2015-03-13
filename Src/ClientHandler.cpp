
#include <nstd/Time.h>
#include <nstd/Math.h>

#include "ClientHandler.h"
#include "ServerHandler.h"
#include "WorkerJob.h"
#include "WorkerHandler.h"
#include "WorkerProtocol.h"
#include "Table.h"
#include "Subscription.h"

Buffer ClientHandler::buffer;

ClientHandler::~ClientHandler()
{
  for(HashSet<WorkerJob*>::Iterator i = openWorkerJobs.begin(), end = openWorkerJobs.end(); i != end; ++i)
    (*i)->detachClientHandler(); // invalidate
  for(HashSet<WorkerJob*>::Iterator i = suspendedWorkerJobs.begin(), end = suspendedWorkerJobs.end(); i != end; ++i)
  {
    WorkerJob* workerJob = *i;
    workerJob->detachClientHandler();
    serverHandler.removeWorkerJob(*workerJob);
  }
  for(HashMap<Table*, Subscription*>::Iterator i = subscriptions.begin(), end = subscriptions.end(); i != end; ++i)
  {
    Subscription* subscription = *i;
    subscription->detachClientHandler();
    serverHandler.removeSubscription(*subscription);
  }
}

size_t ClientHandler::handle(byte_t* data, size_t size)
{
  byte_t* pos = data;
  while(size > 0)
  {
    if(size < sizeof(ClientProtocol::Header))
      break;
    ClientProtocol::Header* header = (ClientProtocol::Header*)pos;
    if(header->size < sizeof(ClientProtocol::Header) || header->size >= sizeof(ClientProtocol::AddRequest) + 0xffff)
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

void_t ClientHandler::handleMessage(ClientProtocol::Header& header)
{
  switch((ClientProtocol::MessageType)header.message_type)
  {
  case ClientProtocol::loginRequest:
    if(header.size >= sizeof(ClientProtocol::LoginRequest))
      handleLogin((const ClientProtocol::LoginRequest&)header);
    else
      sendErrorResponse(header.request_id, ClientProtocol::invalidMessageSize);
    break;
  case ClientProtocol::authRequest:
    if(header.size >= sizeof(ClientProtocol::AuthRequest))
      handleAuth((const ClientProtocol::AuthRequest&)header);
    else
      sendErrorResponse(header.request_id, ClientProtocol::invalidMessageSize);
    break;
  case ClientProtocol::addRequest:
    if(header.size >= sizeof(ClientProtocol::AddRequest))
      handleAdd((ClientProtocol::AddRequest&)header);
    else
      sendErrorResponse(header.request_id, ClientProtocol::invalidMessageSize);
    break;
  case ClientProtocol::updateRequest:
    if(header.size >= sizeof(ClientProtocol::UpdateRequest))
      handleUpdate((const ClientProtocol::UpdateRequest&)header);
    else
      sendErrorResponse(header.request_id, ClientProtocol::invalidMessageSize);
    break;
  case ClientProtocol::removeRequest:
    if(header.size >= sizeof(ClientProtocol::RemoveRequest))
      handleRemove((const ClientProtocol::RemoveRequest&)header);
    else
      sendErrorResponse(header.request_id, ClientProtocol::invalidMessageSize);
    break;
  case ClientProtocol::subscribeRequest:
    if(header.size >= sizeof(ClientProtocol::SubscribeRequest))
      handleSubscribe((const ClientProtocol::SubscribeRequest&)header);
    else
      sendErrorResponse(header.request_id, ClientProtocol::invalidMessageSize);
    break;
  case ClientProtocol::unsubscribeRequest:
    if(header.size >= sizeof(ClientProtocol::UnsubscribeRequest))
      handleUnsubscribe((const ClientProtocol::UnsubscribeRequest&)header);
    else
      sendErrorResponse(header.request_id, ClientProtocol::invalidMessageSize);
    break;
  case ClientProtocol::queryRequest:
    if(header.size >= sizeof(ClientProtocol::QueryRequest))
      handleQuery((const ClientProtocol::QueryRequest&)header);
    else
      sendErrorResponse(header.request_id, ClientProtocol::invalidMessageSize);
    break;
  case ClientProtocol::syncRequest:
    if(header.size >= sizeof(ClientProtocol::SyncRequest))
      handleSync((const ClientProtocol::SyncRequest&)header);
    else
      sendErrorResponse(header.request_id, ClientProtocol::invalidMessageSize);
    break;
  default:
    sendErrorResponse(header.request_id, ClientProtocol::invalidMessageType);
    break;
  }
}

void_t ClientHandler::handleLogin(const ClientProtocol::LoginRequest& login)
{
  String userName;
  if(!ClientProtocol::getString(login.header, sizeof(ClientProtocol::LoginRequest), login.user_name_size, userName))
    return sendErrorResponse(login.header.request_id, ClientProtocol::invalidMessageData);
  Table* table = serverHandler.findTable(String("users/") + userName + "/user");
  if(!table)
    return sendErrorResponse(login.header.request_id, ClientProtocol::invalidLogin);
  serverHandler.createWorkerJob(*this, *table, &login, sizeof(login));
}

void_t ClientHandler::handleAuth(const ClientProtocol::AuthRequest& auth)
{
  bool_t failed = Memory::compare(&auth.signature, &signature, sizeof(signature)) != 0;
  Memory::zero(&signature, sizeof(signature));
  if(failed)
  {
    sendErrorResponse(auth.header.request_id, ClientProtocol::invalidLogin);
    return;
  }

  ClientProtocol::Header authResponse;
  authResponse.size = sizeof(authResponse);
  authResponse.message_type = ClientProtocol::authResponse;
  authResponse.request_id = auth.header.request_id;
  sendResponse(authResponse);
}

void_t ClientHandler::handleAdd(ClientProtocol::AddRequest& add)
{
  switch((ClientProtocol::TableId)add.table_id)
  {
  case ClientProtocol::clientsTable:
    return sendErrorResponse(add.header.request_id, ClientProtocol::invalidRequest);
  case ClientProtocol::tablesTable:
    if(add.header.size < sizeof(add) + sizeof(ClientProtocol::Table))
      return sendErrorResponse(add.header.request_id, ClientProtocol::invalidMessageSize);
    else
    {
      // get table name
      String tableName;
      const ClientProtocol::Table* tableEntity = (const ClientProtocol::Table*)(&add + 1);
      if(!ClientProtocol::getString(add.header, tableEntity->entity, sizeof(*tableEntity), tableEntity->name_size, tableName))
        return sendErrorResponse(add.header.request_id, ClientProtocol::invalidMessageData);

      // create table without opening the file
      Table* table = serverHandler.findTable(tableName);
      if(table)
      {
        ClientProtocol::AddResponse addResponse;
        ClientProtocol::setHeader(addResponse.header, ClientProtocol::addResponse, sizeof(addResponse), add.header.request_id);
        addResponse.id = table->getId();
        return sendResponse(addResponse.header);
      }
      table = &serverHandler.createTable(tableName);

      // create internal job to create the file
      serverHandler.createWorkerJob(*this, *table, &add, add.header.size);

      // notify subscribers
      table = serverHandler.findTable(ClientProtocol::tablesTable);
      HashSet<Subscription*>& subscriptions = table->getSubscriptions();
      for(HashSet<Subscription*>::Iterator i = subscriptions.begin(), end = subscriptions.end(); i != end; ++i)
      {
        Subscription* subscription = *i;
        if(subscription->isSynced())
          subscription->getClientHandler()->client.send((const byte_t*)&add, add.header.size);
      }
    }
    break;
  default:
    if(add.header.size < sizeof(add) + sizeof(ClientProtocol::Entity))
      return sendErrorResponse(add.header.request_id, ClientProtocol::invalidMessageSize);
    else
    {
      timestamp_t now = Time::time();

      // find table
      Table* table = serverHandler.findTable(add.table_id);
      if(!table)
        return sendErrorResponse(add.header.request_id, ClientProtocol::tableNotFound);

      // create id and timestamp?
      ClientProtocol::Entity* entity = (ClientProtocol::Entity*)(&add + 1);
      if(entity->id == 0)
        entity->id = table->getLastEntityId() + 1;
      if(entity->time == 0)
        entity->time = now;
      table->setLastEntityId(entity->id);
      timestamp_t timeOffset = table->updateTimeOffset(now - entity->time);

      // create job to add entity
      WorkerJob& workerJob = serverHandler.createWorkerJob(*this, *table, &add, add.header.size);
      workerJob.setTimeOffset(timeOffset);

      // notify subscribers
      HashSet<Subscription*>& subscriptions = table->getSubscriptions();
      for(HashSet<Subscription*>::Iterator i = subscriptions.begin(), end = subscriptions.end(); i != end; ++i)
      {
        Subscription* subscription = *i;
        if(subscription->isSynced())
          subscription->getClientHandler()->client.send((const byte_t*)&add, add.header.size);
      }
    }
    break;
  }
}

void_t ClientHandler::handleUpdate(const ClientProtocol::UpdateRequest& update)
{
  switch((ClientProtocol::TableId)update.table_id)
  {
  case ClientProtocol::clientsTable:
  case ClientProtocol::tablesTable:
    return sendErrorResponse(update.header.request_id, ClientProtocol::invalidRequest);
  default:
    {
      Table* table = serverHandler.findTable(update.table_id);
      if(!table)
        return sendErrorResponse(update.header.request_id, ClientProtocol::tableNotFound);

      serverHandler.createWorkerJob(*this, *table, &update, update.header.size);

      // notify subscribers
      HashSet<Subscription*>& subscriptions = table->getSubscriptions();
      for(HashSet<Subscription*>::Iterator i = subscriptions.begin(), end = subscriptions.end(); i != end; ++i)
      {
        Subscription* subscription = *i;
        subscription->getClientHandler()->client.send((const byte_t*)&update, update.header.size);
      }
    }
    break;
  }
}

void_t ClientHandler::handleRemove(const ClientProtocol::RemoveRequest& remove)
{
  switch((ClientProtocol::TableId)remove.table_id)
  {
  case ClientProtocol::clientsTable:
    return sendErrorResponse(remove.header.request_id, ClientProtocol::invalidRequest);
  case ClientProtocol::tablesTable:
    return sendErrorResponse(remove.header.request_id, ClientProtocol::notImplemented);
  default:
    {
      Table* table = serverHandler.findTable(remove.table_id);
      if(!table)
        return sendErrorResponse(remove.header.request_id, ClientProtocol::tableNotFound);

      serverHandler.createWorkerJob(*this, *table, &remove, remove.header.size);

      // notify subscribers
      HashSet<Subscription*>& subscriptions = table->getSubscriptions();
      for(HashSet<Subscription*>::Iterator i = subscriptions.begin(), end = subscriptions.end(); i != end; ++i)
      {
        Subscription* subscription = *i;
        subscription->getClientHandler()->client.send((const byte_t*)&remove, remove.header.size);
      }
    }
    break;
  }
}

void_t ClientHandler::handleSubscribe(const ClientProtocol::SubscribeRequest& subscribe)
{
  Table* table = serverHandler.findTable(subscribe.table_id);
  if(!table)
    return sendErrorResponse(subscribe.header.request_id, ClientProtocol::tableNotFound);
  if(subscriptions.contains(table))
    return sendOkResponse(ClientProtocol::subscribeResponse, subscribe.header.request_id);
  Subscription& subscription = serverHandler.createSubscription(*this, *table);
  if(table->getTableFile())
  {
    if(table->getLastEntityId() == 0)
      subscription.setSynced();
      // todo: send an answer
    else
      serverHandler.createWorkerJob(*this, *table, &subscribe, sizeof(subscribe));
  }
  else
  {
    handleMetaQuery(subscribe, ClientProtocol::subscribeResponse);
    subscription.setSynced();
  }
}

void_t ClientHandler::handleUnsubscribe(const ClientProtocol::UnsubscribeRequest& unsubscribe)
{
  Table* table = serverHandler.findTable(unsubscribe.table_id);
  if(!table)
    return sendErrorResponse(unsubscribe.header.request_id, ClientProtocol::subscriptionNotFound);
  HashMap<Table*, Subscription*>::Iterator it = subscriptions.find(table);
  if(it == subscriptions.end())
    return sendErrorResponse(unsubscribe.header.request_id, ClientProtocol::subscriptionNotFound);
  serverHandler.removeSubscription(**it);
}

void_t ClientHandler::handleQuery(const ClientProtocol::QueryRequest& query)
{
  Table* table = serverHandler.findTable(query.table_id);
  if(!table)
    return sendErrorResponse(query.header.request_id, ClientProtocol::tableNotFound);
  if(table->getTableFile())
    serverHandler.createWorkerJob(*this, *table, &query, sizeof(query));
  else
    handleMetaQuery(query, ClientProtocol::queryResponse);
}

void_t ClientHandler::handleSync(const ClientProtocol::SyncRequest& sync)
{
  switch((ClientProtocol::TableId)sync.table_id)
  {
  case ClientProtocol::clientsTable:
  case ClientProtocol::tablesTable:
    {
      ClientProtocol::SyncResponse syncResponse;
      ClientProtocol::setHeader(syncResponse.header, ClientProtocol::syncResponse, sizeof(syncResponse), sync.header.request_id);
      syncResponse.table_time = syncResponse.server_time = Time::time();
      return sendResponse(syncResponse.header);
    }
  default:
    {
      timestamp_t now = Time::time();

      Table* table = serverHandler.findTable(sync.table_id);
      if(!table)
        return sendErrorResponse(sync.header.request_id, ClientProtocol::tableNotFound);

      ClientProtocol::SyncResponse syncResponse;
      ClientProtocol::setHeader(syncResponse.header, ClientProtocol::syncResponse, sizeof(syncResponse), sync.header.request_id);
      syncResponse.table_time = now;
      syncResponse.server_time = now + table->getTimeOffset();
      return sendResponse(syncResponse.header);
    }
  }
}

void_t ClientHandler::handleMetaQuery(const ClientProtocol::QueryRequest& query, ClientProtocol::MessageType responseType)
{
  switch(query.table_id)
  {
  case ClientProtocol::tablesTable:
    switch(query.type)
    {
    case ClientProtocol::QueryType::all:
      {
        const HashMap<uint32_t, Table*>& tables = serverHandler.getTables();
        buffer.resize(4096);
        ClientProtocol::Header* response = (ClientProtocol::Header*)(byte_t*)buffer;
        response->message_type = responseType;
        response->request_id = query.header.request_id;
        byte_t* start;
        byte_t* pos = start = (byte_t*)response + sizeof(ClientProtocol::Header);
        for(HashMap<uint32_t, Table*>::Iterator i = tables.begin(), end = tables.end(); i != end; ++i)
        {
          const Table* table = *i;
          if(!table->getTableFile())
            continue;
          uint32_t entitySize = table->getEntitySize();
          uint32_t reqBufferSize = pos + entitySize - start;
          if(reqBufferSize > buffer.size())
          {
            response->size = pos - start;
            response->flags = ClientProtocol::HeaderFlag::fragmented;
            client.send(buffer, response->size);
            pos = start;
          }
          table->getEntity(*(ClientProtocol::Table*)pos);
          pos += entitySize;
        }
        response->size = pos - start + sizeof(ClientProtocol::Header);
        response->flags = 0;
        client.send(buffer, response->size);
      }
      break;
    case ClientProtocol::QueryType::byId:
      {
        const Table* table = serverHandler.findTable((uint32_t)query.param);
        if(!table)
          return sendErrorResponse(query.header.request_id, ClientProtocol::entityNotFound);
        buffer.resize(sizeof(ClientProtocol::Header) + table->getEntitySize());
        ClientProtocol::Header* response = (ClientProtocol::Header*)(byte_t*)buffer;
        ClientProtocol::Table* tableBuf = (ClientProtocol::Table*)((byte_t*)response + sizeof(ClientProtocol::Header));
        response->size = sizeof(buffer);
        response->flags = 0;
        response->message_type = responseType;
        response->request_id = query.header.request_id;
        table->getEntity(*tableBuf);
        client.send(buffer, sizeof(buffer));
      }
      break;
    case ClientProtocol::QueryType::sinceTime:
      return sendErrorResponse(query.header.request_id, ClientProtocol::notImplemented);
    case ClientProtocol::QueryType::sinceId:
      return sendErrorResponse(query.header.request_id, ClientProtocol::notImplemented);
    default:
      return sendErrorResponse(query.header.request_id, ClientProtocol::invalidRequest);
    }
    break;
  default:
    return sendErrorResponse(query.header.request_id, ClientProtocol::notImplemented);
  }
}

void_t ClientHandler::sendErrorResponse(uint32_t requestId, ClientProtocol::Error error)
{
  ClientProtocol::ErrorResponse response;
  response.header.size = sizeof(response);
  response.header.message_type = ClientProtocol::errorResponse;
  response.header.request_id = requestId;
  response.header.flags = 0;
  response.error = error;
  client.send((const byte_t*)&response, sizeof(response));
}

void_t ClientHandler::sendOkResponse(ClientProtocol::MessageType type, uint32_t requestId)
{
  ClientProtocol::Header response;
  response.size = sizeof(response);
  response.message_type = type;
  response.request_id = requestId;
  response.flags = 0;
  sendResponse(response);
}

void_t ClientHandler::sendResponse(ClientProtocol::Header& header)
{
  header.flags = 0;
  client.send((const byte_t*)&header, header.size);
}

void_t ClientHandler::handleWorkerJob(WorkerJob& workerJob)
{
  ClientProtocol::Header* header = (ClientProtocol::Header*)(const byte_t*)workerJob.getResponseData();
  switch(header->message_type)
  {
  case ClientProtocol::loginResponse:
    handleInternalLoginResponse((WorkerProtocol::LoginResponse&)*header);
    break;
  case ClientProtocol::subscribeResponse:
    handleInternalSubscribeResponse(workerJob, (ClientProtocol::Header&)*header);
    break;
  case ClientProtocol::errorResponse:
    handleInternalErrorResponse(workerJob, (ClientProtocol::ErrorResponse&)*header);
    break;
  default:
    if(!client.send((const byte_t*)header, header->size))
      suspend();
    break;
  }
}

void_t ClientHandler::suspend()
{
  client.suspend();
  suspended = true;
}

void_t ClientHandler::resume()
{
  client.resume();
  suspended = false;
  for(HashSet<WorkerJob*>::Iterator i = suspendedWorkerJobs.begin(), end = suspendedWorkerJobs.end(); i != end; ++i)
  {
    WorkerJob* workerJob = *i;
    workerJob->getTable().getWorkerHandler()->continueWorkerJob(*workerJob);
  }
  suspendedWorkerJobs.clear();
}

void_t ClientHandler::addSubscription(Subscription& subscription)
{
  subscriptions.append(&subscription.getTable(), &subscription);
}

void_t ClientHandler::removeSubscription(Subscription& subscription)
{
  subscriptions.remove(&subscription.getTable());
}

void_t ClientHandler::handleInternalLoginResponse(const WorkerProtocol::LoginResponse& loginResponse)
{
  Memory::copy(&signature, &loginResponse.signature, sizeof(signature));

  ClientProtocol::LoginResponse response;
  response.header.flags = 0;
  response.header.size = sizeof(response);
  response.header.message_type = ClientProtocol::loginResponse;
  response.header.request_id = loginResponse.header.request_id;
  Memory::copy(&response.pw_salt, &loginResponse.pw_salt, sizeof(response.pw_salt));
  Memory::copy(&response.auth_salt, &loginResponse.auth_salt, sizeof(response.auth_salt));
  client.send((const byte_t*)&response, sizeof(response));
}

void_t ClientHandler::handleInternalSubscribeResponse(WorkerJob& workerJob, ClientProtocol::Header& subscribeResponse)
{
  client.send((const byte_t*)&subscribeResponse, subscribeResponse.size);
  bool finished = (subscribeResponse.flags & ClientProtocol::HeaderFlag::fragmented) == 0;
  if(finished)
  {
    const ClientProtocol::SubscribeRequest* subscribeRequest = (const ClientProtocol::SubscribeRequest*)(const byte_t*)workerJob.getRequestData();
    Table& table = workerJob.getTable();
    uint64_t lastReplayedEntityId = subscribeRequest->param;
    if(lastReplayedEntityId != table.getLastEntityId())
      subscribeResponse.flags |= ClientProtocol::HeaderFlag::fragmented;
    else
    {
      HashMap<Table*, Subscription*>::Iterator i = subscriptions.find(&table);
      if(i != subscriptions.end())
      {
        Subscription* subscription = *i;
        subscription->setSynced();
      }
    }
  }
}

void_t ClientHandler::handleInternalErrorResponse(WorkerJob& workerJob, const ClientProtocol::ErrorResponse& errorResponse)
{
  const Buffer& request = workerJob.getRequestData();
  const ClientProtocol::Header* requestHeader = (const ClientProtocol::Header*)(const byte_t*)request;
  if(requestHeader->message_type == ClientProtocol::addRequest)
  {
    const ClientProtocol::AddRequest* addRequest = (const ClientProtocol::AddRequest*)requestHeader;
    if(addRequest->table_id == ClientProtocol::tablesTable)
    {
      Table& table = workerJob.getTable();
      table.invalidate();

      // notify subcribers
      {
        ClientProtocol::RemoveRequest removeRequest;
        ClientProtocol::setHeader(removeRequest.header, ClientProtocol::removeRequest, sizeof(removeRequest), 0);
        removeRequest.table_id = ClientProtocol::tablesTable;
        removeRequest.id = table.getId();
        Table* table = serverHandler.findTable(ClientProtocol::tablesTable);
        HashSet<Subscription*>& subscriptions = table->getSubscriptions();
        for(HashSet<Subscription*>::Iterator i = subscriptions.begin(), end = subscriptions.end(); i != end; ++i)
        {
          Subscription* subscription = *i;
          if(subscription->isSynced())
            subscription->getClientHandler()->client.send((const byte_t*)&removeRequest, removeRequest.header.size);
        }
      }
    }
  }
  client.send((const byte_t*)&errorResponse, errorResponse.header.size);
}
