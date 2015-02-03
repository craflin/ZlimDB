
#include <nstd/Time.h>
#include <nstd/Math.h>

#include "ClientHandler.h"
#include "ServerHandler.h"
#include "WorkerJob.h"
#include "WorkerHandler.h"
#include "InternalProtocol.h"
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
    if(size < sizeof(DataProtocol::Header))
      break;
    DataProtocol::Header* header = (DataProtocol::Header*)pos;
    if(header->size < sizeof(DataProtocol::Header) || header->size >= sizeof(DataProtocol::AddRequest) + 0xffff)
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

void_t ClientHandler::handleMessage(DataProtocol::Header& header)
{
  switch((DataProtocol::MessageType)header.message_type)
  {
  case DataProtocol::loginRequest:
    if(header.size >= sizeof(DataProtocol::LoginRequest))
      handleLogin((const DataProtocol::LoginRequest&)header);
    else
      sendErrorResponse(header.request_id, DataProtocol::invalidMessageSize);
    break;
  case DataProtocol::authRequest:
    if(header.size >= sizeof(DataProtocol::AuthRequest))
      handleAuth((const DataProtocol::AuthRequest&)header);
    else
      sendErrorResponse(header.request_id, DataProtocol::invalidMessageSize);
    break;
  case DataProtocol::addRequest:
    if(header.size >= sizeof(DataProtocol::AddRequest))
      handleAdd((DataProtocol::AddRequest&)header);
    else
      sendErrorResponse(header.request_id, DataProtocol::invalidMessageSize);
    break;
  case DataProtocol::updateRequest:
    if(header.size >= sizeof(DataProtocol::UpdateRequest))
      handleUpdate((const DataProtocol::UpdateRequest&)header);
    else
      sendErrorResponse(header.request_id, DataProtocol::invalidMessageSize);
    break;
  case DataProtocol::removeRequest:
    if(header.size >= sizeof(DataProtocol::RemoveRequest))
      handleRemove((const DataProtocol::RemoveRequest&)header);
    else
      sendErrorResponse(header.request_id, DataProtocol::invalidMessageSize);
    break;
  case DataProtocol::subscribeRequest:
    if(header.size >= sizeof(DataProtocol::SubscribeRequest))
      handleSubscribe((const DataProtocol::SubscribeRequest&)header);
    else
      sendErrorResponse(header.request_id, DataProtocol::invalidMessageSize);
    break;
  case DataProtocol::unsubscribeRequest:
    if(header.size >= sizeof(DataProtocol::UnsubscribeRequest))
      handleUnsubscribe((const DataProtocol::UnsubscribeRequest&)header);
    else
      sendErrorResponse(header.request_id, DataProtocol::invalidMessageSize);
    break;
  case DataProtocol::queryRequest:
    if(header.size >= sizeof(DataProtocol::QueryRequest))
      handleQuery((const DataProtocol::QueryRequest&)header);
    else
      sendErrorResponse(header.request_id, DataProtocol::invalidMessageSize);
    break;
  default:
    sendErrorResponse(header.request_id, DataProtocol::invalidMessageType);
    break;
  }
}

void_t ClientHandler::handleLogin(const DataProtocol::LoginRequest& login)
{
  String userName;
  if(!DataProtocol::getString(login.header, sizeof(DataProtocol::LoginRequest), login.user_name_size, userName))
    return sendErrorResponse(login.header.request_id, DataProtocol::invalidData);
  Table* table = serverHandler.findTable(String("users/") + userName + "/.user");
  if(!table)
    return sendErrorResponse(login.header.request_id, DataProtocol::invalidLogin);
  serverHandler.createWorkerJob(*this, *table, &login, sizeof(login));
}

void_t ClientHandler::handleAuth(const DataProtocol::AuthRequest& auth)
{
  bool_t failed = Memory::compare(&auth.signature, &signature, sizeof(signature)) != 0;
  Memory::zero(&signature, sizeof(signature));
  if(failed)
  {
    sendErrorResponse(auth.header.request_id, DataProtocol::Error::invalidLogin);
    return;
  }

  DataProtocol::Header authResponse;
  authResponse.size = sizeof(authResponse);
  authResponse.message_type = DataProtocol::authResponse;
  authResponse.request_id = auth.header.request_id;
  sendResponse(authResponse);
}

void_t ClientHandler::handleAdd(DataProtocol::AddRequest& add)
{
  switch((DataProtocol::TableId)add.table_id)
  {
  case DataProtocol::clientsTable:
  case DataProtocol::timeTable:
    return sendErrorResponse(add.header.request_id, DataProtocol::invalidRequest);
  case DataProtocol::tablesTable:
    if(add.header.size < sizeof(add) + sizeof(DataProtocol::Table))
      return sendErrorResponse(add.header.request_id, DataProtocol::invalidMessageSize);
    else
    {
      // get table name
      String tableName;
      const DataProtocol::Table* tableEntity = (const DataProtocol::Table*)(&add + 1);
      if(!DataProtocol::getString(add.header, tableEntity->entity, sizeof(*tableEntity), tableEntity->name_size, tableName))
        return sendErrorResponse(add.header.request_id, DataProtocol::invalidData);

      // create table without opening the file
      Table* table = serverHandler.findTable(tableName);
      if(table)
        return sendErrorResponse(add.header.request_id, DataProtocol::tableAlreadyExists);
      table = &serverHandler.createTable(tableName);

      // create internal job to create the file
      serverHandler.createWorkerJob(*this, *table, &add, add.header.size);
    }
    break;
  default:
    if(add.header.size < sizeof(add) + sizeof(DataProtocol::Entity))
      return sendErrorResponse(add.header.request_id, DataProtocol::invalidMessageSize);
    else
    {
      // find table
      Table* table = serverHandler.findTable(add.table_id);
      if(!table)
        return sendErrorResponse(add.header.request_id, DataProtocol::tableNotFound);

      // create id and timestamp?
      DataProtocol::Entity* entity = (DataProtocol::Entity*)(&add + 1);
      if(entity->id == 0)
        entity->id = table->getLastEntityId() + 1;
      if(entity->time == 0)
        entity->time = Time::time();
      table->setLastEntityId(entity->id);

      // create job to add entity
      serverHandler.createWorkerJob(*this, *table, &add, add.header.size);

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

void_t ClientHandler::handleUpdate(const DataProtocol::UpdateRequest& update)
{
  switch((DataProtocol::TableId)update.table_id)
  {
  case DataProtocol::clientsTable:
  case DataProtocol::timeTable:
  case DataProtocol::tablesTable:
    return sendErrorResponse(update.header.request_id, DataProtocol::invalidRequest);
  default:
    {
      Table* table = serverHandler.findTable(update.table_id);
      if(!table)
        return sendErrorResponse(update.header.request_id, DataProtocol::tableNotFound);

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

void_t ClientHandler::handleRemove(const DataProtocol::RemoveRequest& remove)
{
  switch((DataProtocol::TableId)remove.table_id)
  {
  case DataProtocol::clientsTable:
  case DataProtocol::timeTable:
    return sendErrorResponse(remove.header.request_id, DataProtocol::invalidRequest);
  case DataProtocol::tablesTable:
    return sendErrorResponse(remove.header.request_id, DataProtocol::notImplemented);
  default:
    {
      Table* table = serverHandler.findTable(remove.table_id);
      if(!table)
        return sendErrorResponse(remove.header.request_id, DataProtocol::tableNotFound);

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

void_t ClientHandler::handleSubscribe(const DataProtocol::SubscribeRequest& subscribe)
{
  Table* table = serverHandler.findTable(subscribe.table_id);
  if(!table)
    return sendErrorResponse(subscribe.header.request_id, DataProtocol::tableNotFound);
  if(subscriptions.contains(table))
    return sendOkResponse(DataProtocol::subscribeResponse, subscribe.header.request_id);
  Subscription& subscription = serverHandler.createSubscription(*this, *table);
  if(table->getLastEntityId() == 0)
    subscription.setSynced();
  else
  {
    serverHandler.createWorkerJob(*this, *table, &subscribe, sizeof(subscribe));
  }
  /*
  wie bekomme ich dies richtig synchronisiert?
    - daten über worker thread requesten
    - wenn bei der reponse die letzte id gleich der neusten id, dann wirklich subscriben
    - woher weis ich, welches die wirklich neuste id ist?
      - die neuste id wird beim öffnen der table datei bestimmt
      - die neuste id ist diejenige für die zuletzt ein add job erstellt wurde
      - damit darf ich keine ids mehr im worker thread erstellen, da ich diese vorweg kennen muss

  was ist, wenn beim requesten der vorhandenen daten etwas manipuliert wird?
    - über die last replayed id kann ich bestimmen, ob relevant oder nicht
      - ne nicht wirklich, da daten in der queue sein können
    - also muss ich alle änderung übertragen, was die gefahr birgt, dass der client damit nichts anfangen kann.. egal?
    */
}

void_t ClientHandler::handleUnsubscribe(const DataProtocol::UnsubscribeRequest& unsubscribe)
{
  Table* table = serverHandler.findTable(unsubscribe.table_id);
  if(!table)
    return sendErrorResponse(unsubscribe.header.request_id, DataProtocol::subscriptionNotFound);
  HashMap<Table*, Subscription*>::Iterator it = subscriptions.find(table);
  if(it == subscriptions.end())
    return sendErrorResponse(unsubscribe.header.request_id, DataProtocol::subscriptionNotFound);
  serverHandler.removeSubscription(**it);
}

void_t ClientHandler::handleQuery(const DataProtocol::QueryRequest& query)
{
  switch((DataProtocol::TableId)query.table_id)
  {
  case DataProtocol::clientsTable:
    return sendErrorResponse(query.header.request_id, DataProtocol::notImplemented);
  case DataProtocol::tablesTable:
    switch(query.type)
    {
    case DataProtocol::QueryType::all:
      {
        const HashMap<uint32_t, Table*>& tables = serverHandler.getTables();
        buffer.resize(4096);
        DataProtocol::Header* response = (DataProtocol::Header*)(byte_t*)buffer;
        response->message_type = DataProtocol::queryResponse;
        response->request_id = query.header.request_id;
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
            response->flags = DataProtocol::HeaderFlag::fragmented;
            client.send(buffer, response->size);
            pos = start;
          }
          table->getEntity(*(DataProtocol::Table*)pos);
          pos += entitySize;
        }
        response->size = pos - start + sizeof(DataProtocol::Header);
        response->flags = 0;
        client.send(buffer, response->size);
      }
      break;
    case DataProtocol::QueryType::byId:
      {
        const Table* table = serverHandler.findTable((uint32_t)query.param);
        if(!table)
          return sendErrorResponse(query.header.request_id, DataProtocol::entityNotFound);
        buffer.resize(sizeof(DataProtocol::Header) + table->getEntitySize());
        DataProtocol::Header* response = (DataProtocol::Header*)(byte_t*)buffer;
        DataProtocol::Table* tableBuf = (DataProtocol::Table*)((byte_t*)response + sizeof(DataProtocol::Header));
        response->size = sizeof(buffer);
        response->flags = 0;
        response->message_type = DataProtocol::queryResponse;
        response->request_id = query.header.request_id;
        table->getEntity(*tableBuf);
        client.send(buffer, sizeof(buffer));
      }
      break;
    case DataProtocol::QueryType::sinceTime:
      return sendErrorResponse(query.header.request_id, DataProtocol::notImplemented);
    case DataProtocol::QueryType::sinceId:
      return sendErrorResponse(query.header.request_id, DataProtocol::notImplemented);
    default:
      return sendErrorResponse(query.header.request_id, DataProtocol::invalidRequest);
    }
    break;
  case DataProtocol::timeTable:
    return sendErrorResponse(query.header.request_id, DataProtocol::notImplemented);
  default:
    {
      Table* table = serverHandler.findTable(query.table_id);
      if(!table)
        return sendErrorResponse(query.header.request_id, DataProtocol::tableNotFound);

      serverHandler.createWorkerJob(*this, *table, &query, sizeof(query));
    }
    break;
  }
}

void_t ClientHandler::sendErrorResponse(uint32_t requestId, DataProtocol::Error error)
{
  DataProtocol::ErrorResponse response;
  response.header.size = sizeof(response);
  response.header.message_type = DataProtocol::errorResponse;
  response.header.request_id = requestId;
  response.error = error;
  client.send((const byte_t*)&response, sizeof(response));
}

void_t ClientHandler::sendOkResponse(DataProtocol::MessageType type, uint32_t requestId)
{
  DataProtocol::Header response;
  response.size = sizeof(response);
  response.message_type = DataProtocol::errorResponse;
  response.request_id = requestId;
  sendResponse(response);
}

void_t ClientHandler::sendResponse(DataProtocol::Header& header)
{
  header.flags = 0;
  client.send((const byte_t*)&header, header.size);
}

void_t ClientHandler::handleWorkerJob(WorkerJob& workerJob)
{
  DataProtocol::Header* header = (DataProtocol::Header*)(const byte_t*)workerJob.getResponseData();
  switch(header->message_type)
  {
  case DataProtocol::loginResponse:
    handleInternalLoginResponse((InternalProtocol::LoginResponse&)*header);
    break;
  case DataProtocol::subscribeResponse:
    handleInternalSubscribeResponse(workerJob, (DataProtocol::Header&)*header);
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

void_t ClientHandler::handleInternalLoginResponse(const InternalProtocol::LoginResponse& loginResponse)
{
  Memory::copy(&signature, &loginResponse.signature, sizeof(signature));

  DataProtocol::LoginResponse response;
  response.header.flags = 0;
  response.header.size = sizeof(response);
  response.header.message_type = DataProtocol::loginResponse;
  response.header.request_id = loginResponse.header.request_id;
  Memory::copy(&response.pw_salt, &loginResponse.pw_salt, sizeof(response.pw_salt));
  Memory::copy(&response.auth_salt, &loginResponse.auth_salt, sizeof(response.auth_salt));
  client.send((const byte_t*)&response, sizeof(response));
}

void_t ClientHandler::handleInternalSubscribeResponse(WorkerJob& workerJob, const DataProtocol::Header& subscribeResponse)
{
  client.send((const byte_t*)&subscribeResponse, subscribeResponse.size);
  bool finished = (subscribeResponse.flags & DataProtocol::HeaderFlag::fragmented) == 0;
  if(finished)
  {
    const DataProtocol::Entity* entity = (const DataProtocol::Entity*)(&subscribeResponse + 1);
    uint64_t maxId = entity->id;
    entity = (const DataProtocol::Entity*)((const byte_t*)entity + entity->size);
    const DataProtocol::Entity* end = (const DataProtocol::Entity*)((const byte_t*)&subscribeResponse + subscribeResponse.size);
    for(; entity < end; entity = (const DataProtocol::Entity*)((const byte_t*)entity + entity->size))
      maxId = entity->id;
    Table& table = workerJob.getTable();
    if(maxId != table.getLastEntityId())
    {
      Buffer& request = workerJob.getRequestData();
      serverHandler.createWorkerJob(*this, table, (byte_t*)request, request.size());
    }
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
