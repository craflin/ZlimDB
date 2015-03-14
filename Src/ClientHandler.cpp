
#include <nstd/Time.h>
#include <nstd/Math.h>

#include "ClientHandler.h"
#include "ServerHandler.h"
#include "WorkerJob.h"
#include "WorkerHandler.h"
#include "WorkerThread.h"
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
    if(size < sizeof(zlimdb_header))
      break;
    zlimdb_header* header = (zlimdb_header*)pos;
    if(header->size < sizeof(zlimdb_header) || header->size >= sizeof(zlimdb_add_request) + 0xffff)
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

void_t ClientHandler::handleMessage(const zlimdb_header& header)
{
  switch(header.message_type)
  {
  case zlimdb_message_login_request:
    if(header.size >= sizeof(zlimdb_login_request))
      handleLogin((const zlimdb_login_request&)header);
    else
      sendErrorResponse(header.request_id, zlimdb_error_invalid_message_size);
    break;
  case zlimdb_message_auth_request:
    if(header.size >= sizeof(zlimdb_auth_request))
      handleAuth((const zlimdb_auth_request&)header);
    else
      sendErrorResponse(header.request_id, zlimdb_error_invalid_message_size);
    break;
  case zlimdb_message_add_request:
    if(header.size >= sizeof(zlimdb_add_request))
      handleAdd((const zlimdb_add_request&)header);
    else
      sendErrorResponse(header.request_id, zlimdb_error_invalid_message_size);
    break;
  case zlimdb_message_update_request:
    if(header.size >= sizeof(zlimdb_update_request))
      handleUpdate((const zlimdb_update_request&)header);
    else
      sendErrorResponse(header.request_id, zlimdb_error_invalid_message_size);
    break;
  case zlimdb_message_remove_request:
    if(header.size >= sizeof(zlimdb_remove_request))
      handleRemove((const zlimdb_remove_request&)header);
    else
      sendErrorResponse(header.request_id, zlimdb_error_invalid_message_size);
    break;
  case zlimdb_message_subscribe_request:
    if(header.size >= sizeof(zlimdb_subscribe_request))
      handleSubscribe((const zlimdb_subscribe_request&)header);
    else
      sendErrorResponse(header.request_id, zlimdb_error_invalid_message_size);
    break;
  case zlimdb_message_unsubscribe_request:
    if(header.size >= sizeof(zlimdb_unsubscribe_request))
      handleUnsubscribe((const zlimdb_unsubscribe_request&)header);
    else
      sendErrorResponse(header.request_id, zlimdb_error_invalid_message_size);
    break;
  case zlimdb_message_query_request:
    if(header.size >= sizeof(zlimdb_query_request))
      handleQuery((const zlimdb_query_request&)header);
    else
      sendErrorResponse(header.request_id, zlimdb_error_invalid_message_size);
    break;
  case zlimdb_message_sync_request:
    if(header.size >= sizeof(zlimdb_sync_request))
      handleSync((const zlimdb_sync_request&)header);
    else
      sendErrorResponse(header.request_id, zlimdb_error_invalid_message_size);
    break;
  default:
    sendErrorResponse(header.request_id, zlimdb_error_invalid_message_type);
    break;
  }
}

void_t ClientHandler::handleLogin(const zlimdb_login_request& login)
{
  String userName;
  if(!ClientProtocol::getString(login.header, sizeof(zlimdb_login_request), login.user_name_size, userName))
    return sendErrorResponse(login.header.request_id, zlimdb_error_invalid_message_data);
  Table* table = serverHandler.findTable(String("users/") + userName + "/user");
  if(!table)
    return sendErrorResponse(login.header.request_id, zlimdb_error_invalid_login);
  serverHandler.createWorkerJob(*this, *table, &login, sizeof(login));
}

void_t ClientHandler::handleAuth(const zlimdb_auth_request& auth)
{
  bool_t failed = Memory::compare(&auth.signature, &signature, sizeof(signature)) != 0;
  Memory::zero(&signature, sizeof(signature));
  if(failed)
    return sendErrorResponse(auth.header.request_id, zlimdb_error_invalid_login);

  sendOkResponse(zlimdb_message_auth_response, auth.header.request_id);
}

void_t ClientHandler::handleAdd(const zlimdb_add_request& add)
{
  switch(add.table_id)
  {
  case zlimdb_table_clients:
    return sendErrorResponse(add.header.request_id, zlimdb_error_invalid_request);
  case zlimdb_table_tables:
    if(add.header.size < sizeof(add) + sizeof(zlimdb_table_entity))
      return sendErrorResponse(add.header.request_id, zlimdb_error_invalid_message_size);
    else
    {
      // get table name
      String tableName;
      const zlimdb_table_entity* tableEntity = (const zlimdb_table_entity*)(&add + 1);
      if(!ClientProtocol::getString(add.header, tableEntity->entity, sizeof(*tableEntity), tableEntity->name_size, tableName))
        return sendErrorResponse(add.header.request_id, zlimdb_error_invalid_message_data);

      // create table without opening the file
      Table* table = serverHandler.findTable(tableName);
      if(table)
      {
        zlimdb_add_response addResponse;
        ClientProtocol::setHeader(addResponse.header, zlimdb_message_add_response, sizeof(addResponse), add.header.request_id);
        addResponse.id = table->getId();
        return sendResponse(addResponse.header);
      }
      table = &serverHandler.createTable(tableName);

      // create internal job to create the file
      serverHandler.createWorkerJob(*this, *table, &add, add.header.size);

      // notify subscribers
      table = serverHandler.findTable(zlimdb_table_tables);
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
    if(add.header.size < sizeof(add) + sizeof(zlimdb_entity))
      return sendErrorResponse(add.header.request_id, zlimdb_error_invalid_message_size);
    else
    {
      timestamp_t now = Time::time();

      // find table
      Table* table = serverHandler.findTable(add.table_id);
      if(!table)
        return sendErrorResponse(add.header.request_id, zlimdb_error_table_not_found);

      // create id and timestamp?
      zlimdb_entity* entity = (zlimdb_entity*)(&add + 1);
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

void_t ClientHandler::handleUpdate(const zlimdb_update_request& update)
{
  switch(update.table_id)
  {
  case zlimdb_table_clients:
  case zlimdb_table_tables:
    return sendErrorResponse(update.header.request_id, zlimdb_error_invalid_request);
  default:
    {
      Table* table = serverHandler.findTable(update.table_id);
      if(!table)
        return sendErrorResponse(update.header.request_id, zlimdb_error_table_not_found);

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

void_t ClientHandler::handleRemove(const zlimdb_remove_request& remove)
{
  switch(remove.table_id)
  {
  case zlimdb_table_clients:
    return sendErrorResponse(remove.header.request_id, zlimdb_error_invalid_request);
  case zlimdb_table_tables:
    return sendErrorResponse(remove.header.request_id, zlimdb_error_not_implemented);
  default:
    {
      Table* table = serverHandler.findTable(remove.table_id);
      if(!table)
        return sendErrorResponse(remove.header.request_id, zlimdb_error_table_not_found);

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

void_t ClientHandler::handleSubscribe(const zlimdb_subscribe_request& subscribe)
{
  Table* table = serverHandler.findTable(subscribe.table_id);
  if(!table)
    return sendErrorResponse(subscribe.header.request_id, zlimdb_error_table_not_found);
  if(subscriptions.contains(table))
    return sendOkResponse(zlimdb_message_subscribe_response, subscribe.header.request_id);
  Subscription& subscription = serverHandler.createSubscription(*this, *table);
  if(table->getTableFile())
  {
    if(table->getLastEntityId() == 0)
    {
      subscription.setSynced();
      return sendOkResponse(zlimdb_message_subscribe_response, subscribe.header.request_id);
    }
    else
      serverHandler.createWorkerJob(*this, *table, &subscribe, sizeof(subscribe));
  }
  else
  {
    handleMetaQuery(subscribe, zlimdb_message_subscribe_response);
    subscription.setSynced();
  }
}

void_t ClientHandler::handleUnsubscribe(const zlimdb_unsubscribe_request& unsubscribe)
{
  Table* table = serverHandler.findTable(unsubscribe.table_id);
  if(!table)
    return sendErrorResponse(unsubscribe.header.request_id, zlimdb_error_subscription_not_found);
  HashMap<Table*, Subscription*>::Iterator it = subscriptions.find(table);
  if(it == subscriptions.end())
    return sendErrorResponse(unsubscribe.header.request_id, zlimdb_error_subscription_not_found);
  serverHandler.removeSubscription(**it);
  sendOkResponse(zlimdb_message_unsubscribe_response, unsubscribe.header.request_id);
}

void_t ClientHandler::handleQuery(const zlimdb_query_request& query)
{
  Table* table = serverHandler.findTable(query.table_id);
  if(!table)
    return sendErrorResponse(query.header.request_id, zlimdb_error_table_not_found);
  if(table->getTableFile())
    serverHandler.createWorkerJob(*this, *table, &query, sizeof(query));
  else
    handleMetaQuery(query, zlimdb_message_query_response);
}

void_t ClientHandler::handleSync(const zlimdb_sync_request& sync)
{
  switch(sync.table_id)
  {
  case zlimdb_table_clients:
  case zlimdb_table_tables:
    {
      zlimdb_sync_response syncResponse;
      ClientProtocol::setHeader(syncResponse.header, zlimdb_message_sync_response, sizeof(syncResponse), sync.header.request_id);
      syncResponse.table_time = syncResponse.server_time = Time::time();
      return sendResponse(syncResponse.header);
    }
  default:
    {
      timestamp_t now = Time::time();

      Table* table = serverHandler.findTable(sync.table_id);
      if(!table)
        return sendErrorResponse(sync.header.request_id, zlimdb_error_table_not_found);

      zlimdb_sync_response syncResponse;
      ClientProtocol::setHeader(syncResponse.header, zlimdb_message_sync_response, sizeof(syncResponse), sync.header.request_id);
      syncResponse.table_time = now;
      syncResponse.server_time = now + table->getTimeOffset();
      return sendResponse(syncResponse.header);
    }
  }
}

void_t ClientHandler::handleMetaQuery(const zlimdb_query_request& query, zlimdb_message_type responseType)
{
  switch(query.table_id)
  {
  case zlimdb_table_tables:
    switch(query.type)
    {
    case zlimdb_query_type_all:
      {
        const HashMap<uint32_t, Table*>& tables = serverHandler.getTables();
        buffer.resize(4096);
        zlimdb_header* response = (zlimdb_header*)(byte_t*)buffer;
        response->message_type = responseType;
        response->request_id = query.header.request_id;
        byte_t* start;
        byte_t* pos = start = (byte_t*)response + sizeof(zlimdb_header);
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
            response->flags = zlimdb_header_flag_fragmented;
            client.send(buffer, response->size);
            pos = start;
          }
          table->getEntity(*(zlimdb_table_entity*)pos);
          pos += entitySize;
        }
        response->size = pos - start + sizeof(zlimdb_header);
        response->flags = 0;
        client.send(buffer, response->size);
      }
      break;
    case zlimdb_query_type_by_id:
      {
        const Table* table = serverHandler.findTable((uint32_t)query.param);
        if(!table)
          return sendErrorResponse(query.header.request_id, zlimdb_error_entity_not_found);
        buffer.resize(sizeof(zlimdb_header) + table->getEntitySize());
        zlimdb_header* response = (zlimdb_header*)(byte_t*)buffer;
        zlimdb_table_entity* tableBuf = (zlimdb_table_entity*)((byte_t*)response + sizeof(zlimdb_header));
        ClientProtocol::setHeader(*response, responseType, buffer.size(), query.header.request_id);
        table->getEntity(*tableBuf);
        client.send(buffer, sizeof(buffer));
      }
      break;
    case zlimdb_query_type_since_time:
      return sendErrorResponse(query.header.request_id, zlimdb_error_not_implemented);
    case zlimdb_query_type_since_id:
      return sendErrorResponse(query.header.request_id, zlimdb_error_not_implemented);
    default:
      return sendErrorResponse(query.header.request_id, zlimdb_error_invalid_request);
    }
    break;
  default:
    return sendErrorResponse(query.header.request_id, zlimdb_error_not_implemented);
  }
}

void_t ClientHandler::sendErrorResponse(uint32_t requestId, zlimdb_message_error error)
{
  zlimdb_error_response response;
  ClientProtocol::setHeader(response.header, zlimdb_message_error_response, sizeof(response), requestId);
  response.error = error;
  client.send((const byte_t*)&response, sizeof(response));
}

void_t ClientHandler::sendOkResponse(zlimdb_message_type type, uint32_t requestId)
{
  zlimdb_header response;
  ClientProtocol::setHeader(response, type, sizeof(response), requestId);
  sendResponse(response);
}

void_t ClientHandler::sendResponse(zlimdb_header& header)
{
  client.send((const byte_t*)&header, header.size);
}

void_t ClientHandler::handleWorkerJob(WorkerJob& workerJob)
{
  zlimdb_header* header = (zlimdb_header*)(byte_t*)workerJob.getResponseData();
  switch(header->message_type)
  {
  case zlimdb_message_login_response:
    handleInternalLoginResponse((const zlimdb_login_response&)*header);
    break;
  case zlimdb_message_subscribe_response:
    handleInternalSubscribeResponse(workerJob, (zlimdb_header&)*header);
    break;
  case zlimdb_message_error_response:
    handleInternalErrorResponse(workerJob, (const zlimdb_error_response&)*header);
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

void_t ClientHandler::handleInternalLoginResponse(const zlimdb_login_response& loginResponse)
{
  ASSERT(loginResponse.header.size == sizeof(WorkerThread::LoginResponse));
  Memory::copy(&signature, ((WorkerThread::LoginResponse&)loginResponse).signature, sizeof(signature));

  zlimdb_login_response response;
  ClientProtocol::setHeader(response.header, zlimdb_message_login_response, sizeof(response), loginResponse.header.request_id);
  Memory::copy(&response.pw_salt, &loginResponse.pw_salt, sizeof(response.pw_salt));
  Memory::copy(&response.auth_salt, &loginResponse.auth_salt, sizeof(response.auth_salt));
  client.send((const byte_t*)&response, sizeof(response));
}

void_t ClientHandler::handleInternalSubscribeResponse(WorkerJob& workerJob, zlimdb_header& subscribeResponse)
{
  client.send((const byte_t*)&subscribeResponse, subscribeResponse.size);
  bool finished = (subscribeResponse.flags & zlimdb_header_flag_fragmented) == 0;
  if(finished)
  {
    const zlimdb_subscribe_request* subscribeRequest = (const zlimdb_subscribe_request*)(const byte_t*)workerJob.getRequestData();
    Table& table = workerJob.getTable();
    uint64_t lastReplayedEntityId = subscribeRequest->param;
    if(lastReplayedEntityId != table.getLastEntityId())
      subscribeResponse.flags |= zlimdb_header_flag_fragmented;
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

void_t ClientHandler::handleInternalErrorResponse(WorkerJob& workerJob, const zlimdb_error_response& errorResponse)
{
  const Buffer& request = workerJob.getRequestData();
  const zlimdb_header* requestHeader = (const zlimdb_header*)(const byte_t*)request;
  if(requestHeader->message_type == zlimdb_message_add_request)
  {
    const zlimdb_add_request* addRequest = (const zlimdb_add_request*)requestHeader;
    if(addRequest->table_id == zlimdb_table_tables)
    {
      Table& table = workerJob.getTable();
      table.invalidate();

      // notify subcribers
      {
        zlimdb_remove_request removeRequest;
        ClientProtocol::setHeader(removeRequest.header, zlimdb_message_remove_request, sizeof(removeRequest), 0);
        removeRequest.table_id = zlimdb_table_tables;
        removeRequest.id = table.getId();
        Table* table = serverHandler.findTable(zlimdb_table_tables);
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
