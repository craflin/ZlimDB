
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

void_t ClientHandler::handleMessage(zlimdb_header& header)
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
      handleAdd((zlimdb_add_request&)header);
    else
      sendErrorResponse(header.request_id, zlimdb_error_invalid_message_size);
    break;
  case zlimdb_message_update_request:
    if(header.size >= sizeof(zlimdb_update_request))
      handleUpdate((zlimdb_update_request&)header);
    else
      sendErrorResponse(header.request_id, zlimdb_error_invalid_message_size);
    break;
  case zlimdb_message_remove_request:
    if(header.size >= sizeof(zlimdb_remove_request))
      handleRemove((zlimdb_remove_request&)header);
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
  case zlimdb_message_clear_request:
    if(header.size >= sizeof(zlimdb_clear_request))
      handleClear((zlimdb_clear_request&)header);
    else
      sendErrorResponse(header.request_id, zlimdb_error_invalid_message_size);
    break;
  case zlimdb_message_find_request:
    if(header.size >= sizeof(zlimdb_find_request))
      handleFind((zlimdb_find_request&)header);
    else
      sendErrorResponse(header.request_id, zlimdb_error_invalid_message_size);
    break;
  case zlimdb_message_copy_request:
    if(header.size >= sizeof(zlimdb_copy_request))
      handleCopy((zlimdb_copy_request&)header);
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
  serverHandler.createWorkerJob(*this, *table, &login, sizeof(login), 0);
}

void_t ClientHandler::handleAuth(const zlimdb_auth_request& auth)
{
  bool_t failed = Memory::compare(&auth.signature, &signature, sizeof(signature)) != 0;
  Memory::zero(&signature, sizeof(signature));
  if(failed)
    return sendErrorResponse(auth.header.request_id, zlimdb_error_invalid_login);

  sendOkResponse(zlimdb_message_auth_response, auth.header.request_id);
}

void_t ClientHandler::handleAdd(zlimdb_add_request& add)
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
      serverHandler.createWorkerJob(*this, *table, &add, add.header.size, 0);

      // notify subscribers
      table = serverHandler.findTable(zlimdb_table_tables);
      HashSet<Subscription*>& subscriptions = table->getSubscriptions();
      add.header.request_id = 0;
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
      serverHandler.createWorkerJob(*this, *table, &add, add.header.size, (uint64_t)timeOffset);
    }
    break;
  }
}

void_t ClientHandler::handleUpdate(zlimdb_update_request& update)
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

      serverHandler.createWorkerJob(*this, *table, &update, update.header.size, 0);
    }
    break;
  }
}

void_t ClientHandler::handleRemove(zlimdb_remove_request& remove)
{
  switch(remove.table_id)
  {
  case zlimdb_table_clients:
    return sendErrorResponse(remove.header.request_id, zlimdb_error_invalid_request);
  case zlimdb_table_tables:
    {
      Table* table = serverHandler.findTable((uint32_t)remove.id);
      if(!table)
        return sendErrorResponse(remove.header.request_id, zlimdb_error_entity_not_found);

      // create internal job to delete the file
      table->invalidate();
      serverHandler.createWorkerJob(*this, *table, &remove, remove.header.size, 0);

      // notify subscribers
      table = serverHandler.findTable(zlimdb_table_tables);
      HashSet<Subscription*>& subscriptions = table->getSubscriptions();
      remove.header.request_id = 0;
      for(HashSet<Subscription*>::Iterator i = subscriptions.begin(), end = subscriptions.end(); i != end; ++i)
      {
        Subscription* subscription = *i;
        if(subscription->isSynced())
          subscription->getClientHandler()->client.send((const byte_t*)&remove, remove.header.size);
      }
    }
    break;
  default:
    {
      Table* table = serverHandler.findTable(remove.table_id);
      if(!table)
        return sendErrorResponse(remove.header.request_id, zlimdb_error_table_not_found);

      serverHandler.createWorkerJob(*this, *table, &remove, remove.header.size, 0);
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
      serverHandler.createWorkerJob(*this, *table, &subscribe, sizeof(subscribe), 0);
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
    serverHandler.createWorkerJob(*this, *table, &query, sizeof(query), 0);
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

void_t ClientHandler::handleClear(zlimdb_clear_request& clear)
{
  switch(clear.table_id)
  {
  case zlimdb_table_clients:
  case zlimdb_table_tables:
    return sendErrorResponse(clear.header.request_id, zlimdb_error_invalid_request);
  default:
    {
      Table* table = serverHandler.findTable(clear.table_id);
      if(!table)
        return sendErrorResponse(clear.header.request_id, zlimdb_error_table_not_found);

      serverHandler.createWorkerJob(*this, *table, &clear, clear.header.size, 0);
    }
  }
}

void_t ClientHandler::handleFind(zlimdb_find_request& find)
{
  if(find.header.size < sizeof(find) + sizeof(zlimdb_table_entity))
    return sendErrorResponse(find.header.request_id, zlimdb_error_invalid_message_size);
  else
  {
    // get table name
    String tableName;
    const zlimdb_table_entity* tableEntity = (const zlimdb_table_entity*)(&find + 1);
    if(!ClientProtocol::getString(find.header, tableEntity->entity, sizeof(*tableEntity), tableEntity->name_size, tableName))
      return sendErrorResponse(find.header.request_id, zlimdb_error_invalid_message_data);

    // find table
    Table* table = serverHandler.findTable(tableName);
    if(!table)
      return sendErrorResponse(find.header.request_id, zlimdb_error_table_not_found);

    // send response
    zlimdb_find_response findResponse;
    ClientProtocol::setHeader(findResponse.header, zlimdb_message_find_response, sizeof(findResponse), find.header.request_id);
    findResponse.id = table->getId();
    return sendResponse(findResponse.header);
  }
}

void_t ClientHandler::handleCopy(const zlimdb_copy_request& copy)
{
  switch(copy.table_id)
  {
  case zlimdb_table_clients:
  case zlimdb_table_tables:
    return sendErrorResponse(copy.header.request_id, zlimdb_error_invalid_request);
  default:
    if(copy.header.size < sizeof(copy) + sizeof(zlimdb_table_entity))
      return sendErrorResponse(copy.header.request_id, zlimdb_error_invalid_message_size);
    else
    {
      const zlimdb_table_entity* tableEntity = (const zlimdb_table_entity*)(&copy + 1);

      // find source table
      Table* table = serverHandler.findTable(copy.table_id);
      if(!table)
        return sendErrorResponse(copy.header.request_id, zlimdb_error_table_not_found);

      // check if destination table does already exist
      String newTableName;
      if(!ClientProtocol::getString(copy.header, tableEntity->entity, sizeof(*tableEntity), tableEntity->name_size, newTableName))
        return sendErrorResponse(copy.header.request_id, zlimdb_error_invalid_message_data);
      if(serverHandler.findTable(newTableName))
        return sendErrorResponse(copy.header.request_id, zlimdb_error_table_already_exists);

      // create job to copy the table file
      serverHandler.createWorkerJob(*this, *table, &copy, copy.header.size, 0);
    }
    break;
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
  case zlimdb_message_add_response:
    handleInternalAddResponse(workerJob, (const zlimdb_add_response&)*header);
    break;
  case zlimdb_message_update_response:
    handleInternalUpdateResponse(workerJob, (const zlimdb_header&)*header);
    break;
  case zlimdb_message_remove_response:
    handleInternalRemoveResponse(workerJob, (const zlimdb_header&)*header);
    break;
  case zlimdb_message_clear_response:
    handleInternalClearResponse(workerJob, (const zlimdb_header&)*header);
    break;
  case zlimdb_message_subscribe_response:
    handleInternalSubscribeResponse(workerJob, (zlimdb_header&)*header);
    break;
  case zlimdb_message_copy_response:
    handleInternalCopyResponse(workerJob, (zlimdb_header&)*header);
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

void_t ClientHandler::handleInternalAddResponse(WorkerJob& workerJob, const zlimdb_add_response& addResponse)
{
  // notify subscribers
  zlimdb_add_request* addRequest = (zlimdb_add_request*)(byte_t*)workerJob.getRequestData();
  Table* table = serverHandler.findTable(addRequest->table_id);
  if(table)
  {
      HashSet<Subscription*>& subscriptions = table->getSubscriptions();
      addRequest->header.request_id = 0;
      for(HashSet<Subscription*>::Iterator i = subscriptions.begin(), end = subscriptions.end(); i != end; ++i)
      {
        Subscription* subscription = *i;
        if(subscription->isSynced())
          subscription->getClientHandler()->client.send((const byte_t*)addRequest, addRequest->header.size);
      }
  }

  // send response to client
  client.send((const byte_t*)&addResponse, sizeof(addResponse));
}

void_t ClientHandler::handleInternalUpdateResponse(WorkerJob& workerJob, const zlimdb_header& updateResponse)
{
  // notify subscribers
  zlimdb_update_request* updateRequest = (zlimdb_update_request*)(byte_t*)workerJob.getRequestData();
  const zlimdb_entity* entity = (const zlimdb_entity*)(updateRequest + 1);
  Table* table = serverHandler.findTable(updateRequest->table_id);
  if(table)
  {
      HashSet<Subscription*>& subscriptions = table->getSubscriptions();
      updateRequest->header.request_id = 0;
      for(HashSet<Subscription*>::Iterator i = subscriptions.begin(), end = subscriptions.end(); i != end; ++i)
      {
        Subscription* subscription = *i;
        if(subscription->isSynced() || entity->id <= subscription->getMaxEntityId())
          subscription->getClientHandler()->client.send((const byte_t*)updateRequest, updateRequest->header.size);
      }
  }

  // send response to client
  client.send((const byte_t*)&updateResponse, sizeof(updateResponse));
}

void_t ClientHandler::handleInternalRemoveResponse(WorkerJob& workerJob, const zlimdb_header& removeResponse)
{
  // notify subscribers
  zlimdb_remove_request* removeRequest = (zlimdb_remove_request*)(byte_t*)workerJob.getRequestData();
  Table* table = serverHandler.findTable(removeRequest->table_id);
  if(table)
  {
      HashSet<Subscription*>& subscriptions = table->getSubscriptions();
      removeRequest->header.request_id = 0;
      for(HashSet<Subscription*>::Iterator i = subscriptions.begin(), end = subscriptions.end(); i != end; ++i)
      {
        Subscription* subscription = *i;
        if(subscription->isSynced() || removeRequest->id <= subscription->getMaxEntityId())
          subscription->getClientHandler()->client.send((const byte_t*)removeRequest, removeRequest->header.size);
      }
  }

  // send response to client
  client.send((const byte_t*)&removeResponse, sizeof(removeResponse));
}

void_t ClientHandler::handleInternalClearResponse(WorkerJob& workerJob, const zlimdb_header& clearResponse)
{
  // notify subscribers
  zlimdb_clear_request* clearRequest = (zlimdb_clear_request*)(byte_t*)workerJob.getRequestData();
  Table* table = serverHandler.findTable(clearRequest->table_id);
  if(table)
  {
      HashSet<Subscription*>& subscriptions = table->getSubscriptions();
      clearRequest->header.request_id = 0;
      for(HashSet<Subscription*>::Iterator i = subscriptions.begin(), end = subscriptions.end(); i != end; ++i)
      {
        Subscription* subscription = *i;
        subscription->getClientHandler()->client.send((const byte_t*)clearRequest, clearRequest->header.size);
      }
  }

  // send response to client
  client.send((const byte_t*)&clearResponse, sizeof(clearResponse));
}

void_t ClientHandler::handleInternalSubscribeResponse(WorkerJob& workerJob, zlimdb_header& subscribeResponse)
{
  bool finished = (subscribeResponse.flags & zlimdb_header_flag_fragmented) == 0;
  if(finished)
  {
    const zlimdb_subscribe_request* subscribeRequest = (const zlimdb_subscribe_request*)(const byte_t*)workerJob.getRequestData();
    Table& table = workerJob.getTable();
    HashMap<Table*, Subscription*>::Iterator i = subscriptions.find(&table);
    if(i != subscriptions.end())
    {
      Subscription* subscription = *i;
      uint64_t lastReplayedEntityId = workerJob.getParam1();
      if(lastReplayedEntityId != table.getLastEntityId())
      {
        subscribeResponse.flags |= zlimdb_header_flag_fragmented;
        subscription->setMaxEntityId(lastReplayedEntityId);
      }
      else
        subscription->setSynced();
    }
  }
  client.send((const byte_t*)&subscribeResponse, subscribeResponse.size);
}

void_t ClientHandler::handleInternalCopyResponse(WorkerJob& workerJob, zlimdb_header& copyResponse)
{
  if(workerJob.getParam1() == 0)
  {
    const Buffer& request = workerJob.getRequestData();
    const zlimdb_copy_request* copyRequest = (const zlimdb_copy_request*)(const byte_t*)request;

    // get new table name
    String newTableName;
    {
      const zlimdb_table_entity* tableEntity = (const zlimdb_table_entity*)(copyRequest + 1);
      if(!ClientProtocol::getString(copyRequest->header, tableEntity->entity, sizeof(*tableEntity), tableEntity->name_size, newTableName))
        return sendErrorResponse(copyRequest->header.request_id, zlimdb_error_invalid_message_data);
      if(serverHandler.findTable(newTableName))
        return sendErrorResponse(copyRequest->header.request_id, zlimdb_error_table_already_exists);
    }

    // create table without opening the file
    Table* table = &serverHandler.createTable(newTableName);

    // create internal job to open the copied the file
    serverHandler.createWorkerJob(*this, *table, copyRequest, copyRequest->header.size, 1);

    // notify subscribers
    table = serverHandler.findTable(zlimdb_table_tables);
    HashSet<Subscription*>& subscriptions = table->getSubscriptions();
    Buffer buffer;
    buffer.resize(sizeof(zlimdb_add_request) + sizeof(zlimdb_table_entity) + newTableName.length());
    zlimdb_add_request* addRequest = (zlimdb_add_request*)(byte_t*)buffer;
    ClientProtocol::setHeader(addRequest->header, zlimdb_message_add_request, buffer.size(), 0);
    zlimdb_table_entity* tableEntity = (zlimdb_table_entity*)(addRequest + 1);
    ClientProtocol::setEntityHeader(tableEntity->entity, table->getId(), Time::time(), sizeof(zlimdb_table_entity) + newTableName.length());
    ClientProtocol::setString(tableEntity->entity, tableEntity->name_size, sizeof(zlimdb_table_entity), newTableName);
    for(HashSet<Subscription*>::Iterator i = subscriptions.begin(), end = subscriptions.end(); i != end; ++i)
    {
      Subscription* subscription = *i;
      if(subscription->isSynced())
        subscription->getClientHandler()->client.send((const byte_t*)addRequest, addRequest->header.size);
    }
  }
  else
  {
    client.send((const byte_t*)&copyResponse, copyResponse.size);
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
  else if(requestHeader->message_type == zlimdb_message_copy_request && workerJob.getParam1() != 0)
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
  client.send((const byte_t*)&errorResponse, errorResponse.header.size);
}
