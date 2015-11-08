
#include <nstd/Time.h>
#include <nstd/Math.h>
#include <nstd/Debug.h>

#include "ClientHandler.h"
#include "ServerHandler.h"
#include "WorkerJob.h"
#include "WorkerHandler.h"
#include "WorkerThread.h"
#include "Table.h"
#include "Subscription.h"
#include "ControlJob.h"

ClientHandler::~ClientHandler()
{
  ASSERT(openWorkerJobs.isEmpty());

  HashSet<WorkerJob*> workerJobs;
  workerJobs.swap(suspendedWorkerJobs);
  for(HashSet<WorkerJob*>::Iterator i = workerJobs.begin(), end = workerJobs.end(); i != end; ++i)
    serverHandler.removeWorkerJob(**i);

  HashMap<Table*, Subscription*> subscriptions;
  subscriptions.swap(this->subscriptions);
  for(HashMap<Table*, Subscription*>::Iterator i = subscriptions.begin(), end = subscriptions.end(); i != end; ++i)
    serverHandler.removeSubscription(**i);

  HashSet<ControlJob*> controlJobs;
  controlJobs.swap(openControlJobs);
  for(HashSet<ControlJob*>::Iterator i = controlJobs.begin(), end = controlJobs.end(); i != end; ++i)
    serverHandler.removeControlJob(**i);
}

size_t ClientHandler::handle(byte_t* data, size_t size)
{
  byte_t* pos = data;
  while(size > 0)
  {
    if(size < sizeof(zlimdb_header))
      break;
    zlimdb_header* header = (zlimdb_header*)pos;
    if(header->size < sizeof(zlimdb_header) || header->size > ZLIMDB_MAX_MESSAGE_SIZE)
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
      sendErrorResponse(header.request_id, zlimdb_error_invalid_message_data);
    break;
  case zlimdb_message_auth_request:
    if(header.size >= sizeof(zlimdb_auth_request))
      handleAuth((const zlimdb_auth_request&)header);
    else
      sendErrorResponse(header.request_id, zlimdb_error_invalid_message_data);
    break;
  case zlimdb_message_add_request:
    if(header.size >= sizeof(zlimdb_add_request))
      handleAdd((zlimdb_add_request&)header);
    else
      sendErrorResponse(header.request_id, zlimdb_error_invalid_message_data);
    break;
  case zlimdb_message_update_request:
    if(header.size >= sizeof(zlimdb_update_request))
      handleUpdate((const zlimdb_update_request&)header);
    else
      sendErrorResponse(header.request_id, zlimdb_error_invalid_message_data);
    break;
  case zlimdb_message_remove_request:
    if(header.size >= sizeof(zlimdb_remove_request))
      handleRemove((zlimdb_remove_request&)header);
    else
      sendErrorResponse(header.request_id, zlimdb_error_invalid_message_data);
    break;
  case zlimdb_message_subscribe_request:
    if(header.size >= sizeof(zlimdb_subscribe_request))
      handleSubscribe((const zlimdb_subscribe_request&)header);
    else
      sendErrorResponse(header.request_id, zlimdb_error_invalid_message_data);
    break;
  case zlimdb_message_unsubscribe_request:
    if(header.size >= sizeof(zlimdb_unsubscribe_request))
      handleUnsubscribe((const zlimdb_unsubscribe_request&)header);
    else
      sendErrorResponse(header.request_id, zlimdb_error_invalid_message_data);
    break;
  case zlimdb_message_query_request:
    if(header.size >= sizeof(zlimdb_query_request))
      handleQuery((const zlimdb_query_request&)header);
    else
      sendErrorResponse(header.request_id, zlimdb_error_invalid_message_data);
    break;
  case zlimdb_message_sync_request:
    if(header.size >= sizeof(zlimdb_sync_request))
      handleSync((const zlimdb_sync_request&)header);
    else
      sendErrorResponse(header.request_id, zlimdb_error_invalid_message_data);
    break;
  case zlimdb_message_clear_request:
    if(header.size >= sizeof(zlimdb_clear_request))
      handleClear((const zlimdb_clear_request&)header);
    else
      sendErrorResponse(header.request_id, zlimdb_error_invalid_message_data);
    break;
  case zlimdb_message_find_request:
    if(header.size >= sizeof(zlimdb_find_request))
      handleFind((zlimdb_find_request&)header);
    else
      sendErrorResponse(header.request_id, zlimdb_error_invalid_message_data);
    break;
  case zlimdb_message_copy_request:
    if(header.size >= sizeof(zlimdb_copy_request))
      handleCopy((zlimdb_copy_request&)header);
    else
      sendErrorResponse(header.request_id, zlimdb_error_invalid_message_data);
    break;
  case zlimdb_message_control_request:
    if(header.size >= sizeof(zlimdb_control_request))
      handleControl((zlimdb_control_request&)header);
    else
      sendErrorResponse(header.request_id, zlimdb_error_invalid_message_data);
    break;
  case zlimdb_message_control_response:
    handleControlResponse(header);
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
    {
      // get table name
      String tableName;
      zlimdb_table_entity* tableEntity = (zlimdb_table_entity*)ClientProtocol::getEntity(add.header, sizeof(zlimdb_add_request), sizeof(zlimdb_table_entity));
      if(!tableEntity ||
         !ClientProtocol::getString(tableEntity->entity, sizeof(*tableEntity), tableEntity->name_size, tableName))
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
      tableEntity->entity.id = table->getId();

      // create internal job to create the file
      serverHandler.createWorkerJob(*this, *table, &add, add.header.size, 0);
    }
    break;
  default:
    {
      int64_t now = Time::time();

      // get entity
      zlimdb_entity* entity = ClientProtocol::getEntity(add.header, sizeof(zlimdb_add_request), sizeof(zlimdb_entity));
      if(!entity)
        return sendErrorResponse(add.header.request_id, zlimdb_error_invalid_message_data);

      // find table
      Table* table = serverHandler.findTable(add.table_id);
      if(!table)
        return sendErrorResponse(add.header.request_id, zlimdb_error_table_not_found);

      // create id and timestamp?
      if(entity->id == 0)
        entity->id = table->getLastEntityId() + 1;
      if(entity->time == 0)
        entity->time = now;
      if(entity->id <= table->getLastEntityId() || entity->time < table->getLastEntityTimestamp())
        return sendErrorResponse(add.header.request_id, zlimdb_error_entity_id);
      table->setLastEntityId(entity->id, entity->time);
      int64_t timeOffset = table->updateTimeOffset(now - entity->time);

      // create job to add entity
      serverHandler.createWorkerJob(*this, *table, &add, add.header.size, (uint64_t)timeOffset);
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
      // check entiy data
      const zlimdb_entity* entity = ClientProtocol::getEntity(update.header, sizeof(zlimdb_update_request), sizeof(zlimdb_entity));
      if(!entity)
        return sendErrorResponse(update.header.request_id, zlimdb_error_invalid_message_data);

      // find table
      Table* table = serverHandler.findTable(update.table_id);
      if(!table)
        return sendErrorResponse(update.header.request_id, zlimdb_error_table_not_found);

      // create job to update entity in table file
      serverHandler.createWorkerJob(*this, *table, &update, update.header.size, 0);
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
    {
      Table* table = serverHandler.findTable((uint32_t)remove.id);
      if(!table)
        return sendErrorResponse(remove.header.request_id, zlimdb_error_entity_not_found);

      // create internal job to delete the file
      table->invalidate();
      serverHandler.createWorkerJob(*this, *table, &remove, remove.header.size, 0);
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
  Table* table = serverHandler.findTable(subscribe.query.table_id);
  if(!table)
    return sendErrorResponse(subscribe.query.header.request_id, zlimdb_error_table_not_found);
  if(subscriptions.contains(table))
    return sendOkResponse(zlimdb_message_subscribe_response, subscribe.query.header.request_id);
  if(subscribe.flags & zlimdb_subscribe_flag_responder && table->getResponder())
    return sendErrorResponse(subscribe.query.header.request_id, zlimdb_error_responder_already_present);
  Subscription& subscription = serverHandler.createSubscription(*this, *table);
  if(subscribe.flags & zlimdb_subscribe_flag_responder)
    table->setResponder(*this);
  if(table->getTableFile())
  {
    if(table->getLastEntityId() == 0 || subscribe.query.type == zlimdb_query_type_since_next)
    {
      subscription.setSynced();
      return sendOkResponse(zlimdb_message_subscribe_response, subscribe.query.header.request_id);
    }
    else
      serverHandler.createWorkerJob(*this, *table, &subscribe, sizeof(subscribe), 0);
  }
  else
  {
    handleMetaQuery(subscribe.query, zlimdb_message_subscribe_response);
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
      int64_t now = Time::time();

      Table* table = serverHandler.findTable(sync.table_id);
      if(!table)
        return sendErrorResponse(sync.header.request_id, zlimdb_error_table_not_found);

      zlimdb_sync_response syncResponse;
      ClientProtocol::setHeader(syncResponse.header, zlimdb_message_sync_response, sizeof(syncResponse), sync.header.request_id);
      syncResponse.server_time = now;
      syncResponse.table_time = now - table->getTimeOffset();
      return sendResponse(syncResponse.header);
    }
  }
}

void_t ClientHandler::handleClear(const zlimdb_clear_request& clear)
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

void_t ClientHandler::handleFind(const zlimdb_find_request& find)
{
  // get table name
  String tableName;
  const zlimdb_table_entity* tableEntity = (const zlimdb_table_entity*)ClientProtocol::getEntity(find.header, sizeof(zlimdb_find_request), sizeof(zlimdb_table_entity));
  if(!tableEntity ||
     !ClientProtocol::getString(tableEntity->entity, sizeof(zlimdb_table_entity), tableEntity->name_size, tableName))
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

void_t ClientHandler::handleCopy(const zlimdb_copy_request& copy)
{
  switch(copy.table_id)
  {
  case zlimdb_table_clients:
  case zlimdb_table_tables:
    return sendErrorResponse(copy.header.request_id, zlimdb_error_invalid_request);
  default:
    {
      String newTableName;
      const zlimdb_table_entity* tableEntity = (const zlimdb_table_entity*)ClientProtocol::getEntity(copy.header, sizeof(zlimdb_find_request), sizeof(zlimdb_table_entity));
      if(!tableEntity ||
         !ClientProtocol::getString(tableEntity->entity, sizeof(zlimdb_table_entity), tableEntity->name_size, newTableName))
        return sendErrorResponse(copy.header.request_id, zlimdb_error_invalid_message_data);

      // find source table
      Table* table = serverHandler.findTable(copy.table_id);
      if(!table)
        return sendErrorResponse(copy.header.request_id, zlimdb_error_table_not_found);

      // check if destination table does already exist
      if(serverHandler.findTable(newTableName))
        return sendErrorResponse(copy.header.request_id, zlimdb_error_table_already_exists);

      // create job to copy the table file
      serverHandler.createWorkerJob(*this, *table, &copy, copy.header.size, 0);
    }
    break;
  }
}

void_t ClientHandler::handleControl(zlimdb_control_request& control)
{
  switch(control.table_id)
  {
  case zlimdb_table_clients:
  case zlimdb_table_tables:
    return sendErrorResponse(control.header.request_id, zlimdb_error_invalid_request);
  default:
    {
      // find table
      Table* table = serverHandler.findTable(control.table_id);
      if(!table)
        return sendErrorResponse(control.header.request_id, zlimdb_error_table_not_found);

      // get responder
      ClientHandler* responder = table->getResponder();
      if(!responder)
        return sendErrorResponse(control.header.request_id, zlimdb_error_responder_not_available);

      // create request id
      ControlJob& controlJob = serverHandler.createControlJob(*this, *table, &control, control.header.size);


      // send request to responder
      uint64_t requestId = control.header.request_id;
      control.header.request_id = controlJob.getId();
      responder->client.send((const byte_t*)&control, control.header.size);


    }
    break;
  }
}

void_t ClientHandler::handleControlResponse(const zlimdb_header& response)
{
  // find control job
  ControlJob* controlJob = serverHandler.findControlRequest(response.request_id);
  if(!controlJob)
    return;

  // send response to requester
  controlJob->getClientHandler().client.send((const byte_t*)&response, response.size);

  // notify subscribers
  HashSet<Subscription*>& subscriptions = controlJob->getTable().getSubscriptions();
  zlimdb_control_request* control = (zlimdb_control_request*)(byte_t*)controlJob->getRequestData();
  control->header.request_id = 0;
  for(HashSet<Subscription*>::Iterator i = subscriptions.begin(), end = subscriptions.end(); i != end; ++i)
  {
    Subscription* subscription = *i;
    if(subscription->isSynced())
      subscription->getClientHandler().client.send((const byte_t*)control, control->header.size);
  }

  // remvoe control job
  serverHandler.removeControlJob(*controlJob);
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
        byte_t buffer[ZLIMDB_MAX_MESSAGE_SIZE];
        zlimdb_header* response = (zlimdb_header*)buffer;
        ClientProtocol::setHeader(*response, responseType, sizeof(zlimdb_header), query.header.request_id);
        byte_t* pos = (byte_t*)(response + 1);
        for(HashMap<uint32_t, Table*>::Iterator i = tables.begin(), end = tables.end(); i != end; ++i)
        {
          const Table* table = *i;
          if(!table->getTableFile())
            continue;
          for(;;)
          {
            if(pos - buffer > sizeof(ZLIMDB_MAX_MESSAGE_SIZE) - sizeof(zlimdb_table_entity) ||
              !table->copyEntity(*(zlimdb_table_entity*)pos, sizeof(ZLIMDB_MAX_MESSAGE_SIZE) - (pos - buffer)))
            {
              response->flags = zlimdb_header_flag_fragmented;
              response->size = pos - buffer;
              client.send((const byte_t*)response, response->size);
              pos = (byte_t*)(response + 1);
              continue;
            }
            break;
          }
          pos += ((zlimdb_table_entity*)pos)->entity.size;
        }
        response->flags = 0;
        response->size = pos - buffer;
        client.send((const byte_t*)response, response->size);
      }
      break;
    case zlimdb_query_type_by_id:
      {
        const Table* table = serverHandler.findTable((uint32_t)query.param);
        if(!table)
          return sendErrorResponse(query.header.request_id, zlimdb_error_entity_not_found);
        byte_t buffer[ZLIMDB_MAX_MESSAGE_SIZE];
        zlimdb_header* response = (zlimdb_header*)buffer;
        ClientProtocol::setHeader(*response, responseType, sizeof(zlimdb_header), query.header.request_id);
        zlimdb_table_entity* tableEntity = (zlimdb_table_entity*)(response + 1);
        ASSERT(table->copyEntity(*tableEntity, ZLIMDB_MAX_MESSAGE_SIZE - sizeof(zlimdb_header)));
        response->size += tableEntity->entity.size;
        client.send((const byte_t*)response, response->size);
      }
      break;
    case zlimdb_query_type_since_time:
    case zlimdb_query_type_since_id:
    case zlimdb_query_type_since_last:
    case zlimdb_query_type_since_next:
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
  // send response to client
  client.send((const byte_t*)&addResponse, sizeof(addResponse));

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
          subscription->getClientHandler().client.send((const byte_t*)addRequest, addRequest->header.size);
      }
  }
}

void_t ClientHandler::handleInternalUpdateResponse(WorkerJob& workerJob, const zlimdb_header& updateResponse)
{
  // send response to client
  client.send((const byte_t*)&updateResponse, sizeof(updateResponse));

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
          subscription->getClientHandler().client.send((const byte_t*)updateRequest, updateRequest->header.size);
      }
  }
}

void_t ClientHandler::handleInternalRemoveResponse(WorkerJob& workerJob, const zlimdb_header& removeResponse)
{
  // send response to client
  client.send((const byte_t*)&removeResponse, sizeof(removeResponse));

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
          subscription->getClientHandler().client.send((const byte_t*)removeRequest, removeRequest->header.size);
      }
  }
}

void_t ClientHandler::handleInternalClearResponse(WorkerJob& workerJob, const zlimdb_header& clearResponse)
{
  // send response to client
  client.send((const byte_t*)&clearResponse, sizeof(clearResponse));

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
        subscription->getClientHandler().client.send((const byte_t*)clearRequest, clearRequest->header.size);
      }
  }
}

void_t ClientHandler::handleInternalSubscribeResponse(WorkerJob& workerJob, zlimdb_header& subscribeResponse)
{
  bool finished = (subscribeResponse.flags & zlimdb_header_flag_fragmented) == 0;
  if(finished)
  {
    Table& table = workerJob.getTable();
    HashMap<Table*, Subscription*>::Iterator i = subscriptions.find(&table);
    if(i != subscriptions.end())
    {
      Subscription* subscription = *i;
      uint64_t lastReplayedEntityId = workerJob.getParam1();
      const zlimdb_query_request* request = (const zlimdb_query_request*)(const byte_t*)workerJob.getRequestData();
      if(lastReplayedEntityId != table.getLastEntityId() && request->type != zlimdb_query_type_by_id)
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
      if(!ClientProtocol::getString(tableEntity->entity, sizeof(*tableEntity), tableEntity->name_size, newTableName))
        return sendErrorResponse(copyRequest->header.request_id, zlimdb_error_invalid_message_data);
      if(serverHandler.findTable(newTableName))
        return sendErrorResponse(copyRequest->header.request_id, zlimdb_error_table_already_exists);
    }

    // create table without opening the file
    Table* table = &serverHandler.createTable(newTableName);

    // create internal job to open the copied the file
    serverHandler.createWorkerJob(*this, *table, copyRequest, copyRequest->header.size, 1);
  }
  else
  {
    const Buffer& request = workerJob.getRequestData();
    const zlimdb_copy_request* copyRequest = (const zlimdb_copy_request*)(const byte_t*)request;

    // get new table name
    String newTableName;
    {
      const zlimdb_table_entity* tableEntity = (const zlimdb_table_entity*)(copyRequest + 1);
      if(!ClientProtocol::getString(tableEntity->entity, sizeof(*tableEntity), tableEntity->name_size, newTableName))
        return sendErrorResponse(copyRequest->header.request_id, zlimdb_error_invalid_message_data);
    }

    // send response to client
    client.send((const byte_t*)&copyResponse, copyResponse.size);

    // notify subscribers
    Table* table = serverHandler.findTable(zlimdb_table_tables);
    Table& newTable = workerJob.getTable();
    HashSet<Subscription*>& subscriptions = table->getSubscriptions();
    byte_t buffer[ZLIMDB_MAX_MESSAGE_SIZE];
    zlimdb_add_request* addRequest = (zlimdb_add_request*)buffer;
    ClientProtocol::setHeader(addRequest->header, zlimdb_message_add_request, sizeof(zlimdb_add_request), 0);
    zlimdb_table_entity* tableEntity = (zlimdb_table_entity*)(addRequest + 1);
    ASSERT(newTable.copyEntity(*tableEntity, ZLIMDB_MAX_MESSAGE_SIZE - sizeof(zlimdb_add_request)));
    addRequest->header.size += tableEntity->entity.size;
    for(HashSet<Subscription*>::Iterator i = subscriptions.begin(), end = subscriptions.end(); i != end; ++i)
    {
      Subscription* subscription = *i;
      if(subscription->isSynced())
        subscription->getClientHandler().client.send((const byte_t*)addRequest, addRequest->header.size);
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
    }
  }
  else if(requestHeader->message_type == zlimdb_message_copy_request && workerJob.getParam1() != 0)
  {
    Table& table = workerJob.getTable();
    table.invalidate();
  }
  client.send((const byte_t*)&errorResponse, errorResponse.header.size);
}
