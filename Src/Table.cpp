
#include <nstd/Directory.h>

#include "Table.h"
#include "ServerHandler.h"
#include "WorkerHandler.h"
#include "Subscription.h"
#include "ControlJob.h"
#include "ClientHandler.h"

Table::~Table()
{
  ASSERT(openWorkerJobs.isEmpty());

  HashSet<Subscription*> subscriptions;
  subscriptions.swap(this->subscriptions);
  for(HashSet<Subscription*>::Iterator i = subscriptions.begin(), end = subscriptions.end(); i != end; ++i)
    serverHandler.removeSubscription(**i);

  HashSet<ControlJob*> controlJobs;
  controlJobs.swap(openControlJobs);
  for(HashSet<ControlJob*>::Iterator i = openControlJobs.begin(), end = openControlJobs.end(); i != end; ++i)
  {
    ControlJob* controlJob = *i;
    const zlimdb_control_request* control = (const zlimdb_control_request*)(const byte_t*)controlJob->getRequestData();
    controlJob->getClientHandler().sendErrorResponse(control->header.request_id, zlimdb_error_table_not_found);
    serverHandler.removeControlJob(*controlJob);
  }

  delete tableFile;
}

bool_t Table::open()
{
  if(!tableFile->open())
    return false;
  lastEntityId = tableFile->getLastId();
  lastEntityTimestamp = tableFile->getLastTimestamp();
  if(lastEntityId != 0)
  {
    minTimeOffset = tableFile->getTimeOffset();
    timeOffsets.append(minTimeOffset);
  }
  return true;
}

bool_t Table::create(const zlimdb_entity* entity)
{
  String dir = File::dirname(name);
  if(dir != ".")
    Directory::create(dir);

  if(!tableFile->create())
    return false;
  if(entity && !tableFile->add(*(const TableFile::DataHeader*)entity, 0))
  {
    tableFile->close();
    return false;
  }
  return true;
}

bool_t Table::copyEntity(zlimdb_table_entity& entity, size_t maxSize) const
{
  ClientProtocol::setEntityHeader(entity.entity, id, time, sizeof(zlimdb_table_entity));
  entity.flags = 0;
  return ClientProtocol::copyString(name, entity.entity, entity.name_size, maxSize);
}

void_t Table::removeSubscription(Subscription& subscription)
{
  subscriptions.remove(&subscription);
  if(&subscription.getClientHandler() == responder)
  {
    responder = 0;

    HashSet<ControlJob*> controlJobs;
    controlJobs.swap(openControlJobs);
    for(HashSet<ControlJob*>::Iterator i = openControlJobs.begin(), end = openControlJobs.end(); i != end; ++i)
    {
      ControlJob* controlJob = *i;
      const zlimdb_control_request* control = (const zlimdb_control_request*)(const byte_t*)controlJob->getRequestData();
      controlJob->getClientHandler().sendErrorResponse(control->header.request_id, zlimdb_error_responder_not_available);
      serverHandler.removeControlJob(*controlJob);
    }
  }
}

int64_t Table::updateTimeOffset(int64_t timeOffset)
{
  if(timeOffset < minTimeOffset)
    minTimeOffset = timeOffset;
  timeOffsets.append(timeOffset);
  while(timeOffsets.size() > 100)
  {
    int64_t removedTimeOffset = timeOffsets.front();
    timeOffsets.removeFront();
    if(removedTimeOffset <= minTimeOffset && removedTimeOffset < timeOffset)
    {
      minTimeOffset = timeOffsets.front();
      List<int64_t>::Iterator i = timeOffsets.begin(), end = timeOffsets.end();
      for(++i; i != end; ++i)
        if(*i < minTimeOffset)
          minTimeOffset = *i;
    }
  }
  return minTimeOffset;
}
