
#include <nstd/Directory.h>

#include "Table.h"
#include "ServerHandler.h"
#include "WorkerHandler.h"

Table::~Table()
{
  ASSERT(subscriptions.isEmpty());
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
  return ClientProtocol::copyString(entity.entity, entity.name_size, name, maxSize);
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
