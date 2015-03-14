
#include <nstd/Directory.h>

#include "Table.h"
#include "ServerHandler.h"
#include "WorkerHandler.h"

bool_t Table::open()
{
  if(!tableFile->open(name))
    return false;
  lastEntityId = tableFile->getLastId();
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

  if(!tableFile->create(name))
    return false;
  if(entity && !tableFile->add(*(const TableFile::DataHeader*)entity, 0))
  {
    tableFile->close();
    return false;
  }
  return true;
}

uint32_t Table::getEntitySize() const
{
  return sizeof(zlimdb_table_entity) + name.length();
}

void_t Table::getEntity(zlimdb_table_entity& entity) const
{
  entity.entity.size = sizeof(zlimdb_table_entity) + name.length();
  entity.entity.id = id;
  entity.entity.time = time;
  entity.flags = 0;
  entity.name_size = name.length();
  Memory::copy(&entity + 1, (const char_t*)name, name.length());
}

timestamp_t Table::updateTimeOffset(timestamp_t timeOffset)
{
  if(timeOffset < minTimeOffset)
    minTimeOffset = timeOffset;
  timeOffsets.append(timeOffset);
  while(timeOffsets.size() > 100)
  {
    timestamp_t removedTimeOffset = timeOffsets.front();
    timeOffsets.removeFront();
    if(removedTimeOffset <= minTimeOffset && removedTimeOffset < timeOffset)
    {
      minTimeOffset = timeOffsets.front();
      List<timestamp_t>::Iterator i = timeOffsets.begin(), end = timeOffsets.end();
      for(++i; i != end; ++i)
        if(*i < minTimeOffset)
          minTimeOffset = *i;
    }
  }
  return minTimeOffset;
}
