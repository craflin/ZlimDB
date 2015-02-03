
#include <nstd/Directory.h>

#include "Table.h"
#include "ServerHandler.h"
#include "WorkerHandler.h"

bool_t Table::open()
{
  if(!tableFile.open(name))
    return false;
  lastEntityId = tableFile.getLastId();
  return true;
}

bool_t Table::create(const ClientProtocol::Entity* entity)
{
  String dir = File::dirname(name);
  if(dir != ".")
    Directory::create(dir);

  if(!tableFile.create(name))
    return false;
  if(entity && !tableFile.add(*(const TableFile::DataHeader*)entity))
  {
    tableFile.close();
    return false;
  }
  lastEntityId = 0;
  return true;
}

uint32_t Table::getEntitySize() const
{
  return sizeof(ClientProtocol::Table) + name.length();
}

void_t Table::getEntity(ClientProtocol::Table& entity) const
{
  entity.entity.size = sizeof(ClientProtocol::Table) + name.length();
  entity.entity.id = id;
  entity.entity.time = time;
  entity.flags = 0;
  entity.name_size = name.length();
  Memory::copy(&entity + 1, (const char_t*)name, name.length());
}
