
#include <nstd/Directory.h>

#include "Table.h"
#include "ServerHandler.h"
#include "WorkerHandler.h"

bool_t Table::open()
{
  return tableFile.open(name);
}

bool_t Table::create(const DataProtocol::Entity* entity)
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
  return true;
}

uint32_t Table::getEntitySize() const
{
  return sizeof(DataProtocol::Table) + name.length();
}

void_t Table::getEntity(DataProtocol::Table& entity) const
{
  entity.size = sizeof(DataProtocol::Table) + name.length();
  entity.id = id;
  entity.time = time;
  entity.flags = 0;
  entity.nameSize = name.length();
  Memory::copy(&entity + 1, (const char_t*)name, name.length());
}
