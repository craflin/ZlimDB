
#include <nstd/Process.h>
#include <nstd/Directory.h>
#include <nstd/File.h>
#include <nstd/Debug.h>

#include <zlimdbclient.h>

void_t testClient(const char_t* argv0)
{
  // start zlimdb server
  String binDir = File::dirname(String(argv0, String::length(argv0)));
  if(!File::isAbsolutePath(binDir))
    binDir = Directory::getCurrent() + "/" + binDir;
  Directory::unlink(binDir + "/TestData", true);
  ASSERT(!File::exists(binDir + "/TestData"));
  Process zlimdbServer;
  ASSERT(zlimdbServer.start(binDir + "/../zlimdb/zlimdb.exe -c " + binDir + "/TestData") != 0);

  // test init
  ASSERT(zlimdb_init() == 0);

  // test create
  zlimdb* zdb = zlimdb_create(0, 0);
  ASSERT(zdb);

  // test connect
  ASSERT(zlimdb_connect(zdb, "127.0.0.1", ZLIMDB_DEFAULT_PORT, "root", "root") == 0);
  
  // test add_table
  {
    uint32_t tableId;
    ASSERT(zlimdb_add_table(zdb, "TestTable1", &tableId) == 0);
    ASSERT(tableId != 0);
  }

  // test sync
  {
    uint32_t tableId;
    ASSERT(zlimdb_add_table(zdb, "TestTable1", &tableId) == 0);
    ASSERT(tableId != 0);
    int64_t serverTime, tableTime;
    ASSERT(zlimdb_sync(zdb, tableId, &serverTime, &tableTime) == 0);
  }

  // test nested queries
  {
    uint32_t tableId;
    ASSERT(zlimdb_add_table(zdb, "TestTable2", &tableId) == 0);
    ASSERT(tableId != 0);

    uint32_t tableId2;
    ASSERT(zlimdb_add_table(zdb, "TestTable3", &tableId2) == 0);
    ASSERT(tableId2 != 0);
    ASSERT(tableId2 != tableId);

    char_t data[577];
    zlimdb_entity* entity = (zlimdb_entity*)data;
    entity->size = sizeof(data);
    entity->id = 0;
    entity->time = 0;
    uint64_t entityId, lastId = 0;
    for(int i = 0; i < 10000; ++i)
    {
      ASSERT(zlimdb_add(zdb, tableId, entity, &entityId) == 0);
      ASSERT(entityId > lastId);
      lastId = entityId;
    }

    for(int i = 0; i < 2; ++i)
      ASSERT(zlimdb_query(zdb, tableId, zlimdb_query_type_all, 0) == 0);

    ASSERT(zlimdb_add(zdb, tableId2, entity, &entityId) == 0);

    for(int i = 0; i < 2; ++i)
    {
      char_t buffer[ZLIMDB_MAX_MESSAGE_SIZE];
      int j = 0;
      uint64_t lastId2 = 0;
      while(zlimdb_get_response(zdb, (zlimdb_header*)buffer, sizeof(buffer)) == 0)
        for(const zlimdb_entity* entity = zlimdb_get_first_entity((zlimdb_header*)buffer, sizeof(zlimdb_entity)); entity; entity = zlimdb_get_next_entity((zlimdb_header*)buffer, sizeof(zlimdb_entity), entity))
        {
          ASSERT(entity->id > lastId2);
          lastId2 = entity->id;
          ++j;
        }
      ASSERT(zlimdb_errno() == 0);
      ASSERT(j == 10000);
    }

    ASSERT(zlimdb_free(zdb) == 0);
    ASSERT(zlimdb_cleanup() == 0);
  }

  // terminate zlimdb server
  ASSERT(zlimdbServer.kill());
  Directory::unlink(binDir + "/TestData", true);
}
