
#include <lz4.h>
#include <zlimdbclient.h>

#include <nstd/Debug.h>
#include <nstd/File.h>
#include <nstd/Math.h>
#include <nstd/HashSet.h>

#include <Tools/TableFile.h>

void_t testTableFile()
{
  // test create and open
  {
    TableFile file(123, "test.test");
    ASSERT(file.getTableId() == 123);
    ASSERT(file.create());
    ASSERT(file.isOpen());
    ASSERT(file.getLastId() == 0);
    uint64_t blockId;
    Buffer buffer;
    ASSERT(file.getFirstCompressedBlock2(blockId, buffer, 0));
    ASSERT(buffer.size() == sizeof(uint16_t) + 1);
    ASSERT(*(const uint16_t*)(const byte_t*)buffer == 0);
    ASSERT(!file.getCompressedBlock2(blockId, blockId, buffer, 0));
    ASSERT(file.getTimeOffset() == 0x7fffffffffffffffLL);
    file.close();
    ASSERT(!file.isOpen());
    ASSERT(file.open());
    ASSERT(file.isOpen());
    ASSERT(file.getLastId() == 0);
    ASSERT(file.getFirstCompressedBlock2(blockId, buffer, 0));
    ASSERT(buffer.size() == sizeof(uint16_t) + 1);
    ASSERT(*(const uint16_t*)(const byte_t*)buffer == 0);
    ASSERT(!file.getCompressedBlock2(blockId, blockId, buffer, 0));
    ASSERT(file.getTimeOffset() == 0x7fffffffffffffffLL);
    file.close();
    ASSERT(File::unlink("test.test"));
  }

  // test add, read, close and read again
  {
    TableFile file(213, "test.test");
    ASSERT(file.create());
    byte_t entityBuffer[51];
    TableFile::DataHeader* dataHeader = (TableFile::DataHeader*)entityBuffer;
    dataHeader->timestamp = 323;
    dataHeader->size = sizeof(entityBuffer);
    {
      for(int_t i = 1; i < 100000; ++i)
      {
        dataHeader->id = i;
        ++dataHeader->timestamp;
        ASSERT(file.add(*dataHeader, 0));
      }
    }
    for(int j = 0; j < 2; ++j)
    {
      if(j > 0)
        file.open();
      uint64_t blockId;
      Buffer compressedBlock;
      byte_t decompressedBuffer[0xffff];
      VERIFY(file.getFirstCompressedBlock2(blockId, compressedBlock, 0));
      int_t i = 1;
      uint64_t timestamp = 0;
      for(;;)
      {
      
        ASSERT(file.getTimeOffset() == 0);
        size_t rawSize = *(uint16_t*)(const byte_t*)compressedBlock;
        int_t check = LZ4_decompress_safe((const char*)(const byte_t*)compressedBlock + sizeof(uint16_t), (char*)decompressedBuffer, compressedBlock.size() - sizeof(uint16_t), sizeof(decompressedBuffer));
        ASSERT(check == (int_t)rawSize);
        TableFile::DataHeader* dataHeader = (TableFile::DataHeader*)decompressedBuffer;
        TableFile::DataHeader* dataHeaderEnd = (TableFile::DataHeader*)(decompressedBuffer + rawSize);
        for(; dataHeader < dataHeaderEnd; dataHeader = (TableFile::DataHeader*)((byte_t*)dataHeader + dataHeader->size))
        {
          ASSERT((int_t)dataHeader->id == i);
          ASSERT(dataHeader->timestamp > timestamp);
          timestamp = dataHeader->timestamp;
          ++i;
        }
        if(!file.getCompressedBlock2(blockId, blockId, compressedBlock, 0))
          break;
      }
      ASSERT(i == 100000);
      file.close();
    }
    ASSERT(File::unlink("test.test"));
  }

  // test remove bottom to top
  {
    TableFile file(213, "test.test");
    ASSERT(file.create());
    byte_t entityBuffer[51];
    TableFile::DataHeader* dataHeader = (TableFile::DataHeader*)entityBuffer;
    dataHeader->timestamp = 323;
    dataHeader->size = sizeof(entityBuffer);
    for(int_t i = 1; i < 100000; ++i)
    {
      dataHeader->id = i;
      ++dataHeader->timestamp;
      ASSERT(file.add(*dataHeader, 0));
    }
    for(int_t i = 100000 - 1; i >= 1; --i)
      ASSERT(file.remove(i));
    uint64_t blockId;
    Buffer buffer;
    ASSERT(file.getFirstCompressedBlock2(blockId, buffer, 0));
    ASSERT(buffer.size() == sizeof(uint16_t) + 1);
    ASSERT(*(const uint16_t*)(const byte_t*)buffer == 0);
    ASSERT(!file.getCompressedBlock2(blockId, blockId, buffer, 0));
    file.close();
    ASSERT(file.open());
    ASSERT(file.getFirstCompressedBlock2(blockId, buffer, 0));
    ASSERT(buffer.size() == sizeof(uint16_t) + 1);
    ASSERT(*(const uint16_t*)(const byte_t*)buffer == 0);
    ASSERT(!file.getCompressedBlock2(blockId, blockId, buffer, 0));
    file.close();
    ASSERT(File::unlink("test.test"));
  }

  // test remove top to bottom
  {
    TableFile file(213, "test.test");
    ASSERT(file.create());
    byte_t entityBuffer[51];
    TableFile::DataHeader* dataHeader = (TableFile::DataHeader*)entityBuffer;
    dataHeader->timestamp = 323;
    dataHeader->size = sizeof(entityBuffer);
    for(int_t i = 1; i < 100000; ++i)
    {
      dataHeader->id = i;
      ++dataHeader->timestamp;
      ASSERT(file.add(*dataHeader, 0));
    }
    for(int_t i = 1; i < 100000; ++i)
      ASSERT(file.remove(i));
    uint64_t blockId;
    Buffer buffer;
    ASSERT(file.getFirstCompressedBlock2(blockId, buffer, 0));
    ASSERT(buffer.size() == sizeof(uint16_t) + 1);
    ASSERT(*(const uint16_t*)(const byte_t*)buffer == 0);
    ASSERT(!file.getCompressedBlock2(blockId, blockId, buffer, 0));
    file.close();
    ASSERT(file.open());
    ASSERT(file.getFirstCompressedBlock2(blockId, buffer, 0));
    ASSERT(buffer.size() == sizeof(uint16_t) + 1);
    ASSERT(*(const uint16_t*)(const byte_t*)buffer == 0);
    ASSERT(!file.getCompressedBlock2(blockId, blockId, buffer, 0));
    file.close();
    ASSERT(File::unlink("test.test"));
  }

  // test add and random remove
  {
    TableFile file(213, "test.test");
    ASSERT(file.create());
    byte_t entityBuffer[51];
    TableFile::DataHeader* dataHeader = (TableFile::DataHeader*)entityBuffer;
    dataHeader->timestamp = 323;
    dataHeader->size = sizeof(entityBuffer);
    uint64_t maxId = 1;
    Math::random(123);
    Buffer buffer;
    HashSet<uint64_t> entities;
    for(int i = 0; i < 1000; ++i)
    {
      size_t count = Math::random() % 1000;
      switch(Math::random() % 4)
      {
      case 0: // add
        for(size_t i = 0; i < count; ++i)
        {
          dataHeader->id = maxId++;
          file.add(*dataHeader, 0);
          entities.append(dataHeader->id);
        }
        break;
      case 1: // update
        if(maxId > 1)
        {
          size_t index = Math::random() % entities.size();
          HashSet<uint64_t>::Iterator it = entities.begin();
          for(size_t i = 0; i < index; ++i)
            ++it;
          uint64_t id = *it;
          for(size_t i = 0; i < count; ++i)
          {
            dataHeader->id = id;
            file.update(*dataHeader);
            ++id;
          }
        }
        break;
      case 2: // remove
        if(maxId > 1)
        {
          size_t index = Math::random() % entities.size();
          HashSet<uint64_t>::Iterator it = entities.begin();
          for(size_t i = 0; i < index; ++i)
            ++it;
          if(count > entities.size() / 2)
            count = entities.size() / 2;
          uint64_t id = *it;
          for(size_t i = 0; i < count; ++i)
          {
            file.remove(id);
            entities.remove(id);
            ++id;
          }
        }
        break;
      case 3: // reopen
        file.close();
        file.open();
        break;
      }

      // check
      uint64_t nextBlockId;
      ASSERT(file.getFirstCompressedBlock2(nextBlockId, buffer, 0));
      byte_t decompBuffer[ZLIMDB_MAX_ENTITY_SIZE];
      HashSet<uint64_t>::Iterator curEntity = entities.begin();
      do
      {
        ASSERT(buffer.size() >= sizeof(uint16_t));
        uint16_t rawSize = *(const uint16_t*)(const byte_t*)buffer;
        ASSERT(rawSize <= ZLIMDB_MAX_ENTITY_SIZE);
        ASSERT(LZ4_decompress_safe((const char*)(const byte_t*)buffer + sizeof(uint16_t), (char*)decompBuffer, buffer.size() - sizeof(uint16_t), ZLIMDB_MAX_ENTITY_SIZE) == rawSize);
        TableFile::DataHeader* entity = (TableFile::DataHeader*)decompBuffer;
        TableFile::DataHeader* entityEnd = (TableFile::DataHeader*)((byte_t*)decompBuffer + rawSize);
        for(;entity < entityEnd; entity = (TableFile::DataHeader*)((byte_t*)entity + entity->size))
        {
          ASSERT(*curEntity == entity->id);
          ++curEntity;
        }
      } while(file.getCompressedBlock2(nextBlockId, nextBlockId, buffer, 0));
    }
    file.close();
    ASSERT(File::unlink("test.test"));
  }
}
