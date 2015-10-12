
#include <lz4.h>

#include <nstd/Debug.h>
#include <nstd/File.h>

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
    ASSERT(buffer.size() == sizeof(uint16_t));
    ASSERT(*(const uint16_t*)(const byte_t*)buffer == 0);
    ASSERT(!file.getCompressedBlock2(blockId, blockId, buffer, 0));
    ASSERT(file.getTimeOffset() == 0x7fffffffffffffffLL);
    file.close();
    ASSERT(!file.isOpen());
    ASSERT(file.open());
    ASSERT(file.isOpen());
    ASSERT(file.getLastId() == 0);
    ASSERT(file.getFirstCompressedBlock2(blockId, buffer, 0));
    ASSERT(buffer.size() == sizeof(uint16_t));
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
    ASSERT(buffer.size() == sizeof(uint16_t));
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
    ASSERT(buffer.size() == sizeof(uint16_t));
    ASSERT(*(const uint16_t*)(const byte_t*)buffer == 0);
    ASSERT(!file.getCompressedBlock2(blockId, blockId, buffer, 0));
    file.close();
    ASSERT(File::unlink("test.test"));
  }

  // todo: test add and random remove
}
