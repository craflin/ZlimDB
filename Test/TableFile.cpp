
#include <lz4.h>

#include <nstd/Debug.h>
#include <nstd/File.h>

#include <Tools/TableFile.h>

void_t testTableFile()
{
  // test create and open
  {
    TableFile file(123);
    ASSERT(file.getTableId() == 123);
    ASSERT(file.create("test.test"));
    ASSERT(file.isOpen());
    ASSERT(file.getLastId() == 0);
    ASSERT(file.getTimeOffset() == 0x7fffffffffffffffLL);
    file.close();
    ASSERT(!file.isOpen());
    ASSERT(file.open("test.test"));
    ASSERT(file.isOpen());
    ASSERT(file.getLastId() == 0);
    ASSERT(file.getTimeOffset() == 0x7fffffffffffffffLL);
    file.close();
    ASSERT(File::unlink("test.test"));
  }

  // test add, read, close and read again
  {
    TableFile file(213);
    ASSERT(file.create("test.test"));
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
    ASSERT(file.getTimeOffset() == 0);
    uint64_t blockId;
    Buffer compressedBlock;
    byte_t decompressedBuffer[0xffff];
    int_t i = 1;
    uint64_t timestamp = 0;
    VERIFY(file.getFirstCompressedBlock(blockId, compressedBlock, 0));
    for(;;)
    {
      size_t rawSize = *(uint16_t*)(const byte_t*)compressedBlock;
      int_t check = LZ4_decompress_safe((const char*)(const byte_t*)compressedBlock + sizeof(uint16_t), (char*)decompressedBuffer, compressedBlock.size() - sizeof(uint16_t), sizeof(decompressedBuffer));
      ASSERT(check == rawSize);
      TableFile::DataHeader* dataHeader = (TableFile::DataHeader*)decompressedBuffer;
      TableFile::DataHeader* dataHeaderEnd = (TableFile::DataHeader*)(decompressedBuffer + rawSize);
      for(; dataHeader < dataHeaderEnd; dataHeader = (TableFile::DataHeader*)((byte_t*)dataHeader + dataHeader->size))
      {
        ASSERT(dataHeader->id == i);
        ASSERT(dataHeader->timestamp > timestamp);
        timestamp = dataHeader->timestamp;
        ++i;
      }
      if(i == 12561)
      {
        int k = 42;
      }
      if(!file.getNextCompressedBlock(blockId, blockId, compressedBlock, 0))
        break;
    }
    ASSERT(i == 100000);
  }

  // todo: test remove bottom to top

  // todo: test remove top to bottom

  // todo: test add and random remove

}
