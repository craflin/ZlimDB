
#pragma once

#include <nstd/File.h>
#include <nstd/Buffer.h>

class TableFile
{
public:
#pragma pack(push, 1)
  struct DataHeader
  {
    uint64_t id;
    uint64_t timestamp;
    uint16_t size; // including header
  };
#pragma pack(pop)

public:
  bool_t create(const String& file);
  bool_t open(const String& file);
  void_t close();
  bool_t add(const DataHeader& data);
  bool_t remove(uint64_t id);
  bool_t get(uint64_t id, Buffer& data);
  //bool_t getCompressedBlock(uint64_t id, Buffer& data, size_t dataOffset);
  bool_t getFirstCompressedBlock(uint64_t& blockId, Buffer& data, size_t dataOffset);
  bool_t hasNextCompressedBlock(uint64_t blockId);
  bool_t getNextCompressedBlock(uint64_t lastBlockId, uint64_t& blockId, Buffer& data, size_t dataOffset);

private:
#pragma pack(push, 1)
  struct FileHeader
  {
    uint32_t magic;
    uint32_t version;
    uint32_t keyCount;
    uint32_t keySize;
    uint16_t blockSize;
  };

  struct Key
  {
    uint64_t id;
    uint64_t timestamp;
    uint64_t position;
    uint16_t size;
  };
#pragma pack(pop)

private:
  File file;
  FileHeader fileHeader;
  Buffer keys;
  ssize_t uncompressedBlockIndex;
  ssize_t firstCompressedBlockIndex;
  uint64_t fileSize;
  Buffer currentBlock;
  uint64_t lastId;
  uint64_t lastTimestamp;

  bool_t increaseIndicesSize();
  const Key* findBlockKey(uint64_t id);
  bool_t getCompressedBlock(const Key* key, Buffer& data, size_t dataOffset);

};
