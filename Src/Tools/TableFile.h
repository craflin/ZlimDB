
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

  enum Error
  {
    noError,
    fileError,
    dataError,
    notFoundError,
    argumentError,
  };

public:
  TableFile(uint32_t id) : tableId(id), lastError(noError) {}

  uint32_t getTableId() const {return tableId;}
  Error getLastError() const {return lastError;}

  bool_t create(const String& file);
  bool_t open(const String& file);
  bool_t isOpen() const {return file2.isOpen();}
  void_t close() {file2.close();}
  uint64_t getLastId() const {return lastId;}
  timestamp_t getTimeOffset() const {return fileHeader.timeOffset;}
  bool_t add(const DataHeader& data, timestamp_t timeOffset);
  bool_t remove(uint64_t id);
  bool_t get(uint64_t id, Buffer& data, size_t dataOffset);
  bool_t getCompressedBlock(uint64_t id, uint64_t& blockId, Buffer& data, size_t dataOffset);
  bool_t getCompressedBlockByTime(uint64_t timestamp, uint64_t& blockId, Buffer& data, size_t dataOffset);
  bool_t getFirstCompressedBlock(uint64_t& blockId, Buffer& data, size_t dataOffset);
  bool_t hasNextCompressedBlock(uint64_t blockId);
  bool_t getNextCompressedBlock(uint64_t lastBlockId, uint64_t& blockId, Buffer& data, size_t dataOffset);

private:
#pragma pack(push, 1)
  struct FileHeader
  {
    uint32_t magic;
    uint32_t version;
    uint16_t blockSize;
    int64_t timeOffset;
    uint64_t keyPosition;
    uint32_t keySize;
    uint32_t keyCount;
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
  uint32_t tableId;
  File file2;
  FileHeader fileHeader;
  Buffer keys;
  uint64_t fileSize;
  Buffer uncompressedBlock;
  uint64_t lastId;
  uint64_t lastTimestamp;
  Error lastError;

  bool_t fileWrite(const void_t* buffer, size_t size);
  bool_t fileWrite(const Buffer& buffer);
  bool_t fileRead(void_t* buffer, size_t size);
  bool_t fileRead(Buffer& buffer);
  bool_t fileSeek(uint64_t position);

  bool addNewBlock(const DataHeader& data, timestamp_t timeOffset);

  Key* findBlockKey(uint64_t id);
  const Key* findBlockKeyByTime(uint64_t timestamp);
  bool_t getCompressedBlock(const Key* key, Buffer& data, size_t dataOffset);

  bool_t removeEntity(uint64_t entity, Buffer& block);
  bool_t increaseKeyBlockSize(size_t freeSize);
  Key* findFirstCompressedBlock();

  static void_t compressBuffer(const Buffer& buffer, Buffer& compressedBuffer);
  static void_t compressBuffer(const Buffer& buffer, Buffer& compressedBuffer, size_t offset);
  static void_t compressBuffer(const void_t* data, size_t size, Buffer& compressedBuffer);
  bool_t decompressBuffer(const Buffer& compressedBuffer, Buffer& buffer);
};
