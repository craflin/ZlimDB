
#include <lz4.h>

#include <nstd/Debug.h>
#include <nstd/Error.h>

#include "TableFile.h"

#define DEFAULT_BLOCK_SIZE 4096
#define DEFAULT_KEY_SIZE (DEFAULT_BLOCK_SIZE / sizeof(Key) * sizeof(Key))

bool_t TableFile::create()
{
  if(!file2.open(fileName, File::readFlag | File::writeFlag))
    return lastError = fileError, false;

  char buffer[sizeof(FileHeader) + DEFAULT_KEY_SIZE + DEFAULT_BLOCK_SIZE];
  fileHeader.magic = *(uint32_t*)"ZLIM";
  fileHeader.version = 2;
  fileHeader.blockSize = DEFAULT_BLOCK_SIZE;
  fileHeader.timeOffset = 0x7fffffffffffffffLL;
  fileHeader.keyPosition = sizeof(FileHeader);
  fileHeader.keyCount = 0;
  fileHeader.keySize = DEFAULT_KEY_SIZE;
  FileHeader* fileHeaderBuf = (FileHeader*)buffer;
  *fileHeaderBuf = fileHeader;
  Memory::zero(buffer + sizeof(FileHeader), DEFAULT_KEY_SIZE + DEFAULT_BLOCK_SIZE);
  if(!fileWrite(&buffer, sizeof(buffer)))
    return file2.close(), false;

  keys.clear();
  uncompressedBlock.clear();
  fileSize = sizeof(buffer);
  lastId = 0;
  lastTimestamp = 0;
  return lastError = noError, true;
}

bool_t TableFile::open()
{
  if(!file2.open(fileName, File::readFlag | File::writeFlag | File::openFlag))
    return lastError = fileError, false;

  // read header
  if(!fileRead(&fileHeader, sizeof(fileHeader)))
    return file2.close(), false;
  if(fileHeader.magic != *(uint32_t*)"ZLIM" ||
     fileHeader.version != 2)
    return file2.close(), lastError = dataError, false;

  // read keys
  size_t usedKeysSize = fileHeader.keyCount * sizeof(Key);
  if(usedKeysSize > fileHeader.keySize)
    return file2.close(), lastError = dataError, false;
  keys.resize(usedKeysSize);
  if(!fileSeek(fileHeader.keyPosition))
    return file2.close(), false;
  if(!fileRead(keys))
    return file2.close(), false;

  // read uncompressed block
  uncompressedBlock.clear();
  if(fileHeader.keyCount)
  {
    const Key* lastKey = (const Key*)(const byte_t*)keys + (fileHeader.keyCount - 1);
    if(lastKey->position == fileHeader.keyPosition + fileHeader.keySize)
    { // last key is uncompressed
      uncompressedBlock.resize(lastKey->size);
      if(!fileSeek(lastKey->position))
        return file2.close(), false;
      if(!fileRead(uncompressedBlock))
        return file2.close(), false;
    }
  }

  // find file size
  if(fileHeader.keyCount)
  {
    const Key* maxKey = (const Key*)(const byte_t*)keys;
    uint64_t maxPosition = maxKey->position;
    for(const Key* i = maxKey + 1, * end = maxKey + fileHeader.keyCount; i < end; ++i)
      if(i->position > maxPosition)
      {
        maxPosition = i->position;
        maxKey = i;
      }
    fileSize = maxPosition + maxKey->size;
  }
  else
    fileSize = fileHeader.keyPosition + fileHeader.keySize + fileHeader.blockSize;

  // find lastId and lastTimestamp
  lastId = 0;
  lastTimestamp = 0;
  if(fileHeader.keyCount)
  {
    Buffer decompressedBuffer;
    const DataHeader* buffer;
    size_t bufferSize;
    const Key* lastKey = (const Key*)(const byte_t*)keys + (fileHeader.keyCount - 1);
    if(lastKey->position == fileHeader.keyPosition + fileHeader.keySize)
    { // last key is uncompressed
      buffer = (const DataHeader*)(const byte_t*)uncompressedBlock;
      bufferSize = uncompressedBlock.size();
    }
    else
    { // last key is compressed
      Buffer compressedBuffer;
      compressedBuffer.resize(lastKey->size);
      if(!fileSeek(lastKey->position))
        return file2.close(), false;
      if(!fileRead(compressedBuffer))
        return file2.close(), false;
      if(!decompressBuffer(compressedBuffer, decompressedBuffer))
        return file2.close(), false;
      buffer = (const DataHeader*)(const byte_t*)decompressedBuffer;
      bufferSize = decompressedBuffer.size();
    }
    while(bufferSize >= sizeof(DataHeader))
    {
      lastId = buffer->id;
      lastTimestamp = buffer->timestamp;
      bufferSize -= buffer->size;
      buffer = (const DataHeader*)((const byte_t*)buffer + buffer->size);
    }
  }

  return lastError = noError, true;
}

bool_t TableFile::copy(const String& dest)
{
  File destFile;
  if(!destFile.open(dest, File::writeFlag))
    return false;
  if(file2.seek(0) != 0)
    return false;
  Buffer buffer;
  buffer.resize(4 * 1024 * 1024);
  for(;;)
  {
    ssize_t bytes = file2.read(buffer, 4 * 1024 * 1024);
    switch(bytes)
    {
    case 0:
      goto done;
    case -1:
      return false;
    default:
      if(destFile.write(buffer, bytes) != bytes)
        return false;
    }
  }
done:
  return true;
}

bool_t TableFile::get(uint64_t id, Buffer& result, size_t dataOffset)
{
  const Key* key = findBlockKey(id);
  if(!key)
    return false;

  size_t bufferSize;
  const DataHeader* buffer;
  Buffer decompressedBuffer;
  if(key->position == fileHeader.keyPosition + fileHeader.keySize)
  {
    bufferSize = uncompressedBlock.size();
    buffer = (const DataHeader*)(const byte_t*)uncompressedBlock;
  }
  else
  {
    // seek to position
    if(!fileSeek(key->position))
      return false;

    // read block
    Buffer compressedBuffer;
    compressedBuffer.resize(key->size);
    if(!fileRead(compressedBuffer))
      return false;

    // decompress block
    if(!decompressBuffer(compressedBuffer, decompressedBuffer))
      return false;
    buffer = (const DataHeader*)(const byte_t*)decompressedBuffer;
    bufferSize = decompressedBuffer.size();
  }

  // find id
  while(bufferSize >= sizeof(DataHeader))
  {
    if(buffer->id == id)
      goto found;
    bufferSize -= buffer->size;
    buffer = (const DataHeader*)((const byte_t*)buffer + buffer->size);
  }
  return lastError = notFoundError, false;
found:;
  if(bufferSize < buffer->size)
    return lastError = dataError, false;

  // return data
  result.resize(dataOffset + buffer->size);
  Memory::copy((byte_t*)result + dataOffset, (const byte_t*)buffer, buffer->size);
  return lastError = noError, true;
}

bool_t TableFile::getCompressedBlock2(uint64_t id, uint64_t& nextBlockId, Buffer& data, size_t dataOffset)
{
  const Key* key = findBlockKey(id);
  if(!key)
    return false;
  const Key* keyEnd = (const Key*)(const byte_t*)keys + keys.size() / sizeof(Key);

  if(key->id == id)
  {
    if(!getCompressedBlock(key, data, dataOffset))
      return false;
    nextBlockId = ++key < keyEnd ? key->id : 0;
    return lastError = noError, true;
  }

  Buffer compressedBlock;
  if(!getCompressedBlock(key, compressedBlock, 0))
    return false;
  Buffer decompressedBlock;
  if(!decompressBuffer(compressedBlock, decompressedBlock))
    return false;

  DataHeader* dataHeader = (DataHeader*)(byte_t*)decompressedBlock;
  size_t remainingDataSize = decompressedBlock.size();
  while(remainingDataSize >= sizeof(DataHeader))
  {
    if(dataHeader->id == id)
      goto found;
    remainingDataSize -= dataHeader->size;
    dataHeader = (DataHeader*)((byte_t*)dataHeader + dataHeader->size);
  }

  decompressedBlock.clear();
  compressBuffer(decompressedBlock, data, dataOffset);
  nextBlockId = ++key < keyEnd ? key->id : 0;
  return lastError = noError, true;
found:;

  compressBuffer(dataHeader, remainingDataSize, data, dataOffset);
  nextBlockId = ++key < keyEnd ? key->id : 0;
  return lastError = noError, true;
}

bool_t TableFile::getCompressedBlockByTime2(uint64_t timestamp, uint64_t& nextBlockId, Buffer& data, size_t dataOffset)
{
  const Key* key = findBlockKeyByTime(timestamp);
  if(!key)
    return getFirstCompressedBlock2(nextBlockId, data, dataOffset);
  if(!getCompressedBlock(key, data, dataOffset))
    return false;
  const Key* keyEnd = (const Key*)(const byte_t*)keys + keys.size() / sizeof(Key);
  nextBlockId = ++key < keyEnd ? key->id : 0;
  return lastError = noError, true;
}

bool_t TableFile::getFirstCompressedBlock2(uint64_t& nextBlockId, Buffer& data, size_t dataOffset)
{
  if(keys.isEmpty())
  {
    nextBlockId = 0;
    data.resize(dataOffset + sizeof(uint16_t));
    *(uint16_t*)((byte_t*)data + dataOffset) = 0;
    return lastError = noError, true;
  }
  const Key* key = (const Key*)(const byte_t*)keys;
  if(!getCompressedBlock(key, data, dataOffset))
    return false;
  const Key* keyEnd = (const Key*)(const byte_t*)keys + keys.size() / sizeof(Key);
  nextBlockId = ++key < keyEnd ? key->id : 0;
  return lastError = noError, true;
}

void_t TableFile::getEmptyCompressedBlock2(uint64_t& nextBlockId, Buffer& data, size_t dataOffset)
{
  nextBlockId = 0;
  Buffer emptyBlock;
  compressBuffer(emptyBlock, data, dataOffset);
}

bool_t TableFile::add(const DataHeader& data, timestamp_t timeOffset)
{
  if(data.id <= lastId || data.timestamp < lastTimestamp)
    return lastError = argumentError, false;

  if(fileHeader.keyCount)
  {
    Key* lastKey = (Key*)(byte_t*)keys + (fileHeader.keyCount - 1);
    if(lastKey->position == fileHeader.keyPosition + fileHeader.keySize) // is last key is uncompressed?
    {
      // add to to current uncompressed block?
      if(lastKey->size + data.size <= fileHeader.blockSize)
      {
        // add new data to uncompressed block in file
        if(!fileSeek(lastKey->position + lastKey->size))
          return false;
        if(!fileWrite(&data, data.size))
          return false;

        // update key of uncompressed block
        size_t oldUncompressedBlockSize = lastKey->size;
        Key newKey = *lastKey;
        newKey.size += data.size;
        if(!fileSeek(fileHeader.keyPosition + ((const byte_t*)lastKey - (const byte_t*)keys)))
          return false;
        if(!fileWrite(&newKey, sizeof(newKey)))
          return false;
        lastKey->size = newKey.size;

        // add data to uncompressed block in memory
        uncompressedBlock.resize(lastKey->size);
        DataHeader* dataHeader = (DataHeader*)((byte_t*)uncompressedBlock + oldUncompressedBlockSize);
        Memory::copy(dataHeader, &data, data.size);

        lastId = data.id;
        lastTimestamp = data.timestamp;

        // update file header
        if(timeOffset != fileHeader.timeOffset)
        {
          FileHeader newHeader = fileHeader;
          newHeader.timeOffset = timeOffset;
          if(!fileSeek(0))
            return false;
          if(!fileWrite(&newHeader, sizeof(newHeader)))
            return false;
          fileHeader.timeOffset = newHeader.timeOffset;
        }

        return lastError = noError, true;
      }

      // write uncompressed block to end of file
      {
        // compress uncompressed block
        Buffer compressedBuffer;
        compressBuffer(uncompressedBlock, compressedBuffer);

        // move block to end of file
        if(!moveCompressedBlockToEnd(compressedBuffer, *lastKey))
          return false;

        // reset uncompressed block
        uncompressedBlock.clear();
      }
    }

    // ensure key block is not full
    if(!increaseKeyBlockSize(sizeof(Key)))
      return false;
  }

  // add new block
  if(!addNewBlock(data, timeOffset))
    return false;

  return lastError = noError, true;
}

/*private*/ bool_t TableFile::addNewBlock(const DataHeader& data, timestamp_t timeOffset)
{
  ASSERT((fileHeader.keyCount + 1) * sizeof(Key) <= fileHeader.keySize);

  if(data.size >= fileHeader.blockSize)
  {
    Buffer compressedBlock;
    compressBuffer(&data, data.size, compressedBlock);

    if(!fileSeek(fileSize))
      return false;
    if(!fileWrite(compressedBlock))
      return false;

    Key newKey = {data.id, data.timestamp, fileSize, (uint16_t)compressedBlock.size()};
    size_t keyPosition = fileHeader.keyCount * sizeof(Key);
    if(!fileSeek(fileHeader.keyPosition + keyPosition))
      return false;
    if(!fileWrite(&newKey, sizeof(newKey)))
      return false;

    FileHeader newFileHeader = fileHeader;
    ++newFileHeader.keyCount;
    newFileHeader.timeOffset = timeOffset;
    if(!fileSeek(0))
      return false;
    if(!fileWrite(&newFileHeader, sizeof(newFileHeader)))
      return false;
    fileHeader.keyCount = newFileHeader.keyCount;
    fileHeader.timeOffset = newFileHeader.timeOffset;

    keys.resize(keyPosition + sizeof(Key));
    Memory::copy((byte_t*)keys + keyPosition, &newKey, sizeof(newKey));
    fileSize += data.size;
  }
  else
  {
    if(!fileSeek(fileHeader.keyPosition + fileHeader.keySize))
      return false;
    if(!fileWrite(&data, data.size))
      return false;

    Key newKey = {data.id, data.timestamp, fileHeader.keyPosition + fileHeader.keySize, data.size};
    size_t keyPosition = fileHeader.keyCount * sizeof(Key);
    if(!fileSeek(fileHeader.keyPosition + keyPosition))
      return false;
    if(!fileWrite(&newKey, sizeof(newKey)))
      return false;

    FileHeader newFileHeader = fileHeader;
    ++newFileHeader.keyCount;
    newFileHeader.timeOffset = timeOffset;
    if(!fileSeek(0))
      return false;
    if(!fileWrite(&newFileHeader, sizeof(newFileHeader)))
      return false;
    fileHeader.keyCount = newFileHeader.keyCount;
    fileHeader.timeOffset = newFileHeader.timeOffset;

    keys.resize(keyPosition + sizeof(Key));
    Memory::copy((byte_t*)keys + keyPosition, &newKey, sizeof(newKey));
    fileSize += data.size;
    uncompressedBlock.resize(data.size);
    Memory::copy(uncompressedBlock, &data, data.size);
  }

  lastId = data.id;
  lastTimestamp = data.timestamp;
  return true;
}

/*private*/ bool_t TableFile::moveCompressedBlockToEnd(const Buffer& compressedBlock, Key& key)
{
  // write block to end of file
  if(!fileSeek(fileSize))
    return false;
  if(!fileWrite(compressedBlock))
    return false;

  // update index of first compressed block
  if(!fileSeek(fileHeader.keyPosition + ((const byte_t*)&key - (const byte_t*)keys)))
    return false;
  Key newKey = key;
  newKey.position = fileSize;
  newKey.size = compressedBlock.size();
  if(!fileWrite(&newKey, sizeof(newKey)))
    return false;
  key.position = newKey.position;
  key.size = newKey.size;
  fileSize += key.size;
  return true;
}

bool_t TableFile::update(const DataHeader& data)
{
  Key* key = findBlockKey(data.id);
  if(!key)
    return false;

  // is entity in uncompressed block?
  if(key->position == fileHeader.keyPosition + fileHeader.keySize)
  {
    // create copy of uncompressed block
    Buffer uncompressedBlock = this->uncompressedBlock;

    // find and remove entity from copy of uncompressed block
    if(!updateEntity(data, uncompressedBlock))
      return false;

    // compress copy of uncompressed block
    Buffer compressedBlock;
    compressBuffer(uncompressedBlock, compressedBlock);

    // move copy of uncomprossed block to end of file
    if(!moveCompressedBlockToEnd(compressedBlock, *key))
      return false;

    // reset uncompressed block
    this->uncompressedBlock.clear();

    // rewrite uncompressed block
    uint64_t uncompressedBlockPosition = fileHeader.keyPosition + fileHeader.keySize;
    if(!fileSeek(uncompressedBlockPosition))
      return false;
    if(!fileWrite(uncompressedBlock))
      return false;

    // update index of uncompressed block
    Key newKey = *key;
    newKey.position = uncompressedBlockPosition;
    newKey.size = uncompressedBlock.size();
    if(!fileSeek(fileHeader.keyPosition + ((const byte_t*)key - (const byte_t*)keys)))
      return false;
    if(!fileWrite(&newKey, sizeof(newKey)))
      return false;
    key->position = newKey.position;
    key->size = newKey.size;
    fileSize -= compressedBlock.size();

    // update uncompressed block
    this->uncompressedBlock.swap(uncompressedBlock);
  }

  else // entity is in a compressed block
  {
    // read compressed block
    Buffer compressedBlock;
    compressedBlock.resize(key->size);
    if(!fileSeek(key->position))
      return false;
    if(!fileRead(compressedBlock))
      return false;

    // decompress block
    Buffer block;
    if(!decompressBuffer(compressedBlock, block))
      return false;

    // remove entity from decompressed block
    if(!updateEntity(data, block))
      return false;

    // compress block
    compressBuffer(block, compressedBlock);

    // move compressed block to end of file
    if(!moveCompressedBlockToEnd(compressedBlock, *key))
      return false;
  }
  return lastError = noError, true;

}

bool_t TableFile::remove(uint64_t id)
{
  Key* key = findBlockKey(id);
  if(!key)
    return false;

  // is entity in uncompressed block?
  if(key->position == fileHeader.keyPosition + fileHeader.keySize)
  {
    // create copy of uncompressed block
    Buffer uncompressedBlock = this->uncompressedBlock;

    // find and remove entity from copy of uncompressed block
    if(!removeEntity(id, uncompressedBlock))
      return false;

    // remove block?
    if(uncompressedBlock.isEmpty())
    {
      FileHeader newFileHeader = fileHeader;
      --newFileHeader.keyCount;
      if(!fileSeek(0))
        return false;
      if(!fileWrite(&newFileHeader, sizeof(newFileHeader)))
        return false;
      fileHeader.keyCount = newFileHeader.keyCount;
      keys.resize(fileHeader.keyCount * sizeof(Key));
      this->uncompressedBlock.clear();
      return true;
    }

    // compress copy of uncompressed block
    Buffer compressedBlock;
    compressBuffer(uncompressedBlock, compressedBlock);

    // move copy of uncomprossed block to end of file
    if(!moveCompressedBlockToEnd(compressedBlock, *key))
      return false;

    // reset uncompressed block
    this->uncompressedBlock.clear();

    // rewrite uncompressed block
    uint64_t uncompressedBlockPosition = fileHeader.keyPosition + fileHeader.keySize;
    if(!fileSeek(uncompressedBlockPosition))
      return false;
    if(!fileWrite(uncompressedBlock))
      return false;

    // update index of uncompressed block
    Key newKey = *key;
    newKey.position = uncompressedBlockPosition;
    newKey.size = uncompressedBlock.size();
    if(!fileSeek(fileHeader.keyPosition + ((const byte_t*)key - (const byte_t*)keys)))
      return false;
    if(!fileWrite(&newKey, sizeof(newKey)))
      return false;
    key->position = newKey.position;
    key->size = newKey.size;
    fileSize -= compressedBlock.size();

    // update uncompressed block
    this->uncompressedBlock.swap(uncompressedBlock);
  }

  else // entity is in a compressed block
  {
    // read compressed block
    Buffer compressedBlock;
    compressedBlock.resize(key->size);
    if(!fileSeek(key->position))
      return false;
    if(!fileRead(compressedBlock))
      return false;

    // decompress block
    Buffer block;
    if(!decompressBuffer(compressedBlock, block))
      return false;

    // remove entity from decompressed block
    if(!removeEntity(id, block))
      return false;

    // remove block?
    if(block.isEmpty())
    {
      // ensure there is enough space left in key block
      if(!increaseKeyBlockSize((fileHeader.keyCount - 1) * sizeof(Key)))
        return false;

      // copy key block without the key to be removed to end of the used key buffer
      size_t oldUsedKeySize = fileHeader.keyCount * sizeof(Key);
      uint64_t newKeyPosition = fileHeader.keyPosition + oldUsedKeySize;
      if(!fileSeek(newKeyPosition))
        return false;
      if((const byte_t*)key > (const byte_t*)keys)
        if(!fileWrite((const byte_t*)keys, (const byte_t*)key - (const byte_t*)keys))
          return false;
      const byte_t* nextKey = (const byte_t*)key + sizeof(Key);
      const byte_t* keysEnd = (const byte_t*)keys + oldUsedKeySize;
      if(nextKey < keysEnd)
        if(!fileWrite(nextKey, keysEnd - nextKey))
          return false;

      // update key position in file header
      FileHeader newFileHeader = fileHeader;
      newFileHeader.keyPosition = newKeyPosition;
      --newFileHeader.keyCount;
      if(!fileSeek(0))
        return false;
      if(!fileWrite(&newFileHeader, sizeof(newFileHeader)))
        return false;
      fileHeader.keyPosition = newFileHeader.keyPosition;
      fileHeader.keyCount = newFileHeader.keyCount;

      // remove key from keys in ram
      if(nextKey < keysEnd)
        Memory::move((byte_t*)key, nextKey, keysEnd - nextKey);
      keys.resize(fileHeader.keyCount * sizeof(Key));

      // rewrite key block
      if(!fileSeek(sizeof(FileHeader)))
        return false;
      if(!fileWrite(keys))
        return false;

      // update file header
      newFileHeader = fileHeader;
      newFileHeader.keyPosition = sizeof(FileHeader);
      if(!fileSeek(0))
        return false;
      if(!fileWrite(&newFileHeader, sizeof(newFileHeader)))
        return false;
      fileHeader.keyPosition = newFileHeader.keyPosition;
      return true;
    }

    // compress block
    compressBuffer(block, compressedBlock);

    // move compressed block to end of file
    if(!moveCompressedBlockToEnd(compressedBlock, *key))
      return false;
  }
  return lastError = noError, true;
}

/*private*/ bool_t TableFile::increaseKeyBlockSize(size_t freeSize)
{
  size_t newKeySize = fileHeader.keyCount * sizeof(Key) + freeSize;
  if(newKeySize <= fileHeader.keySize)
    return true;

  // write uncompressed block to end of file?
  Key* lastKey = (Key*)(byte_t*)keys + (fileHeader.keyCount - 1);
  if(lastKey->position == fileHeader.keyPosition + fileHeader.keySize)
  {
    // compress uncompressed block
    Buffer compressedBuffer;
    compressBuffer(uncompressedBlock, compressedBuffer);

    // move block to end of file
    if(!moveCompressedBlockToEnd(compressedBuffer, *lastKey))
      return false;

    // reset uncompressed block
    uncompressedBlock.clear();
  }

  Key* firstCompressedBlockKey = findFirstCompressedBlock();
  ASSERT(firstCompressedBlockKey);

  do
  {
    // read first compressed block
    Buffer compressedBuffer;
    compressedBuffer.resize(firstCompressedBlockKey->size);
    if(!fileSeek(firstCompressedBlockKey->position))
      return false;
    if(!fileRead(compressedBuffer))
      return false;

    // move compressed block to end of file
    if(!moveCompressedBlockToEnd(compressedBuffer, *firstCompressedBlockKey))
      return false;

    // find new first compressed block
    firstCompressedBlockKey = findFirstCompressedBlock();
    ASSERT(firstCompressedBlockKey);

    // increse key block size in file header
    FileHeader newHeader = fileHeader;
    newHeader.keySize = (uint32_t)(firstCompressedBlockKey->position - fileHeader.keyPosition - fileHeader.blockSize);
    if(!fileSeek(0))
      return false;
    if(!fileWrite(&newHeader, sizeof(newHeader)))
      return false;
    fileHeader.keySize = newHeader.keySize;
  } while(newKeySize > fileHeader.keySize);
  return true;
}

/*private*/ TableFile::Key* TableFile::findFirstCompressedBlock()
{
  if(!fileHeader.keyCount)
    return 0;
  Key* firstKey = (Key*)(byte_t*)keys;
  const Key* end = firstKey + fileHeader.keyCount;
  const Key* lastKey = firstKey + (fileHeader.keyCount - 1);
  if(lastKey->position == fileHeader.keyPosition + fileHeader.keySize) // last key is uncompressed
  {
    if(lastKey == firstKey)
      return 0;
    --end;
  }
  Key* i = firstKey;
  Key* minKey = firstKey;
  uint64_t minPosition = i->position;
  ++i;
  for(; i < end; ++i)
    if(i->position < minPosition)
    {
      minPosition = i->position;
      minKey = i;
    }
  return minKey;
}

/*private*/ bool_t TableFile::getCompressedBlock(const Key* key, Buffer& data, size_t dataOffset)
{
  // read data bock
  if(key->position == fileHeader.keyPosition + fileHeader.keySize)
    compressBuffer(uncompressedBlock, data, dataOffset);
  else
  {
    if(key->size < sizeof(uint16_t))
      return false;

      //seek to position
    if(!fileSeek(key->position))
      return false;

    // read block
    data.resize(dataOffset + key->size);
    if(!fileRead((byte_t*)data + dataOffset, key->size))
      return false;
  }
  return true;
}

bool_t TableFile::fileWrite(const void_t* buffer, size_t size)
{
  if(file2.write(buffer, size) != (ssize_t)size)
    return lastError = fileError, false;
  return true;
}

bool_t TableFile::fileWrite(const Buffer& buffer)
{
  size_t size = buffer.size();
  if(file2.write((const byte_t*)buffer, size) != (ssize_t)size)
    return lastError = fileError, false;
  return true;
}

bool_t TableFile::fileRead(void_t* buffer, size_t size)
{
  if(file2.read(buffer, size) != (ssize_t)size)
    return lastError = ::Error::getLastError() ? fileError : dataError, false;
  return true;
}

bool_t TableFile::fileRead(Buffer& buffer)
{
  size_t size = buffer.size();
  if(file2.read((byte_t*)buffer, size) != (ssize_t)size)
    return lastError = ::Error::getLastError() ? fileError : dataError, false;
  return true;
}

bool_t TableFile::fileSeek(uint64_t position)
{
  if(file2.seek(position) != (int64_t)position)
    return lastError = ::Error::getLastError() ? fileError : dataError, false;
  return true;
}

/*private*/ TableFile::Key* TableFile::findBlockKey(uint64_t id)
{
  // binary search on indices
  Key* key = (Key*)(byte_t*)keys;
  const Key* keyEnd = key + keys.size() / sizeof(Key);
  if(key == keyEnd || id < key->id)
    return lastError = notFoundError, (Key*)0;
  if(keyEnd - key > 1)
    for(size_t stepSize = ((keyEnd - key) + 1) >> 1;; stepSize = (stepSize + 1) >> 1)
    {
      Key* i = key + stepSize;
      if(i >= keyEnd || id < i->id)
      {
        if(stepSize == 1)
          break;
      }
      else
        key = i;
    }

  // todo: add cache for key and (key + 1)

  return key;

  // todo: optimize binary search
  /*
  if(indexEnd - index > 1)
  {
    size_t stepSize = ((indexEnd - index) + 1) >> 1;
    const Index* i = index + stepSize;
  step:
    if(id < i->id)
    {
      if(stepSize == 1)
        goto done;
      stepSize = (stepSize + 1) >> 1;
      i = index + stepSize;
      goto step;
    }
    index = i;
    for(;;)
    {
      stepSize = (stepSize + 1) >> 1;
      i = index + stepSize;
      if(i < indexEnd)
        goto step;
      if(stepSize == 1)
        goto done;
    }
  }
done: ;
  */
  /*
int binsearch_5( arr_t array[], size_t size, arr_t key, size_t *index ){
  if( !array || !size ) return 0;
  arr_t *p=array;
  while( size > 8 ){
    size_t w=size/2;
    if( p[w+1] <= key ){ p+=w+1; size-=w+1; } else  size =w;
  }
  if( size==8 ){ if( p[5] <=  key ){ p+=5; size=3; } else size=4; }
  if( size==6 ){ if( p[4] <=  key ){ p+=4; size=2; } else size=3; }
  if( size==5 ){ if( p[3] <=  key ){ p+=3; size=2; } else size=2; }
  if( size==4 ){ if( p[3] <=  key ){ p+=3; size=1; } else size=2; }
  if( size==2 ){ if( p[2] <=  key ){ p+=2; size=0; } else size=1; }

  switch(size){
    case 7: if( p[4] <= key ) p+=4;
    case 3: if( p[2] <= key ) p+=2;
    case 1: if( p[1] <= key ) p+=1;
  }
  *index=p-array; return p[0]==key;
}
  */
}

/*private*/ const TableFile::Key* TableFile::findBlockKeyByTime(uint64_t timestamp)
{
  // binary search on indices
  const Key* key = (const Key*)(const byte_t*)keys;
  const Key* keyEnd = key + keys.size() / sizeof(Key);
  if(key == keyEnd || timestamp < key->timestamp)
    return lastError = notFoundError, (const Key*)0;
  if(keyEnd - key > 1)
    for(size_t stepSize = ((keyEnd - key) + 1) >> 1;; stepSize = (stepSize + 1) >> 1)
    {
      const Key* i = key + stepSize;
      if(i >= keyEnd || timestamp < i->timestamp)
      {
        if(stepSize == 1)
          break;
      }
      else
        key = i;
    }

  // todo: optimize binary search

  return key;
}

bool_t TableFile::removeEntity(uint64_t id, Buffer& block)
{
  DataHeader* dataHeader = (DataHeader*)(byte_t*)block;
  size_t remainingDataSize = block.size();
  while(remainingDataSize >= sizeof(DataHeader))
  {
    if(dataHeader->id == id)
      goto found;
    remainingDataSize -= dataHeader->size;
    dataHeader = (DataHeader*)((byte_t*)dataHeader + dataHeader->size);
  }
  return lastError = notFoundError, false;
found:;

  // remove entity from block
  size_t entitySize = dataHeader->size;
  if(remainingDataSize > entitySize)
    Memory::move(dataHeader, (const byte_t*)dataHeader + entitySize, remainingDataSize - entitySize);
  block.resize(block.size() - entitySize);
  return true;
}

bool_t TableFile::updateEntity(const DataHeader& data, Buffer& block)
{
  DataHeader* dataHeader = (DataHeader*)(byte_t*)block;
  size_t remainingDataSize = block.size();
  uint64_t id = data.id;
  while(remainingDataSize >= sizeof(DataHeader))
  {
    if(dataHeader->id == id)
      goto found;
    remainingDataSize -= dataHeader->size;
    dataHeader = (DataHeader*)((byte_t*)dataHeader + dataHeader->size);
  }
  return lastError = notFoundError, false;
found:;

  // update entity in block
  size_t entitySize = dataHeader->size;
  size_t oldBlockSize = block.size();
  block.resize(oldBlockSize + data.size);
  if(remainingDataSize > entitySize)
    Memory::move(dataHeader + data.size, (const byte_t*)dataHeader + entitySize, remainingDataSize - entitySize);
  Memory::copy(dataHeader, &data, data.size);
  block.resize(oldBlockSize - entitySize + data.size);
  return true;
}

/*private static*/ void_t TableFile::compressBuffer(const Buffer& buffer, Buffer& compressedBuffer)
{
  size_t uncompressedBlockSize = buffer.size();
  compressedBuffer.resize(sizeof(uint16_t) + LZ4_compressBound(uncompressedBlockSize));
  compressedBuffer.resize(sizeof(uint16_t) + LZ4_compress((const char*)(const byte_t*)buffer, (char*)(byte_t*)compressedBuffer + sizeof(uint16_t), uncompressedBlockSize));
  *(int16_t*)(byte_t*)compressedBuffer = uncompressedBlockSize;
}

/*private static*/ void_t TableFile::compressBuffer(const Buffer& buffer, Buffer& compressedBuffer, size_t offset)
{
  size_t uncompressedBlockSize = buffer.size();
  size_t offsetAndSizeHeader = offset + sizeof(uint16_t);
  compressedBuffer.resize(offsetAndSizeHeader + LZ4_compressBound(uncompressedBlockSize));
  compressedBuffer.resize(offsetAndSizeHeader + LZ4_compress((const char*)(const byte_t*)buffer, (char*)(byte_t*)compressedBuffer + offsetAndSizeHeader, uncompressedBlockSize));
  *(int16_t*)((byte_t*)compressedBuffer + offset) = uncompressedBlockSize;
}

/*private static*/ void_t TableFile::compressBuffer(const void_t* data, size_t size, Buffer& compressedBuffer)
{
  compressedBuffer.resize(sizeof(uint16_t) + LZ4_compressBound(size));
  compressedBuffer.resize(sizeof(uint16_t) + LZ4_compress((const char*)data, (char*)(byte_t*)compressedBuffer + sizeof(uint16_t), size));
  *(int16_t*)(byte_t*)compressedBuffer = size;
}

/*private static*/ void_t TableFile::compressBuffer(const void_t* data, size_t size, Buffer& compressedBuffer, size_t offset)
{
  size_t offsetAndSizeHeader = offset + sizeof(uint16_t);
  compressedBuffer.resize(offsetAndSizeHeader + LZ4_compressBound(size));
  compressedBuffer.resize(offsetAndSizeHeader + LZ4_compress((const char*)data, (char*)(byte_t*)compressedBuffer + offsetAndSizeHeader, size));
  *(int16_t*)(byte_t*)(compressedBuffer + offset) = size;
}

/*private*/ bool_t TableFile::decompressBuffer(const Buffer& compressedBuffer, Buffer& buffer)
{
  if(compressedBuffer.size() < sizeof(uint16_t))
    return lastError = dataError, false;
  int_t originalSize = *(const uint16_t*)(const byte_t*)compressedBuffer;
  buffer.resize(originalSize);
  if(LZ4_decompress_safe((const char*)(const byte_t*)compressedBuffer + sizeof(uint16_t), (char_t*)(byte_t*)buffer, compressedBuffer.size() - sizeof(uint16_t), originalSize) != originalSize)
    return lastError = dataError, false;
  return true;
}

/*private*/ bool_t TableFile::decompressBuffer(const Buffer& compressedBuffer, Buffer& buffer, size_t offset)
{
  if(compressedBuffer.size() < sizeof(uint16_t))
    return lastError = dataError, false;
  int_t originalSize = *(const uint16_t*)(const byte_t*)compressedBuffer;
  buffer.resize(offset + originalSize);
  if(LZ4_decompress_safe((const char*)(const byte_t*)compressedBuffer + sizeof(uint16_t), (char_t*)(byte_t*)buffer + offset, compressedBuffer.size() - sizeof(uint16_t), originalSize) != originalSize)
    return lastError = dataError, false;
  return true;
}
