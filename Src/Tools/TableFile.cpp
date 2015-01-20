
#include <lz4.h>

#include <nstd/Debug.h>
#include <nstd/Error.h>

#include "TableFile.h"

#define DEFAULT_BLOCK_SIZE 4096
#define DEFAULT_KEY_SIZE (DEFAULT_BLOCK_SIZE / sizeof(Key) * sizeof(Key))

bool_t TableFile::create(const String& fileName)
{
  if(!file2.open(fileName, File::readFlag | File::writeFlag))
    return lastError = fileError, false;

  char buffer[sizeof(FileHeader) + DEFAULT_KEY_SIZE + DEFAULT_BLOCK_SIZE];
  fileHeader.magic = *(uint32_t*)"ZLIM";
  fileHeader.version = 1;
  fileHeader.keyCount = 0;
  fileHeader.keySize = DEFAULT_KEY_SIZE;
  fileHeader.blockSize = DEFAULT_BLOCK_SIZE;
  *(FileHeader*)buffer = fileHeader;
  Memory::zero(buffer + sizeof(FileHeader), DEFAULT_KEY_SIZE + DEFAULT_BLOCK_SIZE);
  if(!fileWrite(&buffer, sizeof(buffer)))
    return file2.close(), false;

  uncompressedBlockIndex = -1;
  firstCompressedBlockIndex = -1;
  fileSize = sizeof(buffer);
  lastId = 0;
  lastTimestamp = 0;
  return lastError = noError, true;
}

bool_t TableFile::open(const String& fileName)
{
  if(!file2.open(fileName, File::readFlag | File::writeFlag | File::openFlag))
    return lastError = fileError, false;

  // read header
  if(!fileRead(&fileHeader, sizeof(fileHeader)))
    return file2.close(), false;
  if(fileHeader.magic != *(uint32_t*)"ZLIM" ||
     fileHeader.version != 1)
    return file2.close(), lastError = dataError, false;

  // read indices
  size_t usedKeysSize = fileHeader.keyCount * sizeof(Key);
  if(usedKeysSize > fileHeader.keySize)
    return file2.close(), lastError = dataError, false;
  keys.resize(usedKeysSize);
  if(!fileRead((byte_t*)keys, usedKeysSize))
    return file2.close(), false;

  // find uncompressed block, first compressed block and file size
  lastId = 0;
  lastTimestamp = 0;
  uncompressedBlockIndex = -1;
  firstCompressedBlockIndex = -1;
  fileSize = sizeof(FileHeader) + fileHeader.keySize + fileHeader.blockSize;
  uint32_t compressedKeyCount = fileHeader.keyCount;
  if(fileHeader.keyCount > 0)
  {
    const Key* firstKey = (const Key*)(const byte_t*)keys, * i = firstKey;
    const Key* lastKey = firstKey + fileHeader.keyCount - 1;
    if(lastKey->position == sizeof(FileHeader) + fileHeader.keySize) // last key is uncompressed
    {
      --compressedKeyCount;
      uncompressedBlockIndex = lastKey - firstKey;
    }
  }
  const Key* idMaxKey = 0;
  if(compressedKeyCount > 0)
  {
    const Key* firstKey = (const Key*)(const byte_t*)keys, * i = firstKey;
    const Key* minKey, *maxKey;
    minKey = maxKey = idMaxKey = firstKey;
    uint64_t minPosition, maxPosition, maxId;
    minPosition = maxPosition = i->position;
    maxId = i->id;
    ++i;
    for(const Key * end = firstKey + compressedKeyCount; i < end; ++i)
    {
      if(i->position < minPosition)
      {
        minPosition = i->position;
        minKey = i;
      }
      if(i->position > maxPosition)
      {
        maxPosition = i->position;
        maxKey = i;
      }
      if(i->id > maxId)
      {
        maxId = i->id;
        idMaxKey = i;
      }
    }
    firstCompressedBlockIndex = minKey - firstKey;
    fileSize = maxKey->position + maxKey->size;
  }

  // read uncomressed block
  currentBlock.clear();
  if(uncompressedBlockIndex >= 0)
  {
    const Key& key = ((const Key*)(const byte_t*)keys)[uncompressedBlockIndex];
    if(!fileSeek(key.position))
      return file2.close(), false;
    currentBlock.resize(key.size);
    if(!fileRead((byte_t*)currentBlock, key.size))
      return file2.close(), false;
  }

  // find lastId and lastTimestamp
  Buffer buffer;
  const DataHeader* maxIdBuffer;
  size_t maxIdBufferSize;
  if(uncompressedBlockIndex < 0 && idMaxKey)
  {
    if(!fileSeek(idMaxKey->position))
      return file2.close(), false;
    Buffer compressedBuffer(idMaxKey->size);
    if(!fileRead((byte_t*)compressedBuffer, idMaxKey->size))
      return file2.close(), false;
    int_t originalSize = *(const uint16_t*)(const byte_t*)compressedBuffer;
    buffer.resize(originalSize);
    if(LZ4_decompress_safe((const char*)(const byte_t*)compressedBuffer + sizeof(uint16_t), (char_t*)(byte_t*)buffer, idMaxKey->size - sizeof(uint16_t), originalSize) != originalSize)
      return lastError = dataError, false;
    maxIdBuffer = (const DataHeader*)(const byte_t*)buffer;
    maxIdBufferSize = originalSize;
  }
  else
  {
    maxIdBuffer = (const DataHeader*)(const byte_t*)currentBlock;
    maxIdBufferSize = currentBlock.size();
  }
  while(maxIdBufferSize >= sizeof(DataHeader))
  {
    lastId = maxIdBuffer->id;
    lastTimestamp = maxIdBuffer->id;
    maxIdBufferSize -= maxIdBuffer->size;
    maxIdBuffer = (const DataHeader*)((const byte_t*)maxIdBuffer + maxIdBuffer->size);
  }

  return lastError = noError, true;
}

bool_t TableFile::get(uint64_t id, Buffer& result, size_t dataOffset)
{
  const Key* key = findBlockKey(id);
  if(!key)
    return false;

  int32_t remainingDataSize;
  const DataHeader* dataHeader;
  Buffer buffer;
  if(uncompressedBlockIndex >= 0 && key == &((const Key*)(const byte_t*)keys)[uncompressedBlockIndex])
  {
    remainingDataSize = currentBlock.size();
    dataHeader = (const DataHeader*)(const byte_t*)currentBlock;
  }
  else
  {
    if(key->size < sizeof(uint16_t))
      return lastError = dataError, false;

      //seek to position
    if(!fileSeek(key->position))
      return false;

    // read block
    Buffer compressedBuffer(key->size);
    if(!fileRead((byte_t*)compressedBuffer, key->size))
      return false;

    // decompress block
    int_t originalSize = *(const uint16_t*)(const byte_t*)compressedBuffer;
    buffer.resize(originalSize);
    if(LZ4_decompress_safe((const char*)(const byte_t*)compressedBuffer + sizeof(uint16_t), (char_t*)(byte_t*)buffer, key->size - sizeof(uint16_t), originalSize) != originalSize)
      return lastError = dataError, false;
    dataHeader = (const DataHeader*)(const byte_t*)buffer;
  }

  // find id
  while(remainingDataSize >= sizeof(DataHeader))
  {
    if(dataHeader->id == id)
      goto found;
    remainingDataSize -= dataHeader->size;
    dataHeader = (const DataHeader*)((const byte_t*)dataHeader + dataHeader->size);
  }
  return lastError = notFoundError, false;
found:;
  if(remainingDataSize < dataHeader->size)
    return lastError = dataError, false;

  // return data
  result.resize(dataOffset + dataHeader->size);
  Memory::copy((byte_t*)result + dataOffset, (const byte_t*)dataHeader, dataHeader->size);
  return lastError = noError, true;
}

bool_t TableFile::getCompressedBlock(uint64_t id, uint64_t& blockId, Buffer& data, size_t dataOffset)
{
  const Key* key = findBlockKey(id);
  if(!key)
    return false;
  if(!getCompressedBlock(key, data, dataOffset))
    return false;
  blockId = key->id;
  return lastError = noError, true;
}

bool_t TableFile::getCompressedBlockByTime(uint64_t timestamp, uint64_t& blockId, Buffer& data, size_t dataOffset)
{
  const Key* key = findBlockKeyByTime(timestamp);
  if(!key)
    return false;
  if(!getCompressedBlock(key, data, dataOffset))
    return false;
  blockId = key->id;
  return lastError = noError, true;
}
bool_t TableFile::getFirstCompressedBlock(uint64_t& blockId, Buffer& data, size_t dataOffset)
{
  if(keys.isEmpty())
  {
    blockId = 0;
    data.resize(dataOffset + sizeof(uint16_t));
    *(uint16_t*)((byte_t*)data + dataOffset) = 0;
    return lastError = noError, true;
  }
  const Key* firstKey = (const Key*)(const byte_t*)keys;
  if(!getCompressedBlock(firstKey, data, dataOffset))
    return false;
  blockId = firstKey->id;
  return lastError = noError, true;
}

bool_t TableFile::hasNextCompressedBlock(uint64_t blockId)
{
  const Key* key = findBlockKey(blockId);
  if(!key)
    return false;
  const Key* keyEnd = (const Key*)(const byte_t*)keys + keys.size() / sizeof(Key);
  return lastError = noError, key + 1 < keyEnd;
}

bool_t TableFile::getNextCompressedBlock(uint64_t lastBlockId, uint64_t& blockId, Buffer& data, size_t dataOffset)
{
  const Key* key = findBlockKey(lastBlockId);
  if(!key)
    return false;
  const Key* keyEnd = (const Key*)(const byte_t*)keys + keys.size() / sizeof(Key);
  ++key;
  if(key >= keyEnd)
    return lastError = notFoundError, false;
  if(!getCompressedBlock(key, data, dataOffset))
    return false;
  blockId = key->id;
  return lastError = noError, true;
}

bool_t TableFile::add(const DataHeader& data)
{
/*
  case #1 currentBlock is invalid
    case #1.1 sampleSize > blockSize (new key required, currentBlock remains invalid)
    case #1.2 sampleSize <= blockSize (new key required, currentBlock becomes valid)
  case #2 else
    case #2.1 currentBlockSize + sampleSize > blockSize (new key required)
    case #2.2 currentBlockSize + sampleSize <= blockSize
*/

  if(data.id <= lastId || data.timestamp < lastTimestamp)
    return lastError = argumentError, false;

  // try to add to to current uncompressed block
  if(uncompressedBlockIndex >= 0)
  {
    Key& uncompressedBlockKey = ((Key*)(byte_t*)keys)[uncompressedBlockIndex];
    if(uncompressedBlockKey.size + data.size <= fileHeader.blockSize) // case #2.2
    { // add new data to uncompressed block
      currentBlock.resize(uncompressedBlockKey.size + data.size);
      DataHeader* dataHeader = (DataHeader*)((byte_t*)currentBlock + uncompressedBlockKey.size);
      Memory::copy(dataHeader, &data, data.size);
      uint64_t position = uncompressedBlockKey.position + ((const byte_t*)dataHeader) - (const byte_t*)currentBlock;
      if(!fileSeek(position))
        return false;
      if(!fileWrite(dataHeader, dataHeader->size))
        return false;
      Key newKey = uncompressedBlockKey;
      newKey.size += dataHeader->size;
      position = sizeof(FileHeader) + (const byte_t*)&uncompressedBlockKey - (const byte_t*)keys;
      if(!fileSeek(position))
        return false;
      if(!fileWrite(&newKey, sizeof(newKey)))
        return false;
      uncompressedBlockKey.size = newKey.size;
      lastId = data.id;
      lastTimestamp = data.timestamp;
      return lastError = noError, true;
    }
  }

  // write uncompressed block to end of file
  if(uncompressedBlockIndex >= 0)
  {
    // compresse uncompressed block
    Key& uncompressedBlockKey = ((Key*)(byte_t*)keys)[uncompressedBlockIndex];
    Buffer compressedBuffer(sizeof(uint16_t) + LZ4_compressBound(uncompressedBlockKey.size));
    int_t blockSize = sizeof(uint16_t) + LZ4_compress((const char*)(const byte_t*)currentBlock, (char*)(byte_t*)compressedBuffer + sizeof(uint16_t), uncompressedBlockKey.size);
    *(int16_t*)(byte_t*)compressedBuffer = uncompressedBlockKey.size;

    // write to end of file
    if(!fileSeek(fileSize))
      return false;
    if(!fileWrite((const byte_t*)compressedBuffer, blockSize))
      return false;

    // update key of uncompressed block
    Key newKey = uncompressedBlockKey;
    newKey.position = fileSize;
    newKey.size = blockSize;
    uint64_t position = sizeof(FileHeader) + (const byte_t*)&uncompressedBlockKey - (const byte_t*)keys;
    if(!fileSeek(position))
      return false;
    if(!fileWrite(&newKey, sizeof(newKey)))
      return false;
    uncompressedBlockKey = newKey;

    // update file size, firstCompressedBlockIndex and uncompressedBlockIndex
    fileSize += blockSize;
    if(firstCompressedBlockIndex < 0)
      firstCompressedBlockIndex = uncompressedBlockIndex;
    uncompressedBlockIndex = -1;
    currentBlock.clear();
  }

  // ensure index is not full
  while(keys.size() + sizeof(Key) > fileHeader.keySize)
  {
    // copy first compressed block to end of file
    ASSERT(firstCompressedBlockIndex >= 0);
    Key& firstCompressedBlockKey = ((Key*)(byte_t*)keys)[firstCompressedBlockIndex];
    Buffer compressedBuffer(firstCompressedBlockKey.size);
    if(!fileSeek(firstCompressedBlockKey.position))
      return false;
    if(!fileRead((byte_t*)compressedBuffer, firstCompressedBlockKey.size))
      return false;
    if(!fileSeek(fileSize))
      return false;
    if(!fileWrite((const byte_t*)compressedBuffer, firstCompressedBlockKey.size))
      return false;
    uint64_t position = sizeof(FileHeader) + (const byte_t*)&firstCompressedBlockKey - (const byte_t*)keys;
    if(!fileSeek(position))
      return false;
    Key newKey = firstCompressedBlockKey;
    newKey.position = fileSize;
    if(!fileWrite(&newKey, sizeof(newKey)))
      return false;
    firstCompressedBlockKey.position = fileSize;
    fileSize += firstCompressedBlockKey.size;

    // find new first compressed block
    const Key* minKey;
    {
      const Key* firstKey = (const Key*)(const byte_t*)keys, * i = firstKey;
      minKey = firstKey;
      uint64_t minPosition;
      minPosition = i->position;
      ++i;
      for(const Key* end = firstKey + fileHeader.keyCount; i < end; ++i)
        if(i->position < minPosition)
        {
          minPosition = i->position;
          minKey = i;
        }
      firstCompressedBlockIndex = minKey - firstKey;
    }

    // increse index size
    FileHeader newHeader = fileHeader;
    newHeader.keySize = (uint32_t)(minKey->position - sizeof(FileHeader) - fileHeader.blockSize);
    if(!fileSeek(0))
      return false;
    if(!fileWrite(&newHeader, sizeof(newHeader)))
      return false;
    fileHeader.keySize = newHeader.keySize;
  }

  // compresse data and add it to end of file
  if(data.size > fileHeader.blockSize)
  {
    // compresse data
    size_t uncompressedBlockSize = data.size;
    currentBlock.resize(uncompressedBlockSize);
    DataHeader* dataHeader = (DataHeader*)(byte_t*)currentBlock;
    Memory::copy(dataHeader, &data, data.size);
    Buffer compressedBuffer(sizeof(uint16_t) + LZ4_compressBound(uncompressedBlockSize));
    int_t blockSize = sizeof(uint16_t) + LZ4_compress((const char*)dataHeader, (char*)(byte_t*)compressedBuffer + sizeof(uint16_t), uncompressedBlockSize);
    *(int16_t*)(byte_t*)compressedBuffer = uncompressedBlockSize;
    currentBlock.clear();

    // add data to end of file
    if(!fileSeek(fileSize))
      return false;
    if(!fileWrite((const byte_t*)compressedBuffer, blockSize))
      return false;

    // create new key
    Key key = {data.id, data.timestamp, fileSize, blockSize};
    uint64_t position = sizeof(FileHeader) + fileHeader.keyCount * sizeof(Key);
    if(!fileSeek(position))
      return false;
    if(!fileWrite(&key, sizeof(key)))
      return false;
    FileHeader newHeader = fileHeader;
    ++newHeader.keyCount;
    if(!fileSeek(0))
      return false;
    if(!fileWrite(&newHeader, sizeof(newHeader)))
      return false;
    fileHeader.keyCount = newHeader.keyCount;
    keys.append((const byte_t*)&key, sizeof(key));
    fileSize += blockSize;
  }

  // add data to new uncompressed block
  else
  {
    // write sample to new current block
    currentBlock.resize(data.size);
    DataHeader* dataHeader = (DataHeader*)(byte_t*)currentBlock;
    Memory::copy(dataHeader, &data, data.size);
    uint64_t position = sizeof(FileHeader) + fileHeader.keySize;
    if(!fileSeek(position))
      return false;
    if(!fileWrite(dataHeader, dataHeader->size))
      return false;

    // create new current block key
    Key key = {data.id, data.timestamp, position, dataHeader->size};
    position = sizeof(FileHeader) + fileHeader.keyCount * sizeof(Key);
    if(!fileSeek(position))
      return false;
    if(!fileWrite(&key, sizeof(key)))
      return false;
    FileHeader newHeader = fileHeader;
    ++newHeader.keyCount;
    if(!fileSeek(0))
      return false;
    if(!fileWrite(&newHeader, sizeof(newHeader)))
      return false;
    uncompressedBlockIndex = fileHeader.keyCount;
    fileHeader.keyCount = newHeader.keyCount;
    keys.append((const byte_t*)&key, sizeof(key));
  }
  lastId = data.id;
  lastTimestamp = data.timestamp;
  return lastError = noError, true;
}

/*

remove/update/insert()
{
  if(sample in uncompressed block)
  {
    remove from uncompressed block in ram
    compress block 
    write compressed block to end of file
    update index
    rewrite uncompressed block
    update index
  }
  else
  {
    read compressed block
    remove from compressed block
    write compressed block to end of file
  }
}
*/

/*private*/ bool_t TableFile::getCompressedBlock(const Key* key, Buffer& data, size_t dataOffset)
{
  // read data bock
  if(uncompressedBlockIndex >= 0 && key == &((const Key*)(const byte_t*)keys)[uncompressedBlockIndex])
  {
    data.resize(dataOffset + sizeof(uint16_t) + LZ4_compressBound(key->size));
    int_t compressedSize = LZ4_compress((const char*)(const byte_t*)currentBlock, (char*)(byte_t*)data + dataOffset + sizeof(uint16_t), key->size);
    *(int16_t*)((byte_t*)data + dataOffset) = key->size;
    data.resize(dataOffset + sizeof(uint16_t) + compressedSize);
  }
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
  if(file2.write(buffer, size) != size)
    return lastError = fileError, false;
  return true;
}

bool_t TableFile::fileRead(void_t* buffer, size_t size)
{
  if(file2.read(buffer, size) != size)
    return lastError = ::Error::getLastError() ? fileError : dataError, false;
  return true;
}

bool_t TableFile::fileSeek(uint64_t position)
{
  if(file2.seek(position) != position)
    return lastError = ::Error::getLastError() ? fileError : dataError, false;
  return true;
}

/*private*/ const TableFile::Key* TableFile::findBlockKey(uint64_t id)
{
  // binary search on indices
  const Key* key = (const Key*)(const byte_t*)keys;
  const Key* keyEnd = key + keys.size() / sizeof(Key);
  if(key == keyEnd || id < key->id)
    return lastError = notFoundError, false;
  if(keyEnd - key > 1)
    for(size_t stepSize = ((keyEnd - key) + 1) >> 1;; stepSize = (stepSize + 1) >> 1)
    {
      const Key* i = key + stepSize;
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
    return lastError = notFoundError, false;
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
