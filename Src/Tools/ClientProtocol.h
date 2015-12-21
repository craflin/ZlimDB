
#pragma once

#include <zlimdbprotocol.h>

class ClientProtocol
{
public:
  static bool_t getString(const zlimdb_header& header, size_t offset, size_t length, String& result)
  {
    if(!length || offset + length > header.size)
      return false;
    char_t* str = (char_t*)&header + offset;
    if(str[--length])
      return false;
    result.attach(str, length);
    return true;
  }

  static bool_t getString(const zlimdb_entity& entity, size_t offset, size_t length, String& result)
  {
    if(!length || offset + length > entity.size)
      return false;
    char_t* str = (char_t*)&entity + offset;
    if(str[--length])
      return false;
    result.attach(str, length);
    return true;
  }

  static const zlimdb_entity* getEntity(const zlimdb_header& header, size_t offset, size_t minSize)
  {
    if(offset + minSize > header.size)
      return 0;
    const zlimdb_entity* result = (const zlimdb_entity*)((const byte_t*)&header + offset);
    if(result->size < minSize || offset + result->size > header.size)
      return 0;
    return result;
  }

  static zlimdb_entity* getEntity(zlimdb_header& header, size_t offset, size_t minSize)
  {
    if(offset + minSize > header.size)
      return 0;
    zlimdb_entity* result = (zlimdb_entity*)((byte_t*)&header + offset);
    if(result->size < minSize || offset + result->size > header.size)
      return 0;
    return result;
  }

  static void_t setHeader(zlimdb_header& header, zlimdb_message_type type, size_t size, uint32_t requestId, uint8_t flags = 0)
  {
    header.size = size;
    header.message_type = type;
    header.request_id = requestId;
    header.flags = flags;
  }

  static void_t setEntityHeader(zlimdb_entity& entity, uint64_t id, uint64_t time, uint16_t size)
  {
    entity.id = id;
    entity.time = time;
    entity.size = size;
  }

  static bool_t copyString(const String& str, zlimdb_entity& entity, uint16_t& length, size_t maxSize)
  {
    length = str.length() + 1;
    if((size_t)entity.size + length > maxSize)
      return false;
    Memory::copy((byte_t*)&entity + entity.size, (const char_t*)str, length);
    entity.size += length;
    return true;
  }
};
