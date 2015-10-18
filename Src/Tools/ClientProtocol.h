
#pragma once

#include <zlimdbprotocol.h>

class ClientProtocol
{
public:
  static bool_t getString(const zlimdb_header& header, size_t offset, size_t length, String& result)
  {
    if(offset + length > header.size)
      return false;
    if(!length)
    {
      result = String();
      return true;
    }
    char_t* str = (char_t*)&header + offset;
    str[--length] = '\0';
    result.attach(str, length);
    return true;
  }

  static bool_t getString(const zlimdb_header& header, const zlimdb_entity& entity, size_t offset, size_t length, String& result)
  {
    if(offset + length > entity.size)
      return false;
    if((const byte_t*)&entity + offset + length > (const byte_t*)&header + header.size)
      return false;
    if(!length)
    {
      result = String();
      return true;
    }
    char_t* str = (char_t*)&entity + offset;
    str[--length] = '\0';
    result.attach(str, length);
    return true;
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

  static bool_t copyString(zlimdb_entity& entity, uint16_t& length, size_t offset, const String& str, size_t maxSize)
  {
    length = str.length() + 1;
    if((size_t)entity.size + length > maxSize)
      return false;
    Memory::copy((byte_t*)&entity + offset, (const char_t*)str, length);
    entity.size += length;
    return true;
  }
};
