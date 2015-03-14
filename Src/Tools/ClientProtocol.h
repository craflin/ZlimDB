
#pragma once

#include <zlimdbprotocol.h>

class ClientProtocol
{
public:
  static bool_t getString(const zlimdb_header& header, size_t offset, size_t size, String& result)
  {
    if(offset + size > header.size)
      return false;
    result.attach((const char_t*)&header + offset, size);
    return true;
  }

  static bool_t getString(const zlimdb_header& header, const zlimdb_entity& entity, size_t offset, size_t size, String& result)
  {
    size_t strEnd = offset + size;
    if(strEnd > entity.size || (const byte_t*)&entity + strEnd > (const byte_t*)&header + header.size)
      return false;
    result.attach((const char_t*)&entity + offset, size);
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

  static void_t setString(zlimdb_entity& entity, uint16_t& length, size_t offset, const String& str)
  {
    length = str.length();
    Memory::copy((byte_t*)&entity + offset, (const char_t*)str, str.length());
  }
};
