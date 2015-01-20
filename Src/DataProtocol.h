
#pragma once

#include <nstd/Base.h>

class DataProtocol
{
public:
  enum MessageType
  {
    errorResponse,
    loginRequest,
    loginResponse,
    authRequest,
    authResponse,
    addRequest,
    addResponse,
    updateRequest,
    updateResponse,
    removeRequest,
    removeResponse,
    subscribeRequest,
    subscribeResponse,
    unsubscribeRequest,
    unsubscribeResponse,
    queryRequest,
    queryResponse,
    numOfMessageTypes,
  };
  
  enum TableId
  {
    clientsTable,
    tablesTable,
    timeTable,
    numOfTableIds,
  };

  enum Error
  {
    invalidMessageSize,
    invalidMessageType,
    entityNotFound,
    tableNotFound,
    notImplemented,
    invalidRequest,
    invalidLogin,
    tableAlreadyExists,
    couldNotOpenFile,
    couldNotReadFile,
    couldNotWriteFile,
    subscriptionNotFound,
    invalidData,
  };

#pragma pack(push, 1)
  struct Header
  {
    enum
    {
      fragmented = 0x01,
      compressed = 0x02,
    };

    uint8_t flags;
    uint32_t size:24; // including header
    uint16_t messageType; // MessageType
    uint32_t requestId;
  };

  struct ErrorResponse : public Header
  {
    uint16_t error;
  };

  struct LoginRequest : public Header
  {
    uint16_t userNameSize;
  };

  struct LoginResponse : public Header
  {
    byte_t pwSalt[32];
    byte_t authSalt[32];
  };

  struct AuthRequest : public Header
  {
    byte_t signature[32];
  };

  struct AddRequest : public Header
  {
    uint32_t tableId;
  };

  struct UpdateRequest : public Header
  {
    uint32_t tableId;
  };

  struct RemoveRequest : public Header
  {
    uint32_t tableId;
  };

  struct SubscribeRequest : public Header
  {
    enum Type
    {
      all,
      sinceId,
      sinceTime,
    };

    Type type;
    uint32_t tableId;
    uint64_t param;
  };

  struct QueryRequest : public SubscribeRequest
  {
    enum Type
    {
      all = SubscribeRequest::all,
      sinceId = SubscribeRequest::sinceId,
      sinceTime = SubscribeRequest::sinceTime,
      byId,
    };

    Type type;
    uint32_t tableId;
    uint64_t param;
  };

  struct UnsubscribeRequest : public Header
  {
    uint32_t tableId;
  };

  struct Entity
  {
    uint64_t id;
    uint64_t time;
    uint16_t size;
  };

  struct Table : public Entity
  {
    uint8_t flags; // e.g. PUBLIC,
    uint16_t nameSize;
  };

#pragma pack(pop)

  static bool_t getString(const Header& header, size_t offset, size_t size, String& result)
  {
    if(offset + size > header.size)
      return false;
    result.attach((const char_t*)&header + offset, size);
    return true;
  }

  static bool_t getString(const Header& header, const Entity& entity, size_t offset, size_t size, String& result)
  {
    size_t strEnd = offset + size;
    if(strEnd > entity.size || (const byte_t*)&entity + strEnd > (const byte_t*)&header + header.size)
      return false;
    result.attach((const char_t*)&entity + offset, size);
    return true;
  }

  static void_t setHeader(Header& header, MessageType type, size_t size, uint32_t requestId, uint8_t flags = 0)
  {
    header.size = size;
    header.messageType = type;
    header.requestId = requestId;
    header.flags = flags;
  }

  static void_t setEntityHeader(Entity& entity, uint64_t id, uint64_t time, uint16_t size)
  {
    entity.id = id;
    entity.time = time;
    entity.size = size;
  }

  static void_t setString(Entity& entity, uint16_t& length, size_t offset, const String& str)
  {
    length = str.length();
    Memory::copy((byte_t*)&entity + offset, (const char_t*)str, str.length());
  }
};
