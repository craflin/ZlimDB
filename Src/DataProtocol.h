
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
  };
  
  enum TableId
  {
    clientsTable,
    tablesTable,
    timeTable,
    usersTable,
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
  };

#pragma pack(push, 4)
  struct Header
  {
    enum
    {
      partial = 0x01,
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

  struct QueryRequest : public Header
  {
    enum Type
    {
      all,
      byId,
      sinceId,
      sinceTime,
    };

    Type type;
    uint32_t tableId;
    uint64_t param;
  };

  struct SubscribeRequest
  {
    char_t channel[33];
    uint64_t maxAge;
    uint64_t sinceId;
  };
  struct SubscribeResponse
  {
    uint64_t channelId;
    //uint32_t flags;
  };
  struct UnsubscribeRequest
  {
    uint32_t channelId;
  };
  struct UnsubscribeResponse
  {
    uint32_t channelId;
  };

  struct Entity
  {
    uint32_t size;
    uint64_t id;
    uint64_t time;
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

};
