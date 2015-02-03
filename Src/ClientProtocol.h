
#pragma once

#include <zlimdbprotocol.h>

class ClientProtocol
{
public:
  enum MessageType
  {
    errorResponse = zlimdb_message_error_response,
    loginRequest = zlimdb_message_login_request,
    loginResponse = zlimdb_message_login_response,
    authRequest = zlimdb_message_auth_request,
    authResponse = zlimdb_message_auth_response,
    addRequest = zlimdb_message_add_request,
    addResponse = zlimdb_message_add_response,
    updateRequest = zlimdb_message_update_request,
    updateResponse = zlimdb_message_update_response,
    removeRequest = zlimdb_message_remove_request,
    removeResponse = zlimdb_message_remove_response,
    subscribeRequest = zlimdb_message_subscribe_request,
    subscribeResponse = zlimdb_message_subscribe_response,
    unsubscribeRequest = zlimdb_message_unsubscribe_request,
    unsubscribeResponse = zlimdb_message_unsubscribe_response,
    queryRequest = zlimdb_message_query_request,
    queryResponse = zlimdb_message_query_response,
  };
  
  enum TableId
  {
    clientsTable = zlimdb_tabe_clients,
    tablesTable = zlimdb_tabe_tables,
    timeTable = zlimdb_tabe_time,
  };

  enum Error
  {
    invalidMessageSize = zlimdb_error_invalid_message_size,
    invalidMessageType = zlimdb_error_invalid_message_type,
    entityNotFound = zlimdb_error_entity_not_found,
    tableNotFound = zlimdb_error_table_not_found,
    notImplemented = zlimdb_error_not_implemented,
    invalidRequest = zlimdb_error_invalid_request,
    invalidLogin = zlimdb_error_invalid_login,
    tableAlreadyExists = zlimdb_error_table_already_exists,
    couldNotOpenFile = zlimdb_error_could_not_open_file,
    couldNotReadFile = zlimdb_error_could_not_read_file,
    couldNotWriteFile = zlimdb_error_could_not_write_file,
    subscriptionNotFound = zlimdb_error_subscription_not_found,
    invalidData = zlimdb_error_invalid_data,
  };

  struct HeaderFlag
  {
    static const uint8_t fragmented = zlimdb_header_flag_fragmented;
    static const uint8_t compressed = zlimdb_header_flag_compressed;
  };

  struct QueryType
  {
    static const uint8_t all = zlimdb_query_type_all;
    static const uint8_t sinceId = zlimdb_query_type_since_id;
    static const uint8_t sinceTime = zlimdb_query_type_since_time;
    static const uint8_t byId = zlimdb_query_type_by_id;
  };

  typedef zlimdb_message_header Header;
  typedef zlimdb_error_response ErrorResponse;
  typedef zlimdb_login_request LoginRequest;
  typedef zlimdb_login_response LoginResponse;
  typedef zlimdb_auth_request AuthRequest;
  typedef zlimdb_add_request AddRequest;
  typedef zlimdb_update_request UpdateRequest;
  typedef zlimdb_remove_request RemoveRequest;
  typedef zlimdb_subscribe_request SubscribeRequest;
  typedef zlimdb_query_request QueryRequest;
  typedef zlimdb_unsubscribe_request UnsubscribeRequest;
  typedef zlimdb_entity Entity;
  typedef zlimdb_table_entity Table;

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
    header.message_type = type;
    header.request_id = requestId;
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
