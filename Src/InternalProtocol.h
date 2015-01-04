
#pragma once

#include "DataProtocol.h"

class InternalProtocol
{
public:
  enum MessageType
  {
    loginRequest = DataProtocol::numOfMessageTypes,
    loginResponse,
    createTableRequest,
    createTableResponse,
    numOfMessageTypes
  };
  
  struct LoginResponse : public DataProtocol::Header
  {
    byte_t pwSalt[32];
    byte_t authSalt[32];
    byte_t signature[32];
  };

  struct User : public DataProtocol::Entity
  {
    byte_t pwSalt[32];
    byte_t pwHash[32];
  };
};
