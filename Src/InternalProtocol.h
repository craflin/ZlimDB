
#pragma once

#include "DataProtocol.h"

class InternalProtocol
{
public:
  struct LoginResponse : public DataProtocol::LoginResponse
  {
    byte_t signature[32];
  };

  struct User : public DataProtocol::Entity
  {
    byte_t pwSalt[32];
    byte_t pwHash[32];
  };
};
