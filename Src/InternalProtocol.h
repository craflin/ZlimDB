
#pragma once

#include "ClientProtocol.h"

class InternalProtocol // todo: rename WorkerProtocol
{
public:
  struct LoginResponse : public ClientProtocol::LoginResponse
  {
    byte_t signature[32];
  };

  struct User : public ClientProtocol::Entity
  {
    byte_t pwSalt[32];
    byte_t pwHash[32];
  };
};
