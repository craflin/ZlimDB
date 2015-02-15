
#pragma once

#include "ClientProtocol.h"

class WorkerProtocol
{
public:
  struct LoginResponse : public ClientProtocol::LoginResponse
  {
    byte_t signature[32];
  };
};
