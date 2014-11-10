
#pragma once

#include <nstd/Base.h>

class User
{
  byte_t pwSalt[32];
  byte_t pwHash[32];
};
