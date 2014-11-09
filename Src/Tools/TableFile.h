
#pragma once

#include <nstd/File.h>

class TableFile
{
public:
  bool_t open(const String& file);
  bool_t add(uint64_t id, const byte_t* data, uint32_t size);
  bool_t remove(uint64_t id);
  bool_t get(uint64_t id, byte_t* data, uint32_t& size);
  bool_t getCompressedBlock(uint64_t id, byte_t* data, uint32_t& size);

private:
  File file;
};
