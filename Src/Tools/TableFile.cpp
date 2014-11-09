

#include "TableFile.h"

bool_t TableFile::open(const String& fileName)
{
  return file.open(fileName);
}
