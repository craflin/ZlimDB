
#include <nstd/Debug.h>
#include <nstd/File.h>

#include <Tools/TableFile.h>

void_t testTableFile()
{
  // test create and open
  {
    TableFile file(123);
    ASSERT(file.getTableId() == 123);
    ASSERT(file.create("test.test"));
    ASSERT(file.isOpen());
    ASSERT(file.getLastId() == 0);
    ASSERT(file.getTimeOffset() == 0);
    file.close();
    ASSERT(!file.isOpen());
    ASSERT(file.open("test.test"));
    ASSERT(file.isOpen());
    ASSERT(file.getLastId() == 0);
    ASSERT(file.getTimeOffset() == 0);
    file.close();
    ASSERT(File::unlink("test.test"));
  }

  // todo: test remove bottom to top

  // todo: test remove top to bottom

  // todo: test add and random remove

}
