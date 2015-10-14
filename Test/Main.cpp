
#include <nstd/Console.h>
#include <nstd/Time.h>

void_t testTableFile();
void_t testClient(const char_t* argv0);

int_t main(int_t argc, char_t* argv[])
{
  int64_t startTime = Time::ticks();
  Console::printf(_T("%s\n"), _T("Testing..."));

  testTableFile();
  testClient(argv[0]);

  Console::printf(_T("%s (%lld ms)\n"), _T("done"), Time::ticks() - startTime);
  return 0;
}
