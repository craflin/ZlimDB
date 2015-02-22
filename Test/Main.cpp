
#include <nstd/Console.h>
#include <nstd/Time.h>

void_t testTableFile();

int_t main(int_t argc, char_t* argv[])
{
  timestamp_t startTime = Time::ticks();
  Console::printf(_T("%s\n"), _T("Testing..."));

  testTableFile();

  Console::printf(_T("%s (%lld ms)\n"), _T("done"), Time::ticks() - startTime);
  return 0;
}
