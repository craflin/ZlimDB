
#include <nstd/Console.h>

void_t testTableFile();

int_t main(int_t argc, char_t* argv[])
{
  Console::printf(_T("%s\n"), _T("Testing..."));

  testTableFile();

  Console::printf(_T("%s\n"), _T("done"));
  return 0;
}
