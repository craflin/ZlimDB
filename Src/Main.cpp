
#include <nstd/Console.h>
#include <nstd/Debug.h>
#include <nstd/Directory.h>
#include <nstd/Error.h>
#include <nstd/Process.h>

#include "Tools/Server.h"
#include "ServerHandler.h"

int_t main(int_t argc, char_t* argv[])
{
  static const uint16_t port = 13211;
  String logFile;
  String dataDir("Data");

  // parse parameters
  {
    Process::Option options[] = {
        {'b', "daemon", Process::argumentFlag | Process::optionalFlag},
        {'c', "dir", Process::argumentFlag},
        {'h', "help", Process::optionFlag},
    };
    Process::Arguments arguments(argc, argv, options);
    int_t character;
    String argument;
    while(arguments.read(character, argument))
      switch(character)
      {
      case 'b':
        logFile = argument.isEmpty() ? String("zlimdb.log") : argument;
        break;
      case 'c':
        dataDir = argument;
        break;
      case '?':
        Console::errorf("Unknown option: %s.\n", (const char_t*)argument);
        return -1;
      case ':':
        Console::errorf("Option %s required an argument.\n", (const char_t*)argument);
        return -1;
      default:
        Console::errorf("Usage: %s [-b] [-c <dir>]\n\
  -b, --daemon[=<file>]   Detach from calling shell and write output to <file>.\n\
  -c, --dir <dir>         Use <dir> as data directory. (default: ./Data)\n", argv[0]);
        return -1;
      }
  }

  // daemonize process
#ifndef _WIN32
  if(!logFile.isEmpty())
  {
    Console::printf("Starting as daemon...\n");
    if(!Process::daemonize(logFile))
    {
      Console::errorf("error: Could not daemonize process: %s\n", (const char_t*)Error::getErrorString());
      return -1;
    }
  }
#endif

  // change working directory
  if(!Directory::exists(dataDir) && !Directory::create(dataDir))
  {
    Console::errorf("error: Could not create data directory: %s\n", (const tchar_t*)Error::getErrorString());
    return -1;
  }
  if(!dataDir.isEmpty() && !Directory::change(dataDir))
  {
    Console::errorf("error: Could not enter data directory: %s\n", (const tchar_t*)Error::getErrorString());
    return -1;
  }

  //
  Console::printf("Starting zlimdb server...\n", port);

  // initialize listen server
  Server server;
  ServerHandler serverHandler(server);
  server.setListener(&serverHandler);

  // listen on server port
  if(!server.listen(port))
  {
    Console::errorf("error: Could not listen on port %hu: %s\n", port, (const char_t*)Socket::getErrorString());
    return -1;
  }
  Console::printf("Listening on port %hu.\n", port);

  // load table data
  if(!serverHandler.loadTables())
  {
    Console::errorf("error: Could not load tables: %s\n", (const char_t*)Error::getErrorString());
    return -1;
  }

  // create worker threads
  for(int i = 0; i < 4; ++i) // todo: make 4 a setting or parameter
    if(!serverHandler.createWorkerThread())
    {
      Console::errorf("error: Could not create worker thread: %s\n", (const char_t*)Error::getErrorString());
      return -1;
    }

  // run select loop
  if(!server.process())
  {
    Console::errorf("error: Could not run select loop: %s\n", (const char_t*)Socket::getErrorString());
    return -1;
  }

  return 0;
}

