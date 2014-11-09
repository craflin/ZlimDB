
#include <nstd/Console.h>
#include <nstd/Time.h>
#include <nstd/Directory.h>

#include "ServerHandler.h"
#include "ClientHandler.h"
#include "WorkerHandler.h"
#include "WorkerThread.h"
#include "Table.h"

ServerHandler::ServerHandler(Server& server) : server(server), nextTableId(DataProtocol::numOfTableIds) {}

ServerHandler::~ServerHandler()
{
  for(HashMap<WorkerHandler*, WorkerThread*>::Iterator i = workerThreads.begin(), end = workerThreads.end(); i != end; ++i)
    delete *i;
  for(HashSet<WorkerHandler*>::Iterator i = workerHandlers.begin(), end = workerHandlers.end(); i != end; ++i)
    delete * i;
  for(HashSet<ClientHandler*>::Iterator i = clientHandlers.begin(), end = clientHandlers.end(); i != end; ++i)
    delete *i;
  for(HashMap<uint32_t, Table*>::Iterator i = tables.begin(), end = tables.end(); i != end; ++i)
    delete *i;
}

bool_t ServerHandler::loadTables(const String& path)
{
  Directory dir;
  if(!dir.open(path, String(), false))
    return false;
  String fileName;
  bool isDir;
  while(dir.read(fileName, isDir))
  {
    String filePath = path.isEmpty() ? fileName : path + "/" + fileName;
    if((isDir && !loadTables(filePath)) || (fileName.endsWith(".table") && !loadTable(filePath)))
        return false;
  }
  return true;
}

bool_t ServerHandler::loadTable(const String& path)
{
  uint32_t id;
  if(path == ".users.table")
    id = DataProtocol::usersTable;
  else
    id = nextTableId++;
  File::Time fileTime;
  if(!File::time(path, fileTime))
    return false;
  Table* table = new Table(*this, id, fileTime.creationTime, path);
  if(!table->open())
  {
    delete table;
    return false;
  }
  tables.append(id, table);
  return true;
}

bool_t ServerHandler::createWorkerThread()
{
  Socket workerSocket;
  Server::Client* client = server.pair(workerSocket);
  if(!client)
    return false;
  WorkerThread* workerThread = new WorkerThread(workerSocket);
  if(!workerThread->start())
  {
    client->close();
    return false;
  }
  WorkerHandler* workerHandler = new WorkerHandler(*this, *client);
  client->setListener(workerHandler);
  workerThreads.append(workerHandler, workerThread);
  workerHandlers.append(workerHandler);
  return true;
}

void_t ServerHandler::decreaseWorkerHandlerRank(WorkerHandler& workerHandler)
{
  if(workerHandlers.front() == &workerHandler)
    return;
  size_t load = workerHandler.getLoad();
  if(load == 0)
  {
    workerHandlers.remove(&workerHandler);
    workerHandlers.prepend(&workerHandler);
    return;
  }
  HashSet<WorkerHandler*>::Iterator it = workerHandlers.find(&workerHandler);
  HashSet<WorkerHandler*>::Iterator itPrev = it;
  --itPrev;
  WorkerHandler* workerHandlerPrev = *itPrev;
  if(load < workerHandlerPrev->getLoad())
  {
    workerHandlers.remove(it);
    workerHandlers.insert(itPrev, &workerHandler);
  }
}

void_t ServerHandler::increaseWorkerHandlerRank(WorkerHandler& workerHandler)
{
  if(workerHandlers.back() == &workerHandler)
    return;
  HashSet<WorkerHandler*>::Iterator it = workerHandlers.find(&workerHandler);
  HashSet<WorkerHandler*>::Iterator itNext = it;
  ++itNext;
  WorkerHandler* workerHandlerNext = *itNext;
  if(workerHandler.getLoad() > workerHandlerNext->getLoad())
  {
    workerHandlers.remove(itNext);
    workerHandlers.insert(it, workerHandlerNext);
  }
}

Table* ServerHandler::createTable(uint32_t id, const String& name)
{
  if(tables.find(id) != tables.end())
    return 0;
  Table* table = new Table(*this, id, Time::time(), name);
  tables.append(id, table);
  return table;
}

void_t ServerHandler::removeTable(Table& table)
{
  tables.remove(table.getId());
  delete &table;
}

Table* ServerHandler::findTable(uint32_t id) const
{
  Table* table = *tables.find(id);
  if(table && table->isValid())
    return table;
  return 0;
}

void_t ServerHandler::acceptedClient(Server::Client& client, uint32_t addr, uint16_t port)
{
  ClientHandler* clientHandler = new ClientHandler(*this, client);
  client.setListener(clientHandler);
  clientHandlers.append(clientHandler);
}

void_t ServerHandler::closedClient(Server::Client& client)
{
  {
    ClientHandler* clientHandler = (ClientHandler*)client.getListener();
    HashSet<ClientHandler*>::Iterator it = clientHandlers.find(clientHandler);
    if(it != clientHandlers.end())
    {
      clientHandlers.remove(it);
      delete clientHandler;
      return;
    }
  }
  WorkerHandler* workerHandler = (WorkerHandler*)client.getListener();
  HashMap<WorkerHandler*, WorkerThread*>::Iterator it = workerThreads.find(workerHandler);
  delete *it;
  workerThreads.remove(it);
  workerHandlers.remove(workerHandler);
  delete workerHandler;
}
