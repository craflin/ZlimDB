
#include <nstd/Console.h>
#include <nstd/Time.h>
#include <nstd/Directory.h>
#include <nstd/Math.h>

#include "Tools/Sha256.h"

#include "ServerHandler.h"
#include "ClientHandler.h"
#include "WorkerHandler.h"
#include "WorkerThread.h"
#include "Table.h"
#include "Subscription.h"

ServerHandler::ServerHandler(Server& server) : server(server), nextTableId(100) {}

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
    if((isDir && !loadTables(filePath)) || (!isDir && !loadTable(filePath)))
        return false;
  }
  if(tables.isEmpty()) // add default user
  {
    String tableName("users/root/.user");
    ClientProtocol::User user;
    ClientProtocol::setEntityHeader(user.entity, 1, Time::time(), sizeof(user));
    for(uint16_t* i = (uint16_t*)user.pw_salt, * end = (uint16_t*)(user.pw_salt + sizeof(user.pw_salt)); i < end; ++i)
      *i = Math::random();
    Sha256::hmac(user.pw_salt, sizeof(user.pw_salt), (const byte_t*)"root", 4, user.pw_hash);
    Table& table = createTable(tableName);
    if(!table.create(&user.entity))
    {
      removeTable(table);
      return false;
    }
  }
  return true;
}

bool_t ServerHandler::loadTable(const String& path)
{
  uint32_t id = nextTableId++;
  File::Time fileTime;
  if(!File::time(path, fileTime))
    return false;
  Table* table = new Table(id, fileTime.creationTime, path);
  if(!table->open())
  {
    delete table;
    return false;
  }
  tables.append(id, table);
  tablesByName.append(table->getName(), table);
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

Table& ServerHandler::createTable(const String& name)
{
  uint32_t id = nextTableId++;
  Table* table = new Table(id, Time::time(), name);
  tables.append(id, table);
  tablesByName.append(table->getName(), table);
  return *table;
}

void_t ServerHandler::removeTable(Table& table)
{
  tables.remove(table.getId());
  tablesByName.remove(table.getName());
  delete &table;
}

Table* ServerHandler::findTable(uint32_t id) const
{
  Table* table = *tables.find(id);
  if(table && table->isValid())
    return table;
  return 0;
}

Table* ServerHandler::findTable(const String& name) const
{
  Table* table = *tablesByName.find(name);
  if(table && table->isValid())
    return table;
  return 0;
}

void_t ServerHandler::acceptedClient(Server::Client& client, uint16_t localPort)
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

WorkerJob& ServerHandler::createWorkerJob(ClientHandler& clientHandler, Table& table, const void* data, size_t size)
{
  WorkerJob* workerJob = new WorkerJob(clientHandler, table, table.getTableFile(), data, size);
  WorkerHandler* workerHandler = table.getWorkerHandler();
  if(!workerHandler)
  {
    workerHandler = workerHandlers.front();
    workerHandlers.removeFront();
    workerHandlers.append(workerHandler);
    table.setWorkerHandler(workerHandler);
    workerHandler->addWorkerJob(*workerJob);
  }
  else
  {
    workerHandler->addWorkerJob(*workerJob);
    increaseWorkerHandlerRank(*workerHandler);
  }
  table.addWorkerJob(*workerJob);
  clientHandler.addWorkerJob(*workerJob);
  return *workerJob;
}

void_t ServerHandler::removeWorkerJob(WorkerJob& workerJob)
{
  Table& table = workerJob.getTable();
  ClientHandler* clientHandler = workerJob.getClientHandler();
  WorkerHandler& workerHandler = *table.getWorkerHandler();
  table.removeWorkerJob(workerJob);
  if(clientHandler)
    clientHandler->removeWorkerJob(workerJob);
  workerHandler.removeWorkerJob(workerJob);
  decreaseWorkerHandlerRank(workerHandler);
  if(table.getLoad() == 0)
  {
    table.setWorkerHandler(0);
    if(!table.isValid())
      removeTable(table);
  }
  delete &workerJob;
}

Subscription& ServerHandler::createSubscription(ClientHandler& clientHandler, Table& table)
{
  Subscription& subscription = *new Subscription(clientHandler, table);
  clientHandler.addSubscription(subscription);
  table.addSubscription(subscription);
  return subscription;
}

void_t ServerHandler::removeSubscription(Subscription& subscription)
{
  ClientHandler* clientHandler = subscription.getClientHandler();
  if(clientHandler)
    clientHandler->removeSubscription(subscription);
  subscription.getTable().removeSubscription(subscription);
  delete &subscription;
}
