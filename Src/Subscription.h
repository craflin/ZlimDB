
#pragma once

#include <nstd/Base.h>

class ClientHandler;
class Table;

class Subscription
{
public:
  Subscription(ClientHandler& clientHandler, Table& table) : synched(false), maxEntityId(0), clientHandler(&clientHandler), table(table) {}

  void_t detachClientHandler() {clientHandler = 0;}
  ClientHandler* getClientHandler() {return clientHandler;}
  Table& getTable() {return table;}

  void_t setSynced() {synched = true;}
  bool_t isSynced() const {return synched;}
  void_t setMaxEntityId(uint64_t entityId) {maxEntityId = entityId;}
  uint64_t getMaxEntityId() const {return maxEntityId;}

private:
  bool_t synched;
  uint64_t maxEntityId;
  ClientHandler* clientHandler;
  Table& table;
};
