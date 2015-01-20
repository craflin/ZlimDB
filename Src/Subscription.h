
#pragma once

#include <nstd/Base.h>

class ClientHandler;
class Table;

class Subscription
{
public:
  Subscription(ClientHandler& clientHandler, Table& table) : synched(false), clientHandler(&clientHandler), table(table) {}

  void_t detachClientHandler() {clientHandler = 0;}
  ClientHandler* getClientHandler() {return clientHandler;}
  Table& getTable() {return table;}

  void_t setSynced() {synched = true;}
  bool_t isSynced() const {return synched;}

private:
  bool_t synched;
  ClientHandler* clientHandler;
  Table& table;
};
