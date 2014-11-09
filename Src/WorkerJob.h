
#pragma once

#include <nstd/Buffer.h>

class ClientHandler;
class Table;
class TableFile;

class WorkerJob
{
public:
  WorkerJob(ClientHandler& clientHandler, Table& table, const void* data, size_t size, TableFile& tableFile) : valid(true), clientHandler(clientHandler), table(table), buffer((const byte_t*)data, size), tableFile(tableFile) {}

  void_t invalidate() {valid = false;}
  bool_t isValid() const {return valid;}

  ClientHandler& getClientHandler() {return clientHandler;}
  Table& getTable() {return table;}
  const Buffer& getData() const {return buffer;}
  TableFile& getTableFile() {return tableFile;}

private:
  bool_t valid;
  ClientHandler& clientHandler;
  Table& table;
  Buffer buffer;
  TableFile& tableFile;
};
