
#pragma once

#include <nstd/Buffer.h>

class ClientHandler;
class Table;
class TableFile;

class WorkerJob
{
public:
  WorkerJob(ClientHandler& clientHandler, Table& table, TableFile& tableFile, const void* request, size_t size) : valid(true), clientHandler(clientHandler), table(table), tableFile(tableFile), requestBuffer((const byte_t*)request, size) {}

  void_t invalidate() {valid = false;}
  bool_t isValid() const {return valid;}

  ClientHandler& getClientHandler() {return clientHandler;}
  Table& getTable() {return table;}
  const Buffer& getRequestData() const {return requestBuffer;}
  Buffer& getRequestData() {return requestBuffer;}
  const Buffer& getResponseData() const {return responseBuffer;}
  Buffer& getResponseData() {return responseBuffer;}
  TableFile& getTableFile() {return tableFile;}

private:
  bool_t valid;
  ClientHandler& clientHandler;
  Table& table;
  TableFile& tableFile;
  Buffer requestBuffer;
  Buffer responseBuffer;
};
