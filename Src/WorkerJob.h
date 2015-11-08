
#pragma once

#include <nstd/Buffer.h>

class ClientHandler;
class Table;
class TableFile;

class WorkerJob
{
public:
  WorkerJob(ClientHandler& clientHandler, Table& table, TableFile& tableFile, const void* request, size_t size, uint64_t param1) : clientHandler(clientHandler), table(table), tableFile(tableFile), requestBuffer((const byte_t*)request, size), param1(param1) {}

  ClientHandler& getClientHandler() {return clientHandler;}
  Table& getTable() {return table;}
  const Buffer& getRequestData() const {return requestBuffer;}
  Buffer& getRequestData() {return requestBuffer;}
  const Buffer& getResponseData() const {return responseBuffer;}
  Buffer& getResponseData() {return responseBuffer;}
  TableFile& getTableFile() {return tableFile;}
  void_t setParam1(uint64_t param1) {this->param1 = param1;}
  uint64_t getParam1() const {return param1;}

private:
  ClientHandler& clientHandler;
  Table& table;
  TableFile& tableFile;
  Buffer requestBuffer;
  Buffer responseBuffer;
  uint64_t param1;
};
