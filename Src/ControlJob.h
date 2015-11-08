
#pragma once

#include <nstd/Buffer.h>

class ClientHandler;
class Table;

class ControlJob
{
public:
  ControlJob(ClientHandler& clientHandler, Table& table, uint32_t id, const void* request, size_t size) : clientHandler(clientHandler), table(table), id(id), requestBuffer((const byte_t*)request, size) {}

  uint32_t getId() const {return id;}

  ClientHandler& getClientHandler() {return clientHandler;}
  Table& getTable() {return table;}

  const Buffer& getRequestData() const {return requestBuffer;}
  Buffer& getRequestData() {return requestBuffer;}

private:
  ClientHandler& clientHandler;
  Table& table;
  uint32_t id;
  Buffer requestBuffer;
};
