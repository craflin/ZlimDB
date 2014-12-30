
#include <nstd/Console.h>
#include <nstd/Debug.h>
#include <nstd/Math.h>

#include "Tools/TableFile.h"
#include "Tools/Sha256.h"

#include "WorkerThread.h"
#include "WorkerJob.h"

WorkerThread::WorkerThread(Socket& socket)
{
  this->socket.swap(socket);
}

uint_t WorkerThread::main(void_t* param)
{
  WorkerThread* thread = (WorkerThread*)param;
  Socket& socket = thread->socket;
  
  for(;;)
  {
    size_t read = 0;
    WorkerJob* workerJob;
    do
    {
      size_t i = socket.recv((byte_t*)&workerJob + read, sizeof(workerJob) - read);
      if(i <= 0)
      {
        if(i == 0)
          return 0;
        Console::errorf("error: Could not receive from worker socket: %s", (const char_t*)socket.getErrorString());
        return 1;
      }
      read += i;
    } while(read < sizeof(workerJob));

    thread->currentWorkerJob = workerJob;
    const DataProtocol::Header* header = (const DataProtocol::Header*)(const byte_t*)workerJob->getRequestData();
    thread->handleMessage(*header);

    size_t sent = 0;
    do
    {
      size_t i = socket.send((const byte_t*)&workerJob, sizeof(workerJob));
      if(i <= 0)
      {
        if(i == 0)
          return 0;
        Console::errorf("error: Could not send to worker socket: %s", (const char_t*)socket.getErrorString());
        return 1;
      }
      sent += i;
    } while(sent < sizeof(workerJob));
  }

  return 0;
}

void_t WorkerThread::handleMessage(const DataProtocol::Header& header)
{
  switch((DataProtocol::MessageType)header.messageType)
  {
  case InternalProtocol::loginRequest:
    handleLogin(header);
    break;
  case InternalProtocol::createTableRequest:
    handleCreateTable(header);
    break;
  case DataProtocol::addRequest:
    handleAdd((DataProtocol::AddRequest&)header);
    break;
  case DataProtocol::queryRequest:
    handleQuery((DataProtocol::QueryRequest&)header);
  default:
    ASSERT(false);
    break;
  }
}

void_t WorkerThread::handleLogin(const DataProtocol::Header& header)
{
  TableFile& tableFile = currentWorkerJob->getTableFile();
  Buffer userBuffer;
  if(!tableFile.get(1, userBuffer) || userBuffer.size() < sizeof(InternalProtocol::User))
    return sendErrorResponse(header.requestId, DataProtocol::invalidLogin);
  const InternalProtocol::User* user = (const InternalProtocol::User*)(const byte_t*)userBuffer;

  Buffer& responseBuffer = currentWorkerJob->getResponseData();
  responseBuffer.resize(sizeof(InternalProtocol::LoginResponse));
  InternalProtocol::LoginResponse* response = (InternalProtocol::LoginResponse*)(byte_t*)responseBuffer;
  response->flags = 0;
  response->size = sizeof(*response);
  response->messageType = InternalProtocol::loginResponse;
  response->requestId = header.requestId;
  Memory::copy(&response->pwHash, &user->pwHash, sizeof(user->pwHash));
  Memory::copy(&response->pwSalt, &user->pwSalt, sizeof(user->pwSalt));
  for(uint16_t* i = (uint16_t*)response->authSalt, * end = (uint16_t*)(response->authSalt + sizeof(response->authSalt)); i < end; ++i)
      *i = Math::random();
  Sha256::hmac(response->authSalt, sizeof(response->authSalt), response->pwHash, sizeof(response->pwHash), response->signature);
}

void_t WorkerThread::handleCreateTable(const DataProtocol::Header& header)
{
  TableFile& tableFile = currentWorkerJob->getTableFile();
  String tableName;
  tableName.attach((const char_t*)(&header + 1), header.size - sizeof(header));
  if(!tableFile.create(tableName))
    return sendErrorResponse(header.requestId, DataProtocol::Error::couldNotOpenFile);

  Buffer& responseBuffer = currentWorkerJob->getResponseData();
  responseBuffer.resize(sizeof(DataProtocol::Header));
  DataProtocol::Header* response = (DataProtocol::Header*)(byte_t*)responseBuffer;
  response->flags = 0;
  response->size = sizeof(*response);
  response->messageType = InternalProtocol::createTableResponse;
  response->requestId = header.requestId;
}

void_t WorkerThread::handleAdd(const DataProtocol::AddRequest& add)
{
  // todo
}

void_t WorkerThread::handleQuery(const DataProtocol::QueryRequest& query)
{
  TableFile& tableFile = currentWorkerJob->getTableFile();
  switch(query.type)
  {
  case DataProtocol::QueryRequest::all:
    {
      uint32_t requestId = query.requestId;
      Buffer& responseBuffer = currentWorkerJob->getResponseData();
      uint64_t blockId;
      if(query.param == 0)
      {
        if(!tableFile.getFirstCompressedBlock(blockId, responseBuffer, sizeof(DataProtocol::Header)))
          return sendErrorResponse(requestId, DataProtocol::Error::couldNotReadFile);
      }
      else if(!tableFile.getNextCompressedBlock(query.param, blockId, responseBuffer, sizeof(DataProtocol::Header)))
        return sendErrorResponse(requestId, DataProtocol::Error::couldNotReadFile);
      DataProtocol::Header* response = (DataProtocol::Header*)(byte_t*)responseBuffer;
      response->flags = DataProtocol::Header::compressed;
      if(tableFile.hasNextCompressedBlock(blockId))
        response->flags |= DataProtocol::Header::fragmented;
      response->messageType = DataProtocol::queryResponse;
      response->requestId = requestId;
      response->size = responseBuffer.size();
      ((DataProtocol::QueryRequest&)query).param = blockId;
      break;
    }
  case DataProtocol::QueryRequest::byId:
    break;
  case DataProtocol::QueryRequest::sinceId:
    break;
  case DataProtocol::QueryRequest::sinceTime:
    break;
  }
  // todo 
}

void_t WorkerThread::sendErrorResponse(uint32_t requestId, DataProtocol::Error error)
{
  Buffer& responseBuffer = currentWorkerJob->getResponseData();
  responseBuffer.resize(sizeof(DataProtocol::ErrorResponse));
  DataProtocol::ErrorResponse* response = (DataProtocol::ErrorResponse*)(byte_t*)responseBuffer;
  response->size = sizeof(response);
  response->messageType = DataProtocol::errorResponse;
  response->requestId = requestId;
  response->error = error;
}
