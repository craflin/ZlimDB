
#include <nstd/Console.h>
#include <nstd/Debug.h>

#include "WorkerThread.h"
#include "WorkerJob.h"
#include "Tools/TableFile.h"
#include "DataProtocol.h"

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

    const DataProtocol::Header* header = (const DataProtocol::Header*)(const byte_t*)workerJob->getData();
    thread->handleMessage(workerJob->getTableFile(), *header);

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

void_t WorkerThread::handleMessage(TableFile& tableFile, const DataProtocol::Header& header)
{
  switch((DataProtocol::MessageType)header.messageType)
  {
  case DataProtocol::addRequest:
    //{
    //DataProtocol::Add
    //}
    //tableFile.add()
    break;
//  case ADD:
//    channel->add(action->getId(), action->getBuffer())
//  case UPDATE:
//    channel->update(action->getId(), action->getBuffer())
//  case REMOVE:
//    channel->remove(action->getId(), action->getBuffer())
//  case QUERY_BY_ID:
//    channel->queryById(action->getId())
//  case QUERY_BY_AGE:
//    channel->queryByAge(action->getId())
  default:
    ASSERT(false);
    break;
  }
}
