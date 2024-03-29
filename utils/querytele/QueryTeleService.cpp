/**
 * Autogenerated by Thrift Compiler (0.9.1)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
#include "QueryTeleService.h"

namespace querytele
{
uint32_t QueryTeleService_postQuery_args::read(::apache::thrift::protocol::TProtocol* iprot)
{
  uint32_t xfer = 0;
  std::string fname;
  ::apache::thrift::protocol::TType ftype;
  int16_t fid;

  xfer += iprot->readStructBegin(fname);

  using ::apache::thrift::protocol::TProtocolException;

  while (true)
  {
    xfer += iprot->readFieldBegin(fname, ftype, fid);

    if (ftype == ::apache::thrift::protocol::T_STOP)
    {
      break;
    }

    switch (fid)
    {
      case 1:
        if (ftype == ::apache::thrift::protocol::T_STRUCT)
        {
          xfer += this->query.read(iprot);
          this->__isset.query = true;
        }
        else
        {
          xfer += iprot->skip(ftype);
        }

        break;

      default: xfer += iprot->skip(ftype); break;
    }

    xfer += iprot->readFieldEnd();
  }

  xfer += iprot->readStructEnd();

  return xfer;
}

uint32_t QueryTeleService_postQuery_args::write(::apache::thrift::protocol::TProtocol* oprot) const
{
  uint32_t xfer = 0;
  xfer += oprot->writeStructBegin("QueryTeleService_postQuery_args");

  xfer += oprot->writeFieldBegin("query", ::apache::thrift::protocol::T_STRUCT, 1);
  xfer += this->query.write(oprot);
  xfer += oprot->writeFieldEnd();

  xfer += oprot->writeFieldStop();
  xfer += oprot->writeStructEnd();
  return xfer;
}

uint32_t QueryTeleService_postQuery_pargs::write(::apache::thrift::protocol::TProtocol* oprot) const
{
  uint32_t xfer = 0;
  xfer += oprot->writeStructBegin("QueryTeleService_postQuery_pargs");

  xfer += oprot->writeFieldBegin("query", ::apache::thrift::protocol::T_STRUCT, 1);
  xfer += (*(this->query)).write(oprot);
  xfer += oprot->writeFieldEnd();

  xfer += oprot->writeFieldStop();
  xfer += oprot->writeStructEnd();
  return xfer;
}

uint32_t QueryTeleService_postQuery_result::read(::apache::thrift::protocol::TProtocol* iprot)
{
  uint32_t xfer = 0;
  std::string fname;
  ::apache::thrift::protocol::TType ftype;
  int16_t fid;

  xfer += iprot->readStructBegin(fname);

  using ::apache::thrift::protocol::TProtocolException;

  while (true)
  {
    xfer += iprot->readFieldBegin(fname, ftype, fid);

    if (ftype == ::apache::thrift::protocol::T_STOP)
    {
      break;
    }

    xfer += iprot->skip(ftype);
    xfer += iprot->readFieldEnd();
  }

  xfer += iprot->readStructEnd();

  return xfer;
}

uint32_t QueryTeleService_postQuery_result::write(::apache::thrift::protocol::TProtocol* oprot) const
{
  uint32_t xfer = 0;

  xfer += oprot->writeStructBegin("QueryTeleService_postQuery_result");

  xfer += oprot->writeFieldStop();
  xfer += oprot->writeStructEnd();
  return xfer;
}

uint32_t QueryTeleService_postQuery_presult::read(::apache::thrift::protocol::TProtocol* iprot)
{
  uint32_t xfer = 0;
  std::string fname;
  ::apache::thrift::protocol::TType ftype;
  int16_t fid;

  xfer += iprot->readStructBegin(fname);

  using ::apache::thrift::protocol::TProtocolException;

  while (true)
  {
    xfer += iprot->readFieldBegin(fname, ftype, fid);

    if (ftype == ::apache::thrift::protocol::T_STOP)
    {
      break;
    }

    xfer += iprot->skip(ftype);
    xfer += iprot->readFieldEnd();
  }

  xfer += iprot->readStructEnd();

  return xfer;
}

uint32_t QueryTeleService_postStep_args::read(::apache::thrift::protocol::TProtocol* iprot)
{
  uint32_t xfer = 0;
  std::string fname;
  ::apache::thrift::protocol::TType ftype;
  int16_t fid;

  xfer += iprot->readStructBegin(fname);

  using ::apache::thrift::protocol::TProtocolException;

  while (true)
  {
    xfer += iprot->readFieldBegin(fname, ftype, fid);

    if (ftype == ::apache::thrift::protocol::T_STOP)
    {
      break;
    }

    switch (fid)
    {
      case 1:
        if (ftype == ::apache::thrift::protocol::T_STRUCT)
        {
          xfer += this->query.read(iprot);
          this->__isset.query = true;
        }
        else
        {
          xfer += iprot->skip(ftype);
        }

        break;

      default: xfer += iprot->skip(ftype); break;
    }

    xfer += iprot->readFieldEnd();
  }

  xfer += iprot->readStructEnd();

  return xfer;
}

uint32_t QueryTeleService_postStep_args::write(::apache::thrift::protocol::TProtocol* oprot) const
{
  uint32_t xfer = 0;
  xfer += oprot->writeStructBegin("QueryTeleService_postStep_args");

  xfer += oprot->writeFieldBegin("query", ::apache::thrift::protocol::T_STRUCT, 1);
  xfer += this->query.write(oprot);
  xfer += oprot->writeFieldEnd();

  xfer += oprot->writeFieldStop();
  xfer += oprot->writeStructEnd();
  return xfer;
}

uint32_t QueryTeleService_postStep_pargs::write(::apache::thrift::protocol::TProtocol* oprot) const
{
  uint32_t xfer = 0;
  xfer += oprot->writeStructBegin("QueryTeleService_postStep_pargs");

  xfer += oprot->writeFieldBegin("query", ::apache::thrift::protocol::T_STRUCT, 1);
  xfer += (*(this->query)).write(oprot);
  xfer += oprot->writeFieldEnd();

  xfer += oprot->writeFieldStop();
  xfer += oprot->writeStructEnd();
  return xfer;
}

uint32_t QueryTeleService_postStep_result::read(::apache::thrift::protocol::TProtocol* iprot)
{
  uint32_t xfer = 0;
  std::string fname;
  ::apache::thrift::protocol::TType ftype;
  int16_t fid;

  xfer += iprot->readStructBegin(fname);

  using ::apache::thrift::protocol::TProtocolException;

  while (true)
  {
    xfer += iprot->readFieldBegin(fname, ftype, fid);

    if (ftype == ::apache::thrift::protocol::T_STOP)
    {
      break;
    }

    xfer += iprot->skip(ftype);
    xfer += iprot->readFieldEnd();
  }

  xfer += iprot->readStructEnd();

  return xfer;
}

uint32_t QueryTeleService_postStep_result::write(::apache::thrift::protocol::TProtocol* oprot) const
{
  uint32_t xfer = 0;

  xfer += oprot->writeStructBegin("QueryTeleService_postStep_result");

  xfer += oprot->writeFieldStop();
  xfer += oprot->writeStructEnd();
  return xfer;
}

uint32_t QueryTeleService_postStep_presult::read(::apache::thrift::protocol::TProtocol* iprot)
{
  uint32_t xfer = 0;
  std::string fname;
  ::apache::thrift::protocol::TType ftype;
  int16_t fid;

  xfer += iprot->readStructBegin(fname);

  using ::apache::thrift::protocol::TProtocolException;

  while (true)
  {
    xfer += iprot->readFieldBegin(fname, ftype, fid);

    if (ftype == ::apache::thrift::protocol::T_STOP)
    {
      break;
    }

    xfer += iprot->skip(ftype);
    xfer += iprot->readFieldEnd();
  }

  xfer += iprot->readStructEnd();

  return xfer;
}

uint32_t QueryTeleService_postImport_args::read(::apache::thrift::protocol::TProtocol* iprot)
{
  uint32_t xfer = 0;
  std::string fname;
  ::apache::thrift::protocol::TType ftype;
  int16_t fid;

  xfer += iprot->readStructBegin(fname);

  using ::apache::thrift::protocol::TProtocolException;

  while (true)
  {
    xfer += iprot->readFieldBegin(fname, ftype, fid);

    if (ftype == ::apache::thrift::protocol::T_STOP)
    {
      break;
    }

    switch (fid)
    {
      case 1:
        if (ftype == ::apache::thrift::protocol::T_STRUCT)
        {
          xfer += this->query.read(iprot);
          this->__isset.query = true;
        }
        else
        {
          xfer += iprot->skip(ftype);
        }

        break;

      default: xfer += iprot->skip(ftype); break;
    }

    xfer += iprot->readFieldEnd();
  }

  xfer += iprot->readStructEnd();

  return xfer;
}

uint32_t QueryTeleService_postImport_args::write(::apache::thrift::protocol::TProtocol* oprot) const
{
  uint32_t xfer = 0;
  xfer += oprot->writeStructBegin("QueryTeleService_postImport_args");

  xfer += oprot->writeFieldBegin("query", ::apache::thrift::protocol::T_STRUCT, 1);
  xfer += this->query.write(oprot);
  xfer += oprot->writeFieldEnd();

  xfer += oprot->writeFieldStop();
  xfer += oprot->writeStructEnd();
  return xfer;
}

uint32_t QueryTeleService_postImport_pargs::write(::apache::thrift::protocol::TProtocol* oprot) const
{
  uint32_t xfer = 0;
  xfer += oprot->writeStructBegin("QueryTeleService_postImport_pargs");

  xfer += oprot->writeFieldBegin("query", ::apache::thrift::protocol::T_STRUCT, 1);
  xfer += (*(this->query)).write(oprot);
  xfer += oprot->writeFieldEnd();

  xfer += oprot->writeFieldStop();
  xfer += oprot->writeStructEnd();
  return xfer;
}

uint32_t QueryTeleService_postImport_result::read(::apache::thrift::protocol::TProtocol* iprot)
{
  uint32_t xfer = 0;
  std::string fname;
  ::apache::thrift::protocol::TType ftype;
  int16_t fid;

  xfer += iprot->readStructBegin(fname);

  using ::apache::thrift::protocol::TProtocolException;

  while (true)
  {
    xfer += iprot->readFieldBegin(fname, ftype, fid);

    if (ftype == ::apache::thrift::protocol::T_STOP)
    {
      break;
    }

    xfer += iprot->skip(ftype);
    xfer += iprot->readFieldEnd();
  }

  xfer += iprot->readStructEnd();

  return xfer;
}

uint32_t QueryTeleService_postImport_result::write(::apache::thrift::protocol::TProtocol* oprot) const
{
  uint32_t xfer = 0;

  xfer += oprot->writeStructBegin("QueryTeleService_postImport_result");

  xfer += oprot->writeFieldStop();
  xfer += oprot->writeStructEnd();
  return xfer;
}

uint32_t QueryTeleService_postImport_presult::read(::apache::thrift::protocol::TProtocol* iprot)
{
  uint32_t xfer = 0;
  std::string fname;
  ::apache::thrift::protocol::TType ftype;
  int16_t fid;

  xfer += iprot->readStructBegin(fname);

  using ::apache::thrift::protocol::TProtocolException;

  while (true)
  {
    xfer += iprot->readFieldBegin(fname, ftype, fid);

    if (ftype == ::apache::thrift::protocol::T_STOP)
    {
      break;
    }

    xfer += iprot->skip(ftype);
    xfer += iprot->readFieldEnd();
  }

  xfer += iprot->readStructEnd();

  return xfer;
}

void QueryTeleServiceClient::postQuery(const QueryTele& query)
{
  send_postQuery(query);
  recv_postQuery();
}

void QueryTeleServiceClient::send_postQuery(const QueryTele& query)
{
  int32_t cseqid = 0;
  oprot_->writeMessageBegin("postQuery", ::apache::thrift::protocol::T_CALL, cseqid);

  QueryTeleService_postQuery_pargs args;
  args.query = &query;
  args.write(oprot_);

  oprot_->writeMessageEnd();
  oprot_->getTransport()->writeEnd();
  oprot_->getTransport()->flush();
}

void QueryTeleServiceClient::recv_postQuery()
{
  int32_t rseqid = 0;
  std::string fname;
  ::apache::thrift::protocol::TMessageType mtype;

  iprot_->readMessageBegin(fname, mtype, rseqid);

  if (mtype == ::apache::thrift::protocol::T_EXCEPTION)
  {
    ::apache::thrift::TApplicationException x;
    x.read(iprot_);
    iprot_->readMessageEnd();
    iprot_->getTransport()->readEnd();
    throw x;
  }

  if (mtype != ::apache::thrift::protocol::T_REPLY)
  {
    iprot_->skip(::apache::thrift::protocol::T_STRUCT);
    iprot_->readMessageEnd();
    iprot_->getTransport()->readEnd();
  }

  if (fname.compare("postQuery") != 0)
  {
    iprot_->skip(::apache::thrift::protocol::T_STRUCT);
    iprot_->readMessageEnd();
    iprot_->getTransport()->readEnd();
  }

  QueryTeleService_postQuery_presult result;
  result.read(iprot_);
  iprot_->readMessageEnd();
  iprot_->getTransport()->readEnd();

  return;
}

void QueryTeleServiceClient::postStep(const StepTele& query)
{
  send_postStep(query);
  recv_postStep();
}

void QueryTeleServiceClient::send_postStep(const StepTele& query)
{
  int32_t cseqid = 0;
  oprot_->writeMessageBegin("postStep", ::apache::thrift::protocol::T_CALL, cseqid);

  QueryTeleService_postStep_pargs args;
  args.query = &query;
  args.write(oprot_);

  oprot_->writeMessageEnd();
  oprot_->getTransport()->writeEnd();
  oprot_->getTransport()->flush();
}

void QueryTeleServiceClient::recv_postStep()
{
  int32_t rseqid = 0;
  std::string fname;
  ::apache::thrift::protocol::TMessageType mtype;

  iprot_->readMessageBegin(fname, mtype, rseqid);

  if (mtype == ::apache::thrift::protocol::T_EXCEPTION)
  {
    ::apache::thrift::TApplicationException x;
    x.read(iprot_);
    iprot_->readMessageEnd();
    iprot_->getTransport()->readEnd();
    throw x;
  }

  if (mtype != ::apache::thrift::protocol::T_REPLY)
  {
    iprot_->skip(::apache::thrift::protocol::T_STRUCT);
    iprot_->readMessageEnd();
    iprot_->getTransport()->readEnd();
  }

  if (fname.compare("postStep") != 0)
  {
    iprot_->skip(::apache::thrift::protocol::T_STRUCT);
    iprot_->readMessageEnd();
    iprot_->getTransport()->readEnd();
  }

  QueryTeleService_postStep_presult result;
  result.read(iprot_);
  iprot_->readMessageEnd();
  iprot_->getTransport()->readEnd();

  return;
}

void QueryTeleServiceClient::postImport(const ImportTele& query)
{
  send_postImport(query);
  recv_postImport();
}

void QueryTeleServiceClient::send_postImport(const ImportTele& query)
{
  int32_t cseqid = 0;
  oprot_->writeMessageBegin("postImport", ::apache::thrift::protocol::T_CALL, cseqid);

  QueryTeleService_postImport_pargs args;
  args.query = &query;
  args.write(oprot_);

  oprot_->writeMessageEnd();
  oprot_->getTransport()->writeEnd();
  oprot_->getTransport()->flush();
}

void QueryTeleServiceClient::recv_postImport()
{
  int32_t rseqid = 0;
  std::string fname;
  ::apache::thrift::protocol::TMessageType mtype;

  iprot_->readMessageBegin(fname, mtype, rseqid);

  if (mtype == ::apache::thrift::protocol::T_EXCEPTION)
  {
    ::apache::thrift::TApplicationException x;
    x.read(iprot_);
    iprot_->readMessageEnd();
    iprot_->getTransport()->readEnd();
    throw x;
  }

  if (mtype != ::apache::thrift::protocol::T_REPLY)
  {
    iprot_->skip(::apache::thrift::protocol::T_STRUCT);
    iprot_->readMessageEnd();
    iprot_->getTransport()->readEnd();
  }

  if (fname.compare("postImport") != 0)
  {
    iprot_->skip(::apache::thrift::protocol::T_STRUCT);
    iprot_->readMessageEnd();
    iprot_->getTransport()->readEnd();
  }

  QueryTeleService_postImport_presult result;
  result.read(iprot_);
  iprot_->readMessageEnd();
  iprot_->getTransport()->readEnd();

  return;
}

bool QueryTeleServiceProcessor::dispatchCall(::apache::thrift::protocol::TProtocol* iprot,
                                             ::apache::thrift::protocol::TProtocol* oprot,
                                             const std::string& fname, int32_t seqid, void* callContext)
{
  ProcessMap::iterator pfn;
  pfn = processMap_.find(fname);

  if (pfn == processMap_.end())
  {
    iprot->skip(::apache::thrift::protocol::T_STRUCT);
    iprot->readMessageEnd();
    iprot->getTransport()->readEnd();
    ::apache::thrift::TApplicationException x(::apache::thrift::TApplicationException::UNKNOWN_METHOD,
                                              "Invalid method name: '" + fname + "'");
    oprot->writeMessageBegin(fname, ::apache::thrift::protocol::T_EXCEPTION, seqid);
    x.write(oprot);
    oprot->writeMessageEnd();
    oprot->getTransport()->writeEnd();
    oprot->getTransport()->flush();
    return true;
  }

  (this->*(pfn->second))(seqid, iprot, oprot, callContext);
  return true;
}

void QueryTeleServiceProcessor::process_postQuery(int32_t seqid, ::apache::thrift::protocol::TProtocol* iprot,
                                                  ::apache::thrift::protocol::TProtocol* oprot,
                                                  void* callContext)
{
  void* ctx = NULL;

  if (this->eventHandler_.get() != NULL)
  {
    ctx = this->eventHandler_->getContext("QueryTeleService.postQuery", callContext);
  }

  ::apache::thrift::TProcessorContextFreer freer(this->eventHandler_.get(), ctx,
                                                 "QueryTeleService.postQuery");

  if (this->eventHandler_.get() != NULL)
  {
    this->eventHandler_->preRead(ctx, "QueryTeleService.postQuery");
  }

  QueryTeleService_postQuery_args args;
  args.read(iprot);
  iprot->readMessageEnd();
  uint32_t bytes = iprot->getTransport()->readEnd();

  if (this->eventHandler_.get() != NULL)
  {
    this->eventHandler_->postRead(ctx, "QueryTeleService.postQuery", bytes);
  }

  QueryTeleService_postQuery_result result;

  try
  {
    iface_->postQuery(args.query);
  }
  catch (const std::exception& e)
  {
    if (this->eventHandler_.get() != NULL)
    {
      this->eventHandler_->handlerError(ctx, "QueryTeleService.postQuery");
    }

    ::apache::thrift::TApplicationException x(e.what());
    oprot->writeMessageBegin("postQuery", ::apache::thrift::protocol::T_EXCEPTION, seqid);
    x.write(oprot);
    oprot->writeMessageEnd();
    oprot->getTransport()->writeEnd();
    oprot->getTransport()->flush();
    return;
  }

  if (this->eventHandler_.get() != NULL)
  {
    this->eventHandler_->preWrite(ctx, "QueryTeleService.postQuery");
  }

  oprot->writeMessageBegin("postQuery", ::apache::thrift::protocol::T_REPLY, seqid);
  result.write(oprot);
  oprot->writeMessageEnd();
  bytes = oprot->getTransport()->writeEnd();
  oprot->getTransport()->flush();

  if (this->eventHandler_.get() != NULL)
  {
    this->eventHandler_->postWrite(ctx, "QueryTeleService.postQuery", bytes);
  }
}

void QueryTeleServiceProcessor::process_postStep(int32_t seqid, ::apache::thrift::protocol::TProtocol* iprot,
                                                 ::apache::thrift::protocol::TProtocol* oprot,
                                                 void* callContext)
{
  void* ctx = NULL;

  if (this->eventHandler_.get() != NULL)
  {
    ctx = this->eventHandler_->getContext("QueryTeleService.postStep", callContext);
  }

  ::apache::thrift::TProcessorContextFreer freer(this->eventHandler_.get(), ctx, "QueryTeleService.postStep");

  if (this->eventHandler_.get() != NULL)
  {
    this->eventHandler_->preRead(ctx, "QueryTeleService.postStep");
  }

  QueryTeleService_postStep_args args;
  args.read(iprot);
  iprot->readMessageEnd();
  uint32_t bytes = iprot->getTransport()->readEnd();

  if (this->eventHandler_.get() != NULL)
  {
    this->eventHandler_->postRead(ctx, "QueryTeleService.postStep", bytes);
  }

  QueryTeleService_postStep_result result;

  try
  {
    iface_->postStep(args.query);
  }
  catch (const std::exception& e)
  {
    if (this->eventHandler_.get() != NULL)
    {
      this->eventHandler_->handlerError(ctx, "QueryTeleService.postStep");
    }

    ::apache::thrift::TApplicationException x(e.what());
    oprot->writeMessageBegin("postStep", ::apache::thrift::protocol::T_EXCEPTION, seqid);
    x.write(oprot);
    oprot->writeMessageEnd();
    oprot->getTransport()->writeEnd();
    oprot->getTransport()->flush();
    return;
  }

  if (this->eventHandler_.get() != NULL)
  {
    this->eventHandler_->preWrite(ctx, "QueryTeleService.postStep");
  }

  oprot->writeMessageBegin("postStep", ::apache::thrift::protocol::T_REPLY, seqid);
  result.write(oprot);
  oprot->writeMessageEnd();
  bytes = oprot->getTransport()->writeEnd();
  oprot->getTransport()->flush();

  if (this->eventHandler_.get() != NULL)
  {
    this->eventHandler_->postWrite(ctx, "QueryTeleService.postStep", bytes);
  }
}

void QueryTeleServiceProcessor::process_postImport(int32_t seqid,
                                                   ::apache::thrift::protocol::TProtocol* iprot,
                                                   ::apache::thrift::protocol::TProtocol* oprot,
                                                   void* callContext)
{
  void* ctx = NULL;

  if (this->eventHandler_.get() != NULL)
  {
    ctx = this->eventHandler_->getContext("QueryTeleService.postImport", callContext);
  }

  ::apache::thrift::TProcessorContextFreer freer(this->eventHandler_.get(), ctx,
                                                 "QueryTeleService.postImport");

  if (this->eventHandler_.get() != NULL)
  {
    this->eventHandler_->preRead(ctx, "QueryTeleService.postImport");
  }

  QueryTeleService_postImport_args args;
  args.read(iprot);
  iprot->readMessageEnd();
  uint32_t bytes = iprot->getTransport()->readEnd();

  if (this->eventHandler_.get() != NULL)
  {
    this->eventHandler_->postRead(ctx, "QueryTeleService.postImport", bytes);
  }

  QueryTeleService_postImport_result result;

  try
  {
    iface_->postImport(args.query);
  }
  catch (const std::exception& e)
  {
    if (this->eventHandler_.get() != NULL)
    {
      this->eventHandler_->handlerError(ctx, "QueryTeleService.postImport");
    }

    ::apache::thrift::TApplicationException x(e.what());
    oprot->writeMessageBegin("postImport", ::apache::thrift::protocol::T_EXCEPTION, seqid);
    x.write(oprot);
    oprot->writeMessageEnd();
    oprot->getTransport()->writeEnd();
    oprot->getTransport()->flush();
    return;
  }

  if (this->eventHandler_.get() != NULL)
  {
    this->eventHandler_->preWrite(ctx, "QueryTeleService.postImport");
  }

  oprot->writeMessageBegin("postImport", ::apache::thrift::protocol::T_REPLY, seqid);
  result.write(oprot);
  oprot->writeMessageEnd();
  bytes = oprot->getTransport()->writeEnd();
  oprot->getTransport()->flush();

  if (this->eventHandler_.get() != NULL)
  {
    this->eventHandler_->postWrite(ctx, "QueryTeleService.postImport", bytes);
  }
}

::std::shared_ptr< ::apache::thrift::TProcessor> QueryTeleServiceProcessorFactory::getProcessor(
    const ::apache::thrift::TConnectionInfo& connInfo)
{
  ::apache::thrift::ReleaseHandler<QueryTeleServiceIfFactory> cleanup(handlerFactory_);
  ::std::shared_ptr<QueryTeleServiceIf> handler(handlerFactory_->getHandler(connInfo), cleanup);
  ::std::shared_ptr< ::apache::thrift::TProcessor> processor(new QueryTeleServiceProcessor(handler));
  return processor;
}
}  // namespace querytele
