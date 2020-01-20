#pragma once

#include <sys/socket.h>
#include <sys/un.h>
#include "iosocket.h"

namespace messageqcpp
{

class UnixSocket : public Socket
{
public:
    UnixSocket();
    UnixSocket(const UnixSocket &);
    virtual ~UnixSocket();

    UnixSocket & operator=(const UnixSocket &);

    void open();
    const SBS read(const struct timespec *timeout = 0, bool *isTimeOut = NULL,
        Stats *stats = NULL) const;
    void write_raw(const ByteStream &msg, Stats *stats = NULL) const;
    void write(const ByteStream &msg, Stats *stats = NULL);
    void write(SBS msg, Stats *stats = NULL);
    void close();

    void bind(const struct sockaddr *serv_addr);
    void listen(int backlog = 5);
    const IOSocket accept(const struct timespec *timeout = 0);
    void connect(const sockaddr *serv_addr);

    const bool isOpen() const;
    const SocketParms socketParms() const;
    void socketParms(const SocketParms &);
    void sa(const sockaddr *);
    Socket *clone() const;
    void connectionTimeout(const struct ::timespec *);
    void syncProto(bool use);
    const int getConnectionNum() const;
    const std::string addr2String() const;
    const bool isSameAddr(const Socket *) const;
    bool isConnected() const;
    bool hasData() const;

private:
    const char *prefix = "\0columnstore-";

    sockaddr_un sun;
    //sockaddr fake_sa;
    SocketParms parms;
};

}
