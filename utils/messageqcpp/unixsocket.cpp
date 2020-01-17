#include "unixsocket.h"
#include "socketclosed.h"

#include <poll.h>

using namespace std;

namespace messageqcpp
{

UnixSocket::UnixSocket() : parms(AF_UNIX, SOCK_STREAM, 0)
{
    memset(&sun, 0, sizeof(sun));
    memset(&fake_sa, 0, sizeof(fake_sa));
    sun.sun_family = AF_UNIX;
}

UnixSocket::UnixSocket(const UnixSocket &rhs)
{
    parms = rhs.parms;
    memcpy(&fake_sa, &rhs.fake_sa, sizeof(fake_sa));
    memcpy(&sun, &rhs.sun, sizeof(sun));
}

UnixSocket::~UnixSocket()
{
}

UnixSocket & UnixSocket::operator=(const UnixSocket &rhs)
{
    if (this == &rhs)
    {
        cout << "UnixSocket copying itself to itself??" << endl;
        return *this;
    }
    parms = rhs.parms;
    memcpy(&fake_sa, &rhs.fake_sa, sizeof(fake_sa));
    memcpy(&sun, &rhs.sun, sizeof(sun));
    return *this;
}

void UnixSocket::open()
{
    cout << "open" << endl;
    int fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
    parms.sd(fd);
}

void UnixSocket::close()
{
    if (parms.sd() >= 0)
    {
        cout << "closing " << parms.sd() << endl;
        ::close(parms.sd());
        parms.sd(-1);
    }
}

void UnixSocket::bind(const sockaddr *serv_addr)
{
    if (parms.sd() < 0)
        throw runtime_error("UnixSocket::bind(): no socket");
    if (serv_addr->sa_family != AF_INET || serv_addr->sa_family != PF_INET)
        throw runtime_error("UnixSocket::bind(): got not-a-sockaddr_in");
    sockaddr_in *sin = (sockaddr_in *) serv_addr;
    uint16_t port = ntohs(sin->sin_port);

    memset(&sun, 0, sizeof(sun));
    sun.sun_family = AF_UNIX;
    sprintf(&sun.sun_path[1], "%s%d", &prefix[1], port);

    cout << "binding to " << &sun.sun_path[1] << endl;

    int err = ::bind(parms.sd(), (sockaddr *) &sun, sizeof(sun));
    if (err < 0)
    {
        int l_errno = errno;
        char buf[80];
        throw runtime_error(string("UnixSocket::bind(): got ") + strerror_r(l_errno, buf, 80));
    }
}

void UnixSocket::listen(int num)
{
    cout << "listen " << num << endl;
    int err = ::listen(parms.sd(), num);
    if (err < 0)
    {
        int l_errno = errno;
        char buf[80];
        throw runtime_error(string("UnixSocket::listen(): got ") + strerror_r(l_errno, buf, 80));
    }

}

void UnixSocket::connect(const sockaddr *serv_addr)
{
    if (parms.sd() < 0)
        throw runtime_error("UnixSocket::connect(): no socket");
    if (serv_addr->sa_family != AF_INET || serv_addr->sa_family != PF_INET)
        throw runtime_error("UnixSocket::connect(): got not-a-sockaddr_in");
    sockaddr_in *sin = (sockaddr_in *) serv_addr;
    uint16_t port = ntohs(sin->sin_port);

    memset(&sun, 0, sizeof(sun));
    sun.sun_family = AF_UNIX;
    sprintf(&sun.sun_path[1], "%s%d", &prefix[1], port);
    cout << "connecting to " << &sun.sun_path[1] << endl;
    int err = ::connect(parms.sd(), (sockaddr *) &sun, sizeof(sun));
    if (err < 0)
        throw runtime_error(string("UnixSocket::connect(): failed to connect on ") + &sun.sun_path[1]);
    cout << "US::connect OK: fd = " << parms.sd() << endl;
}

const IOSocket UnixSocket::accept(const struct timespec *timeout)
{
    cout << "accept" << endl;
    IOSocket ret(new UnixSocket());

    if (timeout)
    {
        struct pollfd pfd[1];
        pfd[0].fd = parms.sd();
        pfd[0].events = POLLIN;

        int msecs = timeout->tv_sec * 1000 + timeout->tv_nsec / 1000000;

        if (poll(pfd, 1, msecs) != 1 || (pfd[0].revents & POLLIN) == 0 ||
                pfd[0].revents & (POLLERR | POLLHUP | POLLNVAL))
            return ret;
    }

    struct sockaddr remote;
    socklen_t sl = sizeof(remote);
    int err, clientfd;

    do
    {
        clientfd = ::accept(parms.sd(), &remote, &sl);
        err = errno;
    } while (clientfd < 0 && (err == EINTR || err == ERESTART ||
        err == ECONNABORTED));

    if (clientfd < 0)
    {
        char buf[80];
        string msg = string("UnixSocket::accept(): error: ") + strerror_r(err, buf, 80);
        throw runtime_error(msg);
    }
    cout << "accepted connection" << endl;

    SocketParms sp = ret.socketParms();
    sp.sd(clientfd);
    ret.socketParms(sp);
    ret.sa(&remote);
    cout << "US::accept OK: fd = " << clientfd << endl;
    return ret;
}

/* This currently assumes the remote is writing complete msgs, not bothering
with message deliminters, or comprehensive timeout checking yet. */
const SBS UnixSocket::read(const timespec *timeout, bool *isTimeout, Stats *stats) const
{
    cout << "read" << endl;
    SBS ret;
    ssize_t err;
    uint64_t msecs;
    uint32_t msglen;
    int l_errno;

    pollfd pfd[1];
    pfd[0].fd = parms.sd();
    pfd[0].events = POLLIN;
    pfd[0].revents = 0;

    if (timeout)
    {
        msecs = timeout->tv_sec * 1000 + timeout->tv_nsec / 1000000;
        cout << "msecs = " << msecs << " fd = " << parms.sd() << endl;
        err = ::poll(pfd, 1, msecs);
        if (err < 0)
        {
            l_errno = errno;
            ostringstream oss;
            char buf[80];
            oss << "UnixSocket::read: I/O error1: " << strerror_r(l_errno, buf, 80);
            throw runtime_error(oss.str());
        }
        else if (pfd[0].revents & (POLLERR | POLLHUP | POLLNVAL))
        {
            if (pfd[0].revents & POLLNVAL)
                cout << "got POLLNVAL" << endl;
            else if (pfd[0].revents & POLLERR)
                cout << "got POLLERR" << endl;
            else cout << "got POLLHUP" << endl;
            throw runtime_error("UnixSocket::read: poll revents has an error");
        }
        if (err == 0)  // timeout
        {
            if (isTimeout)
                *isTimeout = true;
            return SBS(new ByteStream(0));
        }
    }

    while (err != sizeof(msglen))
    {
        cout << "read msglen loop" << endl;
        err = ::read(parms.sd(), &msglen, sizeof(msglen));
        if (err < 0)
        {
            if (errno == EINTR)
                continue;
            else
            {
                l_errno = errno;
                ostringstream oss;
                char buf[80];
                oss << "UnixSocket::read: I/O error2: " << strerror_r(l_errno, buf, 80);
                throw runtime_error(oss.str());
            }
        }
        else if (err == 0)
        {
            if (timeout)
                throw SocketClosed("UnixSocket::read(): Remote is closed");
            else
                return SBS(new ByteStream(0));
        }
        else if (err != sizeof(msglen))
            throw runtime_error("fixme, unixsocket failed to read 4 bytes at once");
    }
    cout << " - read msglen = " << msglen << endl;

    if (stats)
        stats->dataRecvd(sizeof(msglen));

    ret.reset(new ByteStream(msglen));
    uint8_t *buf = ret->getInputPtr();
    size_t readSoFar = 0;
    uint64_t sum = 0;

    while (readSoFar < msglen)
    {
        cout << "main read loop, read so far = " << readSoFar << " msglen = " << msglen << endl;
        err = ::read(parms.sd(), &buf[readSoFar], msglen - readSoFar);
        if (err < 0)
        {
            if (errno == EINTR)
                continue;
            else
            {
                l_errno = errno;
                ostringstream oss;
                char buf[80];
                oss << "UnixSocket::read: I/O error3: " << strerror_r(l_errno, buf, 80);
                throw runtime_error(oss.str());
            }
        }
        else if (err == 0)
        {
            if (stats)
                stats->dataRecvd(readSoFar);
            if (timeout)
                throw SocketClosed("UnixSocket::read: Remote is closed");
            else
                return SBS(new ByteStream(0));
        }
        else
            readSoFar += err;
    }

    for (int i = 0; i < msglen; i++)
        sum += buf[i];

    cout << "main read loop done sum = " << sum << endl;
    if (stats)
        stats->dataRecvd(msglen);
    ret->advanceInputPtr(msglen);
    return ret;
}

void UnixSocket::write_raw(const ByteStream &msg, Stats *stats) const
{
    size_t toSend = msg.length(), sent = 0;
    const uint8_t *buf = msg.buf();
    ssize_t err;
    int l_errno;
    char errbuf[80];

    uint64_t sum = 0;
    for (int i = 0; i < toSend; i++)
        sum += buf[i];
    cout << "  -- sum is " << sum << endl;

    while (sent < toSend)
    {
        err = ::write(parms.sd(), &buf[sent], toSend - sent);
        if (err < 0)
        {
            if (errno == EINTR)
                continue;
            l_errno = errno;
            if (stats)
                stats->dataSent(sent);
            throw runtime_error(string("UnixSocket:write_raw: got ") +
                strerror_r(l_errno, errbuf, 80));
        }
        sent += err;
    }
    if (stats)
        stats->dataSent(sent);
}

void UnixSocket::write(SBS msg, Stats *stats)
{
    write(*msg, stats);
}

void UnixSocket::write(const ByteStream &msg, Stats *stats)
{
    cout << "write bytestream len = " << msg.length() << endl;
    ssize_t err = 0;
    uint32_t size = msg.length();
    char buf[80];

    while (err != sizeof(size))
    {
        err = ::write(parms.sd(), &size, sizeof(size));
        if (err < 0)   // todo, robustify...
        {
            if (errno == EINTR)
                continue;
            int l_errno = errno;
            throw runtime_error(string("UnixSocket:write: got ") +
                strerror_r(l_errno, buf, 80));
        }
    }
    cout << "wrote len" << endl;

    if (stats)
        stats->dataSent(sizeof(size));
    write_raw(msg, stats);
    cout << "wrote msg" << endl;
}

const bool UnixSocket::isOpen() const
{
    return parms.isOpen();
}

const SocketParms UnixSocket::socketParms() const
{
    return parms;
}

void UnixSocket::socketParms(const SocketParms &other)
{
    parms = other;
}

void UnixSocket::sa(const sockaddr *other)
{
    memcpy(&fake_sa, other, sizeof(fake_sa));
    if (other->sa_family == AF_UNIX)
        memcpy(&sun, other, sizeof(sun));
    else if (other->sa_family == PF_INET || other->sa_family == AF_INET)
    {
        //cout << "Not an AF_UNIX socket" << endl;
        sockaddr_in *sin = (sockaddr_in *) other;
        uint16_t port = ntohs(sin->sin_port);
        sun.sun_family = AF_UNIX;
        sprintf(&sun.sun_path[1], "%s%d", &prefix[1], port);
    }
}

Socket * UnixSocket::clone() const
{
    return new UnixSocket(*this);
}

void UnixSocket::connectionTimeout(const struct ::timespec *)
{
    // TODO: not implemented yet
}

void UnixSocket::syncProto(bool use)
{
    // doubt this needs to be implemented for unix sockets
}

const int UnixSocket::getConnectionNum() const
{
    return parms.sd();
}

const string UnixSocket::addr2String() const
{
    return string("UnixSocket on ") + &sun.sun_path[1];
}

const bool UnixSocket::isSameAddr(const Socket *s) const
{
    const UnixSocket *other = dynamic_cast<const UnixSocket *>(s);
    if (!other)
    {
        cout << "compared to not-a-unixsocket?" << endl;
        return false;
    }
    else
        return strncmp(&sun.sun_path[1], &(other->sun.sun_path[1]),
            sizeof(sun.sun_path) - 1) == 0;
}

bool UnixSocket::isConnected() const
{
    // lifted from InetStreamSocket::isConnected()

    int error = 0;
    socklen_t len = sizeof(error);
    int retval = getsockopt(parms.sd(), SOL_SOCKET, SO_ERROR, &error, &len);

    if (error || retval)
        return false;

    struct pollfd pfd[1];
    pfd[0].fd = parms.sd();
    pfd[0].events = POLLIN;
    pfd[0].revents = 0;

    error = ::poll(pfd, 1, 0);

    if ((error < 0) || (pfd[0].revents & (POLLHUP | POLLNVAL | POLLERR)))
        return false;

    return true;
}

bool UnixSocket::hasData() const
{
    char buf;
    int err = ::recv(parms.sd(), &buf, 1, MSG_PEEK | MSG_DONTWAIT);
    return (err == 1);
}

}
