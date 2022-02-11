/* Copyright (C) 2020 MariaDB Corporation

   This program is free software; you can redistribute it and/or
   modify it under the terms of the GNU General Public License
   as published by the Free Software Foundation; version 2 of
   the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
   MA 02110-1301, USA. */

#ifndef PIPE_H_INCLUDED
#define PIPE_H_INCLUDED

/*
  A helper class to hold the file descriptors returned from a pipe() call.
*/
class Pipe
{
  int fd[2];

 public:
  Pipe()
  {
    fd[0] = 0;
    fd[1] = 0;
  }
  ~Pipe()
  {
    close();
  }
  bool is_open_for_read() const
  {
    return fd[0] > 0;
  }
  bool is_open_for_write() const
  {
    return fd[1] > 0;
  }
  bool open()
  {
    return ::pipe(fd) == -1;
  }
  bool init()  // TODO: remove this
  {
    return open();
  }
  ssize_t read(char* str, size_t nbytes)
  {
    return ::read(fd[0], str, nbytes);
  }
  ssize_t readtm(const struct timeval& tv, void* buf, size_t nbytes)
  {
    fd_set rfds;
    FD_ZERO(&rfds);
    FD_SET(fd[0], &rfds);
    struct timeval tmptv = tv;
    int retval = select(fd[0] + 1, &rfds, NULL, NULL, &tmptv);
    if (retval == -1)
      return -1;
    if (!retval)
    {
      errno = ETIME;
      return -1;
    }
    return ::read(fd[0], buf, nbytes);
  }
  ssize_t write(const char* str, size_t nbytes)
  {
    return ::write(fd[1], str, nbytes);
  }
  ssize_t write(const std::string& str)
  {
    return write(str.data(), str.length());
  }
  void close()
  {
    if (fd[0])
    {
      ::close(fd[0]);
      fd[0] = 0;
    }
    if (fd[1])
    {
      ::close(fd[1]);
      fd[1] = 0;
    }
  }
};

#endif  // PIPE_H_INCLUDED
