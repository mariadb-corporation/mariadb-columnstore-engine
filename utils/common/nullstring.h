// nullstring.h
//
// A class that can reprpesent string-with-NULL (special value not representable by a string value).

#pragma once

#include <iostream>
#include <memory>
#include "exceptclasses.h"
#include "conststring.h"

namespace utils
{
// A class for striings that can hold NULL values - a value that is separate from all possible string.
class NullString
{
 protected:
  std::shared_ptr<std::string> mStrPtr;

 public:
  NullString() : mStrPtr(nullptr)
  {
  }
  NullString(const char* str, size_t length) : mStrPtr(new std::string(str, length))
  {
  }
  explicit NullString(const std::string& str) : mStrPtr(new std::string(str))
  {
  }
  explicit NullString(const ConstString& str) : mStrPtr()
  {
    assign((const uint8_t*)str.str(), str.length());
  }
  ConstString toConstString() const
  {
    if (isNull())
    {
      return ConstString(nullptr, 0);
    }
    return ConstString(mStrPtr->c_str(), mStrPtr->length());
  }
  const char* str() const
  {
    if (!mStrPtr)
    {
      return nullptr;
    }
    return mStrPtr->c_str();
  }
  const char* end() const
  {
    if (!mStrPtr)
    {
      return nullptr;
    }
    return str() + length();
  }
  size_t length() const
  {
    if (!mStrPtr)
    {
      return 0;
    }
    return mStrPtr->length();
  }
  std::string toString() const
  {
    idbassert(mStrPtr);
    return std::string(*mStrPtr);
  }
  // "unsafe" means we do not check for NULL.
  // it should be used after we know there is data in NullString.
  const std::string& unsafeStringRef() const
  {
    idbassert(mStrPtr);
    return (*mStrPtr);
  }
  bool eq(char ch) const
  {
    return length() == 1 && str()[0] == ch;
  }
  // this is SQL-like NULL handling equality. NULL will not be equal to anything, including NULL.
  bool eq(const NullString& rhs) const
  {
    if (!rhs.mStrPtr)
    {
      return false;
    }
    if (!mStrPtr)
    {
      return false;
    }
    return *mStrPtr == *(rhs.mStrPtr);
  }
  NullString& rtrimZero()
  {
    return *this; // TODO
  }
  // this can be used to safely get a string value, with default value for NULL substitution.
  // it does not raise anything and provides some nonsensical default value for you that will be
  // easy to find.
  std::string safeString(const char* defaultValue = "<<<no default value for null provided>>>") const
  {
    if (!mStrPtr)
    {
      return std::string(defaultValue);
    }
    return std::string(*mStrPtr);
  }
  bool isNull() const
  {
    return !mStrPtr;
  }
  void resize(size_t newSize, char pad)
  {
    if (mStrPtr)
    {
      mStrPtr->resize(newSize, pad);
    }
  }
  void dropString()
  {
    mStrPtr.reset();
  }
  void assign(const uint8_t* p, size_t len)
  {
    if (!p)
    {
      mStrPtr.reset();
      return;
    }
    mStrPtr.reset(new std::string((const char*)p, len));
  }
  void assign(const std::string& newVal)
  {
    mStrPtr.reset(new std::string(newVal));
  }
  // XXX: here we implement what Row::equals expects.
  //      It is not SQL-NULL-handling compatible, please beware.
  bool operator ==(const NullString& a) const
  {
    if (!mStrPtr && !a.mStrPtr)
    {
      return true;
    }
    if (!mStrPtr)
    {
      return false;
    }
    if (!a.mStrPtr)
    {
      return false;
    }
    // fall to std::string equality.
    return (*mStrPtr) == (*a.mStrPtr);
  }
  bool operator <(const NullString& a) const
  {
    // order NULLs first.
    if (isNull() > a.isNull())
    {
      return true;
    }
    if (isNull() < a.isNull())
    {
      return false;
    }
    if (!isNull())
    {
      // fall to std::string equality.
      return (*mStrPtr) < (*a.mStrPtr);
    }
    return false; // both are NULLs.
  }
  bool operator >(const NullString& a) const
  {
    return a < (*this);
  }
};

} // namespace utils.

std::istream& operator >>(std::istream& in, utils::NullString& ns)
{
  uint8_t isNull;
  in.read((char*)(&isNull), sizeof(isNull));
  if (!isNull)
  {
    uint16_t len;
    char t[32768];
    in.read((char*)(&len), sizeof(len));
    in.read(t, len);
    ns.assign(t, len);
  }
  else
  {
    ns.dropString();
  }
  return in;
}

std::ostream& operator <<(std::ostream& out, const utils::NullString& ns)
{
  uint8_t isNull = ns.isNull();
  out.write((char*)(&isNull), sizeof(isNull));
  if (!isNull)
  {
    idbassert(ns.length() < 32768);
    uint16_t len = ns.length();
    out.write((char*)(&len), sizeof(len));
    out.write(ns.str(), ns.length());
  }
  return out;
}

