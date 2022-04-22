// nullstring.h
//
// A class that can reprpesent string-with-NULL (special value not representable by a string value).

#pragma once

#include <memory>
#include "exceptclasses.h"

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
  void assign(const std::string& newVal)
  {
    mStrPtr.reset(new std::string(newVal));
  }
  // XXX: here we implement what Row::equals expects.
  //      It is not SQL-NULL-handling compatible, please beware.
  bool operator ==(const NullString& a) const
  {
    if (mStrPtr && !b.mStrPtr)
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
};
} // namespace utils.

