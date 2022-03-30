// nullstring.h
//
// A class that can reprpesent string-with-NULL (special value not representable by a string value).

#pragma once


// A class for striings that can hold NULL values - a value that is separate from all possible string.
class NullString
{
 protected:
  std::unique_ptr<std::string> mStrPtr;

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
  bool eq(char ch) const
  {
    return length() == 1 && str()[0] == ch;
  }
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
  ConstString& rtrimZero()
  {
    return *this; // TODO
  }
};

