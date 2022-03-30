// nullstring.h
//
// A class that can reprpesent string-with-NULL (special value not representable by a string value).

#pragma once


// A class for striings that can hold NULL values - a value that is separate from all possible string.
class NullString
{
 protected:
  std::auto_ptr<std::string> mStrPtr;

 public:
  NullString() : mStrPtr(nullptr)
  {
  }
  NullString(const char* str, size_t length) : mStrPtr(new std::string(str, length))
  {
  }
  explicit NullString(const std::string& str) : mStrPtr(new std::string(str)
  {
  }
  const char* str() const
  {
    return mStr;
  }
  const char* end() const
  {
    return mStr + mLength;
  }
  size_t length() const
  {
    return mLength;
  }
  std::string toString() const
  {
    return std::string(mStr, mLength);
  }
  bool eq(char ch) const
  {
    return mLength == 1 && mStr[0] == ch;
  }
  bool eq(const ConstString& rhs) const
  {
    return mLength == rhs.mLength && !memcmp(mStr, rhs.mStr, mLength);
  }
  ConstString& rtrimZero()
  {
    for (; mLength && mStr[mLength - 1] == '\0'; mLength--)
    {
    }
    return *this;
  }
};

