/* Copyright (C) 2021 MariaDB Corporation.

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

#ifndef GENERICPARSER_H
#define GENERICPARSER_H

#include "conststring.h"

namespace genericparser
{
using utils::ConstString;

class Tokenizer
{
 protected:
  const char* mStr;
  const char* mEnd;

 public:
  explicit Tokenizer(const char* str, size_t length) : mStr(str), mEnd(str + length)
  {
  }
  size_t length() const
  {
    return mEnd - mStr;
  }
  const char* ptr() const
  {
    return mStr;
  }
  bool isSpace() const
  {
    return mStr < mEnd && mStr[0] == ' ';
  }
  bool isDigit() const
  {
    return mStr < mEnd && mStr[0] >= '0' && mStr[0] <= '9';
  }
  bool isChar(char chr) const
  {
    return mStr < mEnd && mStr[0] == chr;
  }
  bool isAnyCharOf(char chr0, char chr1)
  {
    return mStr < mEnd && (mStr[0] == chr0 || mStr[0] == chr1);
  }

  ConstString tokenSpaces()
  {
    if (!isSpace())
      return ConstString(nullptr, 0);
    const char* start = mStr;
    for (; isSpace(); mStr++)
    {
    }
    return ConstString(start, mStr - start);
  }
  ConstString tokenDigits()
  {
    if (!isDigit())
      return ConstString(nullptr, 0);
    const char* start = mStr;
    for (; isDigit(); mStr++)
    {
    }
    return ConstString(start, mStr - start);
  }
  ConstString tokenChar(char chr)
  {
    if (!isChar(chr))
      return ConstString(nullptr, 0);
    return ConstString(mStr++, 1);
  }
  ConstString tokenAnyCharOf(char chr0, char chr1)
  {
    if (!isAnyCharOf(chr0, chr1))
      return ConstString(nullptr, 0);
    return ConstString(mStr++, 1);
  }
};

class Parser : public Tokenizer
{
 protected:
  bool mSyntaxError;

 public:
  explicit Parser(const char* str, size_t length) : Tokenizer(str, length), mSyntaxError(false)
  {
  }
  explicit Parser(const std::string& str) : Parser(str.data(), str.length())
  {
  }
  Parser& skipLeadingSpaces()
  {
    tokenSpaces();
    return *this;
  }
  bool syntaxError() const
  {
    return mSyntaxError;
  }
  bool setSyntaxError()
  {
    mSyntaxError = true;
    return false;
  }
  const char* tokStart() const
  {
    return mStr;
  }
  const ConstString tokStartConstString() const
  {
    return ConstString(mStr, 0);
  }

  // A helper class template to set the parser syntax error
  // if A returned isNull() after parsing.

  template <class A>
  class SetSyntaxErrorOnNull : public A
  {
   public:
    SetSyntaxErrorOnNull(Parser* p) : A(p)
    {
      if (A::isNull())
        p->setSyntaxError();
    }
  };

  // A helper class template for a rule in the form:        <res> := [ <a> ]

  template <class A>
  class Opt : public A
  {
   public:
    explicit Opt(const A& rhs) : A(rhs)
    {
    }
    explicit Opt(Parser* p) : A(p)
    {
      if (A::isNull() && !p->syntaxError())
        A::operator=(A::empty(p));
    }
  };

  // Letters in the class template names below mean:
  // U - unused - the result class does not have the source class inside
  // D - derive - the result class derives from the source class
  // M - member - the result class adds the source class as a member

  // M - mandatory - this part is required during parse time
  // O - optional  - this part is optional during parse time

  // A helper class template for a rule in the form:        <res> := <a> <b>
  // i.e. both parts are mandatory at parse time
  // The value of <a> is not important, and is created
  // only temporary on the stack.
  // Only the value of <b> is important.
  // Example:
  //    <period> <unsigned integer>

  template <class A, class B>
  class UD2MM : public B
  {
   public:
    explicit UD2MM(Parser* p) : B(A(p).isNull() ? B() : SetSyntaxErrorOnNull<B>(p))
    {
    }
    explicit UD2MM(const B& b) : B(b)
    {
    }
    explicit UD2MM() : B()
    {
    }
    bool isNull() const
    {
      return B::isNull();
    }
  };

  // A helper class template for a rule in the form:       <res> := <a> <b>
  // i.e. both parts are mandatory at parse time.
  template <class A, class B>
  class DD2MM : public A, public B
  {
   public:
    // Sets syntax error if <a> was not followed by <b>
    explicit DD2MM(Parser* p) : A(p), B(A::isNull() ? B() : SetSyntaxErrorOnNull<B>(p))
    {
    }
    explicit DD2MM(const A& a, const B& b) : A(b), B(b)
    {
    }
  };

  // A helper class template for a rule in the form:       <res> := <a> [ <b> ]
  // i.e. <a> is mandatory, <b> is optional at parse time.
  template <class A, class B>
  class DD2MO : public A, public B
  {
   public:
    explicit DD2MO(Parser* p) : A(p), B(A::isNull() ? B() : B(p))
    {
    }
    explicit DD2MO(const A& a, const B& b) : A(a), B(b)
    {
    }
  };

  // A helper class template for a rule in the form:       <res> := <a> [ <b> ]
  // i.e. <a> is mandatory, <b> is optional at parse time.
  // The value of <a> is not important and is not included
  // into the target class, e.g.
  // <period> [ <unsigned integer> ]
  template <class A, class B>
  class UD2MO : public B
  {
   public:
    explicit UD2MO(Parser* p) : B(A(p).isNull() ? B() : B(p))
    {
    }
    explicit UD2MO(const B& b) : B(b)
    {
    }
    explicit UD2MO() : B()
    {
    }
  };

  // A helper class template for a rule in the form:       <res> := <a> [ <b> ]
  // i.e. <a> is mandatory, <b> is optional at parse time.
  // The result class derives from "A".
  // The result class puts "B" as a member.
  template <class A, class B>
  class DM2MO : public A
  {
   protected:
    B mB;

   public:
    explicit DM2MO(Parser* p) : A(p), mB(A::isNull() ? B() : B(p))
    {
    }
  };

  // A helper class template for a rule in the form:       <res> := [ <a> ] <b>
  // i.e. <a> is optional, <b> is mandatory at parse time.
  template <class A, class B>
  class DD2OM : public Opt<A>, public B
  {
   public:
    explicit DD2OM(Parser* p) : Opt<A>(p), B(p)
    {
      if (B::isNull())
        p->setSyntaxError();
    }
    explicit DD2OM() : Opt<A>(A())
    {
    }
    explicit DD2OM(const A& a, const B& b) : Opt<A>(a), B(b)
    {
    }
  };

  // A helper class template for a rule in the form:       <res> := a | b
  template <class Container, class A, class B>
  class Choice2 : public Container
  {
   public:
    explicit Choice2(const A& a) : Container(a)
    {
    }
    explicit Choice2(const B& b) : Container(b)
    {
    }
    explicit Choice2(Parser* p) : Container(A(p))
    {
      if (Container::isNull() && !p->syntaxError())
        *this = Choice2(B(p));
    }
  };
};

}  // namespace genericparser

#endif  // GENERICPARSER_H
