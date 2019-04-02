#pragma once


#if !defined(PRAGMA_)
#define PRAGMA_(x) _Pragma( #x )
#endif

#if !defined(QUOTE2_)
  #define QUOTE2_( x ) # x
#endif
#if !defined(QUOTE)
  #define QUOTE( x ) QUOTE2_( x )
#endif

#if COLOR_MESSAGES
#define TODO( x ) PRAGMA_(message ( __FILE__ ":" QUOTE(__LINE__) " : \e[31mTODO : " x "\e[0m" ))
#define NOTE( x ) PRAGMA_(message ( __FILE__ ":" QUOTE(__LINE__) " : \e[32mNOTE : " x  "\e[0m"))
#else
#define TODO( x ) PRAGMA_(message ( __FILE__ ":" QUOTE(__LINE__) " : TODO : " x ))
#define NOTE( x ) PRAGMA_(message ( __FILE__ ":" QUOTE(__LINE__) " : NOTE : " x ))
#endif
