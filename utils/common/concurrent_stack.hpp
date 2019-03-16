#pragma once

#include <atomic>


namespace concurrent{

  /*
   * thread-safe  LIFO stack
  */
  template <typename _value_t> class stack {
  public:

    using value_type = _value_t;

    stack() : _root(nullptr){}

    ~stack(){
      while (_root.load()){
        auto pNode = _root.load();
        if (_root.compare_exchange_strong(pNode, pNode->_next)){
          delete pNode;
        }
      }
    }

    stack(stack&& src) : _root(src._root.load()){
      src._root.store(nullptr);
    }

    stack& operator=(stack&& src){
      if (&src == this) return *this;
      std::swap(_root, src._root);
    }

    stack(const stack&) = delete;

    stack& operator=(const stack&) = delete;

    bool try_pop(value_type& oRet){
      auto oTmp = _root.load();
      if (!oTmp) return false;
      if (!_root.compare_exchange_strong(oTmp, oTmp->_next)){
        return false;
      }
      oRet = oTmp->_value;
      delete oTmp;
      return true;
    }

    void push(const value_type& value){
      typename node::pointer pNode = new node(nullptr, value);
      for(;;){
        pNode->_next = _root.load();
        if (_root.compare_exchange_strong(pNode->_next, pNode)){
          break;
        }
      }
    }

    value_type pop(){
      for(;;){
        auto oTemp = _root.load();
        if (_root.compare_exchange_strong(oTemp, oTemp->_next)) {
          value_type oRet(oTemp->_value);
          delete oTemp;
          return oRet;
        }
      }
    }

    size_t unsafe_count() const  {
      size_t iRet = 0;
      for (const node * tmp = _root.load(); tmp; tmp = tmp->_next, ++iRet);
      return iRet;
    }

  private:

    struct node{
      using pointer = node *;
      using atomic_ptr = std::atomic<pointer>;
      node() = delete;
      node(const node&) = delete;
      node(node * pNext, const value_type& value) : _next(pNext), _value(value){}
      node(node&&)=delete;
      node& operator=(const node&) = delete;
      node& operator=(node&&)=delete;
      node * _next;
      value_type _value;
    };

    typename node::atomic_ptr _root;

  };
}