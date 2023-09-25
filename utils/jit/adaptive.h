//
// Created by nuc on 24-5-25.
//

#ifndef MARIADB_ADAPTIVE_H
#define MARIADB_ADAPTIVE_H

class JitAdaptive
{
 public:
  // 构造函数
  JitAdaptive();

  // 析构函数
  ~JitAdaptive();

  // EvaluateResult 方法
  static bool EvaluateResult(int count, int time, int remainCount)
  {
    return false;
  }

 private:
  // 私有成员变量和方法可以根据需要添加
};
#endif  // MARIADB_ADAPTIVE_H
