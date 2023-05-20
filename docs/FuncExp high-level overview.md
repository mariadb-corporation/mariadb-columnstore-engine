The following introduction will take this SQL statement as an example:
```SQL
select a+b*c, c from test where a>10;
```

Firstly, according to the link you provided, we can see that the main calculation code starts in BatchPrimitiveProcess::execute() from here:
```c++
  fe2Output.resetRowGroup(baseRid);
  fe2Output.getRow(0, &fe2Out);
  fe2Input->getRow(0, &fe2In);

  for (j = 0; j < outputRG.getRowCount(); j++, fe2In.nextRow())
  {
    if (fe2->evaluate(&fe2In))
    {
      applyMapping(fe2Mapping, fe2In, &fe2Out);
      fe2Out.setRid(fe2In.getRelRid());
      fe2Output.incRowCount();
      fe2Out.nextRow();
    }
  }
```

By calling the `evaluate` method of `fe2`, with a reference of `row` type `fe2In` as the parameter. The execution of the code then enters the `FuncExpWrapper`, which contains the current calculation expression. It mainly performs two steps, which are:

1.  Filtering the conditions in the `where` statement through the functions defined in the `filters` variable. If the `where` condition is met, the calculation proceeds to the next step; otherwise, it will return directly.
2.  Calculating the data in `row` through `expression` and obtaining the final result.

```c++
bool FuncExpWrapper::evaluate(Row* r)
{
  uint32_t i;

  for (i = 0; i < filters.size(); i++)
  // filter the conditions in the where statement
    if (!fe->evaluate(*r, filters[i].get()))
      return false;
  // calculate the row that meets the where condition
  fe->evaluate(*r, rcs);

  return true;
}
```

That is to say, the function that actually performs the calculation is the `evaluate` method pointed to by `fe`. Its parameters are a `row` and a `ReturnedColumn` type array `expression`.
```c++
void FuncExp::evaluate(rowgroup::Row& row, std::vector<execplan::SRCP>& expression)
{
  bool isNull;

  for (uint32_t i = 0; i < expression.size(); i++)
  {
    isNull = false;

    switch (expression[i]->resultType().colDataType)
    {
      case CalpontSystemCatalog::DATE:
      {
        int64_t val = expression[i]->getIntVal(row, isNull);

        // @bug6061, workaround date_add always return datetime for both date and datetime
        if (val & 0xFFFFFFFF00000000)
          val = (((val >> 32) & 0xFFFFFFC0) | 0x3E);

        if (isNull)
          row.setUintField<4>(DATENULL, expression[i]->outputIndex());
        else
          row.setUintField<4>(val, expression[i]->outputIndex());

        break;
      }

 ....
}
```

In this function, each expression will be looped through, and the calculation results will be obtained by calling the `getValue` method. Taking the SQL statement mentioned at the beginning as an example, there will be two expressions, representing the columns `a + b * c` and `c`. Next, we will use the `a + b * c` expression as an example to introduce its data type and calculation process.

`a + b * c` is an arithmetic operation column, so its type is `ArithmeticColumn` inherited from `ReturnedColumn`. It contains a member variable of type `ParseTree* fExpression`, as well as the implementation of the `getIntValue` method:
```c++
virtual int64_t getIntVal(rowgroup::Row& row, bool& isNull)
{
  return fExpression->getIntVal(row, isNull);
}
```
![](./image-20230321175135576.png) Here is AST graphical representation of a the given math expression.
After obtaining the structure of `fExpression`, we continue to look at the calculation process and enter the `getIntVal` method pointed to by `fExpression`:

```c++
//fExpression->getIntVal(row, isNull);
    inline int64_t getIntVal(rowgroup::Row& row, bool& isNull)
  {
    if (fLeft && fRight)
      return (reinterpret_cast<Operator*>(fData))->getIntVal(row, isNull, fLeft, fRight);
    else
      return fData->getIntVal(row, isNull);
  }
```
From the code, we can see that the entire calculation process is actually a recursive process. When both the left and right subtrees of the current node are not empty, the current node is converted into an `Operator` type and continues to recursively call; otherwise, the value of the leaf node is returned. Taking the binary tree in the image as an example, when both left and right subtrees exist, the current node's `TreeNode` will be converted to an `operator` type (in this case, the `ArithmeticOperator` type), and the `getIntVal` method defined in it will be recursively called for calculation. When it is a leaf node, the node data is extracted.

Each row of data will go through this recursive process, which will affect the calculation performance to some extent. Therefore, here is my implementation plan for the GSOC project:

Since we can obtain the `ParseTree` before the calculation, we can dynamically generate LLVM calculation code based on the `ParseTree` using JIT technology. When executing `void FuncExp::evaluate(rowgroup::Row& row, std::vector<execplan::SRCP>& expression);`, it is only necessary to call the generated LLVM code, which can avoid the recursive calculation operation for each row of data. In this way, there will be only one recursive call (when dynamically generating code using JIT).

The above are some thoughts I had after reading this part of the code. I hope you can give me some suggestions. Thank you very much.