# Formal review checklist and considerations

> "The cost of fixing a defect is proportional to the time between defect introduction and discovery" - main axiom of softwaer engineering.

## Purpose of the document and intented audience

The purpose of this document is to list common defects and their sources to prevent their introduction into a code base.

It is advisory for Columnstore developers to consult this document while doing personal erview of the code to be published. We also should extend this document as new sources of defects come to light.


# Performance related issues

## Assignment: x = y;

If x is a std::string and y is a pointer, y is treated as an ASCIIZ string. There is implicit strlen call in there.


# Side effects


## Copy-paste errors

Copy-pasted code can contain obvious and non-obvious side effects. These side effects should be controlled for, especially when logging.

### Obvious side effects

Pre- and post-increments: avoid copying these verbatim.

### Non-obvious side effects

Getters and attribute computations may have side-effecting computations inside them, although rare.

One of the effects can be different implicit conversions within different contexts.


## Changed code errors

Code changes can result in defects.

I should look at the semantics of the code that is moved or otherwise refactored. It is possible to have some non-trivial implicit transformations changed and/or deleted.

If you changed the method, review use sites of this method.

# New code errors

## Side effects

### Data invariants

Objects should be constructed with invariants satisfied. If possible, there should be no invariant exceptions for “uninitialized” objects.

### Create do not reuse

If it is possible, I should construct new objects instead of reusing existing ones.

### Separate inputs and outputs

If possible, signify outputs in the result and inputs in arguments. Reference arguments should be avoided as much as possible, especially in new code.

### Type (conversion) errors

C++ has a standardized type constraint solution engine and it is possible to have a program logic error with type conversions and assignments. Myself, I prefer not to implement assignment and conversion operators (and operators at all, if that’s possible) but other’s code can contain these.


### Change of structure of one of the types

Usually, the deletion or change of field is flagged in compiler errors. The addition of fields is not tracked at all, thus, this is the source of errors in assignment operators and other structure-copying mechanisms.

Tracking can be implemented with the change of class/type’s name, then reverting the change back when everything is done.

# Structure of code - design

## Execution should avoid interpretation

Execution of some action should avoid conditions on the data as much as possible.

This warrants a concrete example.

Instead of

```
if (condition on the fields)
        { auto p = createThisObj(...); p-> bubu(); }
else { auto p = createThatObj(...); p->gaga() }
```

We should create object with either executor (or both) and execute them conditionally:

```
If (thisObjBubuExec) { thisObjBubuExec->bubu(); }
If (thatObjGagaExec) { thatObjGagaExec->gaga(); }
```

First variant pushes optimizations and rewrites of execution down to the execution phase, instead of pulling them up into optimization and rewrite phase.

## Execution must be stupid. It is the optimization that must be smart.

### Templates execute, not decide

It is hard to inspect control decisions inside templated code. For example, not all template parametrizations may have serialization to streams implemented. Because of that, templated code should have as few decisions as possible.

It should be preferred to call templates with all decisions made elsewhere and for templates to execute only simple decisions like loops.

## Inheritance should be shallow

Liskov substitution principle usually can be realized several ways. Classical example is the Square and Rectangle inheritance - which one should inherit the other? One can argue both ways, that Rectangle adds dimension and should inherit Square and that Square is a constrained Rectangle and thus should be inherited from.

Usually inheritance direction depends on the use cases present at the time of the design. But, use cases after a while can change - some can be no longer valid, some can be added.

Inheritance adds constraints through LOGICAL AND: inheriting class supports all constraints of the parent class AND adds its own. Classes at the same level of inheritance combine constraints through the LOGICAL OR: each child of a parent class can be used as a parent class.

The structures of these AND and OR connections are fixed in hierarchies, often quite sturdily.

It is much easier to design shallow hierarchies. It is much easier to modify shallow hierarchies.

The most simple hierarchy is a base class, preferably abstract, and exactly one level of inheritance. It is a direct analogue of an Algebraic Data Types from many functional languages and it expresses a SUM (LOGICAL OR) of PRODUCTS (LOGICAL AND).

I should prefer not to use class hierarchy of any kind

Learning from MCS’ code, I see a great deal of copy-paste.

# Miscellanea

## Looping and conditionals - use code blocks

Loops and conditional statements should always delimit their bodies with curly brackets. E.g., there should not be single statements in bodies, there should be compound statements or code blocks.

This makes conditions and loops heavier but it is one of the points: you should make less conditionals and loops.

This also helps with the refactoring as the conditional or loop body is clearly delimited.

## (De)serialization

Serialization and deserialization should go through intermediate types.

This stems from having adhoc serdes code, where I can receive strings from NullStrings sent. If I have a separate message type where I can change strings to NullString, I will get compilation errors indicating changes needed instead of tests failing for inability to find the table it just created.

## Same errors

When fixing an error, look around for the presence of the error of the same type. Addition may not be added everywhere it needed, deletion may not delete everything that has to be deleted.

## No generic getters/setters

These are often superfluous or may violate class contracts when used incorrectly. Also, there is usually now way to find the correct way to use them.

