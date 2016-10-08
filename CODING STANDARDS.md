# Coding Standards

This is a working document that is subject to change over time. It is intended to be applied to new code going forward rather than instantly applying to the entire codebase immediately.

## Language

We currently use C++ with Boost extensions. Due to the requirement of supporting older platforms such as CentOS 6 we cannot yet support C++11.

## Coding Style

Everyone has a preferred coding style, there is no real correct style. What is important is that we stick to one style throughout the code.

We should use a variant of the [Allman coding style](http://en.wikipedia.org/wiki/Indent_style#Allman_style). The variation is to use 4 spaces instead of tabs. The exception to the rule is Makefiles where space indentation can break them.

Allman style specifies that braces associated with a statement should be on the following line with the same indentation and the statements inside the braces are next level indented. The closing braces are also on a new line at the same indentation as the original statement.

### Switch Statements

Should be indented as follows:

```c++
switch(intValue)
{
    case 1:
        // Do something
        break;
    case 2:
        // Do something else
        break;
    default:
        // Something went wrong
        break;
}
```

### Modeline Headers

The following was generated using the online [Editor Modeline Generator](https://www.wireshark.org/tools/modelines.html), it should be added to the very top of all code files. Most editors will understand one of these:

```c++
/* c-basic-offset: 4; tab-width: 4; indent-tabs-mode: nil
 * vi: set shiftwidth=4 tabstop=4 expandtab:
 * :indentSize=4:tabSize=4:noTabs=true:
 */
```

## Types

Despite being linked to MariaDB we should use std types where possible. For example:

* Use `bool`, not `my_bool`
* Use `true` and `false`, not the `TRUE` and `FALSE` macros
* `ulong` → `uint32_t`
* `ulonglong` → `uint64_t`
* `long int` → `int32_t`

## Pointers

Use `NULL` when referring to the null pointer, not `0`.

## Naming Style

We should use CamelCase everywhere. For types and class definitions these should have an upper case first character. Otherwise they should have a lower case first character.

Do not typedef structs. This is redundant for the compilers we use.

Use `column` instead of `field` and `schema` instead of `database`.

Variables passed around between multiple functions should have consistent naming.

## Includes

Use triangle braces for API includes, this means accessing MariaDB's headers from the engine or system includes. Use double-quotes for local includes.

Use header file `#ifndef` guards instead if `#pragma once`.

## Lengths

These are soft maximum lengths we should try and follow for ease of reading code, but are not hard rules:

* Line length: 80 chars
* Function length: 200 lines

## Braces

Be generous with braces for `if` statements. It makes your logic easier to parse by humans, if there are several depths of braces use multiple lines to make it more readable.

With `if` statements that have one statement the absence of curly braces containing the statement is allowed. But please be careful when editing such code.

When you use a for loop to iterate across something and do not actually do anything in the for loop (for instance, if you just want to walk an array or list of pointers): do this:

```c++
for () {}; 
```

or

```c++
for () {/* Do nothing */};
```

DO NOT do this:

```c++
for (); 
```

Why? When you have an empty body. It gives a hint that you really did want just the for to loop.

## Ignoring Return Types

Generally, return types of a function should not be ignored, however, if you do ignore the return, do:

```c++
(void)func();
```

This indicates explicitly that you are ignoring the return and *know* that you are ignoring the return.

## ENUMs

ENUM identifiers should be in all caps and use a prefix that makes it obvious that it belongs to a given ENUM specifier. The ENUM specifier should also be in all caps.

Use ENUM instead of `#define` where applicable. This aids debugging.

## Class Files

Ideally every class should have its own C++ and header file. The file name should match the class name.
