# LLVM
Central part of this project is to perform whole-stage code generation. However, developing LLVM IR via the builder classes is cumbersome.
There is a solution though for quicker prototyping!

First of all, the llvm tools are needed. If you have installed LLVM via brew, add `$(brew --prefix llvm)/bin`
```$xslt
export PATH=$PATH:"$(brew --prefix llvm)/bin"
```
to `PATH`.

Then, given a C++/C file (does not need to contain a main function) run
```$xslt
clang -S -emit-llvm file.cc && cat file.ll
```
to print out the generated LLVM IR.