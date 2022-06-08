# Columnar Expression Developer Guide

Currently, the columnar expressions in Gazelle are implemented based on Arrow/gandiva. Developer needs to
implement a columnar expression class in Gazelle scala code and also add some logic to replace Spark expression
with the implemented columnar expression. And the native code is implemented in Arrow/gandiva for expression's
core functionality.

Before native code development, we should check whether the desired function is already implemented
in gandiva. If so, we can directly use it or just make a few code changes to meet the compatibility with Spark.

Take `regexp_extract` as example.

## Arrow/gandiva Native Code

### functions need to use external C++ libs
See [arrow/pull/97](https://github.com/oap-project/arrow/pull/97).

Since C++ lib google/RE2 is leveraged, we implemented the core function in a gandiva function holder.

In `extract_holder.h`, we need declare the below functions other than some other necessary functions.
```
static Status Make(const FunctionNode &node, std::shared_ptr<ExtractHolder> *holder);
```
This function is used to construct `ExtractHolder` and check the legality of input if needed. It will
be called by `function_holder_registry.h` to register the function holder.

In `gdv_function_stubs.cc`，we need to implement a function called `gdv_fn_regexp_extract_utf8_utf8_int32`,
which will invoke overloaded `operator()` in `ExtractHolder`. The `operator()` is the core function to do
the extract work.

We need also register `regexp_extract` in `function_registry_string.cc` (for functions handling strings).
This exposed function name will be used to create function tree in Gazelle scala code. In this case,
function holder is required, so we should specify `NativeFunction::kNeedsFunctionHolder` in the registry.

### functions does not need to use external C++ libs
see [arrow/pull/103](https://github.com/oap-project/arrow/pull/103)
The `pmod` function uses standard C libs only so it can be precompiled in Gandiva. The idea is similar
with adding functions using external libs:
- adding the function implemenation in precompile/xxx.c
- register the function pointer in the function registry
Here's also one [detailed guide](https://www.dremio.com/blog/adding-a-user-define-function-to-gandiva/) from Dremio


For unit test, please refer to `extract_holder_test.cc`. Here is the compile steps.
```
cd arrow/cpp/release-build (create by yourself if not exists)
cmake -DARROW_DEPENDENCY_SOURCE=BUNDLED -DARROW_CSV=ON -DARROW_GANDIVA_JAVA=ON -DARROW_GANDIVA=ON
-DARROW_PARQUET=ON -DARROW_HDFS=ON -DARROW_BOOST_USE_SHARED=ON -DARROW_JNI=ON -DARROW_WITH_SNAPPY=ON
-DARROW_FILESYSTEM=ON -DARROW_JSON=ON -DARROW_WITH_PROTOBUF=ON -DARROW_DATASET=ON -DARROW_IPC=ON
-DARROW_WITH_LZ4=ON -DARROW_JEMALLOC=OFF -DARROW_BUILD_TESTS=ON ..
make -j
./release/gandiva-internals-test
```
## Gazelle Scala Code

See [gazelle_plugin/pull/847](https://github.com/oap-project/gazelle_plugin/pull/847).

In scala code, `ColumnarRegExpExtract` is created to replace Spark's `RegExpExtract`.

We can add `buildCheck` to check the input types. For legal types currently not supported in this implementation,
we can throw an `UnsupportedOperationException` to let the expression fallback. 

In `doColumnarCodeGen`, arrow function node is constructed with gandiva function name `regexp_extract` specified.

The `supportColumnarCodegen` function is used to check if columnar wholestage codegen support is added, please set
to `false` if it's not implemented.

At last, in `replaceWithColumnarExpression` of `ColumnarExpressionConverter.scala`，we need to replace Spark's
expression to the implemented columnar expression.
