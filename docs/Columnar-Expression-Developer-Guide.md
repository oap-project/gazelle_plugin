## Columnar Expression Developer Guide

Currently, the columnar expressions in Gazelle are implemented based on Arrow/gandiva. Developer needs to
implement a columnar expression class in Gazelle scala code and also add some logic to replace Spark expression
with the implemented columnar expression. And the native code for expression's core functionality is implemented
in Arrow/gandiva.

We should check whether the desired function is already implemented in gandiva. For functions already implemented,
we may still need to make a few code changes to meet the compatibility with Spark.

Take `regexp_extract` as example.

### Arrow/gandiva Native Code

See [arrow/pull/97](https://github.com/oap-project/arrow/pull/97).

Since C++ lib google/RE2 is leveraged, we implemented the core function in a gandiva function holder.

In `extract_holder.h`, we need declare the below functions other than some other necessary functions.
```
static Status Make(const FunctionNode &node, std::shared_ptr<ExtractHolder> *holder);
```
This function is used to construct `ExtractHolder` and check the legality of input if needed. It will
be called by `function_holder_registry.h` to register function holder.

In `gdv_function_stubs.cc`，we need to implement a function called `gdv_fn_regexp_extract_utf8_utf8_int32`,
which will invoke overloaded `operator()` in `ExtractHolder`. The `operator()` is the core function to do
the extract work.

We need also register `regexp_extract` in `function_registry_string.cc` (for functions handling string).
This exposed function name will be used to create function tree in Gazelle scala code. In this case,
function holder is required, so we should specify `NativeFunction::kNeedsFunctionHolder` in the registry.

For unit test, please refer to `extract_holder_test.cc`. Here is the compile steps.

* `cd arrow/cpp/release-build` (create by yourself if not exists)
* `cmake -DARROW_DEPENDENCY_SOURCE=BUNDLED -DARROW_CSV=ON -DARROW_GANDIVA_JAVA=ON -DARROW_GANDIVA=ON
-DARROW_PARQUET=ON -DARROW_HDFS=ON -DARROW_BOOST_USE_SHARED=ON -DARROW_JNI=ON -DARROW_WITH_SNAPPY=ON
-DARROW_FILESYSTEM=ON -DARROW_JSON=ON -DARROW_WITH_PROTOBUF=ON -DARROW_DATASET=ON -DARROW_IPC=ON
-DARROW_WITH_LZ4=ON -DARROW_JEMALLOC=OFF -DARROW_BUILD_TESTS=ON ..`
* `make -j`
* `./release/gandiva-internals-test`

### Gazelle Scala Code

See [gazelle_plugin/pull/847](https://github.com/oap-project/gazelle_plugin/pull/847).

In scala code, `ColumnarRegExpExtract` is created to replace Spark's `RegExpExtract`.

We can add `buildCheck` to check the input types. For legal types currently not supported by Gazelle, we can throw
an `UnsupportedOperationException` to let it fallback. If whole stage codegen is not supported, we should override
`supportColumnarCodegen` to let it return false.

In `doColumnarCodeGen`, arrow function node is constructed with gandiva function name `regexp_extract` specified.

At last, in `replaceWithColumnarExpression` of `ColumnarExpressionConverter.scala`，we need to replace Spark's
expression to the implemented columnar expression.