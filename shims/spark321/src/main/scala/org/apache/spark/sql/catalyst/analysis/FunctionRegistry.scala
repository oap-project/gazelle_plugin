/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.analysis

import java.util.Locale
import javax.annotation.concurrent.GuardedBy

import scala.collection.mutable
import scala.reflect.ClassTag

import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.xml._
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Range}
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils


/**
 * A catalog for looking up user defined functions, used by an [[Analyzer]].
 *
 * Note:
 *   1) The implementation should be thread-safe to allow concurrent access.
 *   2) the database name is always case-sensitive here, callers are responsible to
 *      format the database name w.r.t. case-sensitive config.
 */
trait FunctionRegistryBase[T] {

  type FunctionBuilder = Seq[Expression] => T

  final def registerFunction(
      name: FunctionIdentifier, builder: FunctionBuilder, source: String): Unit = {
    val info = new ExpressionInfo(
      builder.getClass.getCanonicalName,
      name.database.orNull,
      name.funcName,
      null,
      "",
      "",
      "",
      "",
      "",
      "",
      source)
    registerFunction(name, info, builder)
  }

  def registerFunction(
    name: FunctionIdentifier,
    info: ExpressionInfo,
    builder: FunctionBuilder): Unit

  /* Create or replace a temporary function. */
  final def createOrReplaceTempFunction(
      name: String, builder: FunctionBuilder, source: String): Unit = {
    registerFunction(
      FunctionIdentifier(name),
      builder,
      source)
  }

  @throws[AnalysisException]("If function does not exist")
  def lookupFunction(name: FunctionIdentifier, children: Seq[Expression]): T

  /* List all of the registered function names. */
  def listFunction(): Seq[FunctionIdentifier]

  /* Get the class of the registered function by specified name. */
  def lookupFunction(name: FunctionIdentifier): Option[ExpressionInfo]

  /* Get the builder of the registered function by specified name. */
  def lookupFunctionBuilder(name: FunctionIdentifier): Option[FunctionBuilder]

  /** Drop a function and return whether the function existed. */
  def dropFunction(name: FunctionIdentifier): Boolean

  /** Checks if a function with a given name exists. */
  def functionExists(name: FunctionIdentifier): Boolean = lookupFunction(name).isDefined

  /** Clear all registered functions. */
  def clear(): Unit
}

object FunctionRegistryBase {

  /**
   * Return an expression info and a function builder for the function as defined by
   * T using the given name.
   */
  def build[T : ClassTag](
      name: String,
      since: Option[String]): (ExpressionInfo, Seq[Expression] => T) = {
    val runtimeClass = scala.reflect.classTag[T].runtimeClass
    // For `RuntimeReplaceable`, skip the constructor with most arguments, which is the main
    // constructor and contains non-parameter `child` and should not be used as function builder.
    val constructors = if (classOf[RuntimeReplaceable].isAssignableFrom(runtimeClass)) {
      val all = runtimeClass.getConstructors
      val maxNumArgs = all.map(_.getParameterCount).max
      all.filterNot(_.getParameterCount == maxNumArgs)
    } else {
      runtimeClass.getConstructors
    }
    // See if we can find a constructor that accepts Seq[Expression]
    val varargCtor = constructors.find(_.getParameterTypes.toSeq == Seq(classOf[Seq[_]]))
    val builder = (expressions: Seq[Expression]) => {
      if (varargCtor.isDefined) {
        // If there is an apply method that accepts Seq[Expression], use that one.
        try {
          varargCtor.get.newInstance(expressions).asInstanceOf[T]
        } catch {
          // the exception is an invocation exception. To get a meaningful message, we need the
          // cause.
          case e: Exception => throw new AnalysisException(e.getCause.getMessage)
        }
      } else {
        // Otherwise, find a constructor method that matches the number of arguments, and use that.
        val params = Seq.fill(expressions.size)(classOf[Expression])
        val f = constructors.find(_.getParameterTypes.toSeq == params).getOrElse {
          val validParametersCount = constructors
            .filter(_.getParameterTypes.forall(_ == classOf[Expression]))
            .map(_.getParameterCount).distinct.sorted
          throw QueryCompilationErrors.invalidFunctionArgumentNumberError(
            validParametersCount, name, params)
        }
        try {
          f.newInstance(expressions : _*).asInstanceOf[T]
        } catch {
          // the exception is an invocation exception. To get a meaningful message, we need the
          // cause.
          case e: Exception => throw new AnalysisException(e.getCause.getMessage)
        }
      }
    }

    (expressionInfo(name, since), builder)
  }

  /**
   * Creates an [[ExpressionInfo]] for the function as defined by T using the given name.
   */
  def expressionInfo[T : ClassTag](name: String, since: Option[String]): ExpressionInfo = {
    val clazz = scala.reflect.classTag[T].runtimeClass
    val df = clazz.getAnnotation(classOf[ExpressionDescription])
    if (df != null) {
      if (df.extended().isEmpty) {
        new ExpressionInfo(
          clazz.getCanonicalName,
          null,
          name,
          df.usage(),
          df.arguments(),
          df.examples(),
          df.note(),
          df.group(),
          since.getOrElse(df.since()),
          df.deprecated(),
          df.source())
      } else {
        // This exists for the backward compatibility with old `ExpressionDescription`s defining
        // the extended description in `extended()`.
        new ExpressionInfo(clazz.getCanonicalName, null, name, df.usage(), df.extended())
      }
    } else {
      new ExpressionInfo(clazz.getCanonicalName, name)
    }
  }
}

trait SimpleFunctionRegistryBase[T] extends FunctionRegistryBase[T] with Logging {

  @GuardedBy("this")
  protected val functionBuilders =
    new mutable.HashMap[FunctionIdentifier, (ExpressionInfo, FunctionBuilder)]

  // Resolution of the function name is always case insensitive, but the database name
  // depends on the caller
  private def normalizeFuncName(name: FunctionIdentifier): FunctionIdentifier = {
    FunctionIdentifier(name.funcName.toLowerCase(Locale.ROOT), name.database)
  }

  override def registerFunction(
      name: FunctionIdentifier,
      info: ExpressionInfo,
      builder: FunctionBuilder): Unit = {
    val normalizedName = normalizeFuncName(name)
    internalRegisterFunction(normalizedName, info, builder)
  }

  /**
   * Perform function registry without any preprocessing.
   * This is used when registering built-in functions and doing `FunctionRegistry.clone()`
   */
  def internalRegisterFunction(
      name: FunctionIdentifier,
      info: ExpressionInfo,
      builder: FunctionBuilder): Unit = synchronized {
    val newFunction = (info, builder)
    functionBuilders.put(name, newFunction) match {
      case Some(previousFunction) if previousFunction != newFunction =>
        logWarning(s"The function $name replaced a previously registered function.")
      case _ =>
    }
  }

  override def lookupFunction(name: FunctionIdentifier, children: Seq[Expression]): T = {
    val func = synchronized {
      functionBuilders.get(normalizeFuncName(name)).map(_._2).getOrElse {
        throw QueryCompilationErrors.functionUndefinedError(name)
      }
    }
    func(children)
  }

  override def listFunction(): Seq[FunctionIdentifier] = synchronized {
    functionBuilders.iterator.map(_._1).toList
  }

  override def lookupFunction(name: FunctionIdentifier): Option[ExpressionInfo] = synchronized {
    functionBuilders.get(normalizeFuncName(name)).map(_._1)
  }

  override def lookupFunctionBuilder(
      name: FunctionIdentifier): Option[FunctionBuilder] = synchronized {
    functionBuilders.get(normalizeFuncName(name)).map(_._2)
  }

  override def dropFunction(name: FunctionIdentifier): Boolean = synchronized {
    functionBuilders.remove(normalizeFuncName(name)).isDefined
  }

  override def clear(): Unit = synchronized {
    functionBuilders.clear()
  }
}

/**
 * A trivial catalog that returns an error when a function is requested. Used for testing when all
 * functions are already filled in and the analyzer needs only to resolve attribute references.
 */
trait EmptyFunctionRegistryBase[T] extends FunctionRegistryBase[T] {
  override def registerFunction(
      name: FunctionIdentifier, info: ExpressionInfo, builder: FunctionBuilder): Unit = {
    throw new UnsupportedOperationException
  }

  override def lookupFunction(name: FunctionIdentifier, children: Seq[Expression]): T = {
    throw new UnsupportedOperationException
  }

  override def listFunction(): Seq[FunctionIdentifier] = {
    throw new UnsupportedOperationException
  }

  override def lookupFunction(name: FunctionIdentifier): Option[ExpressionInfo] = {
    throw new UnsupportedOperationException
  }

  override def lookupFunctionBuilder(name: FunctionIdentifier): Option[FunctionBuilder] = {
    throw new UnsupportedOperationException
  }

  override def dropFunction(name: FunctionIdentifier): Boolean = {
    throw new UnsupportedOperationException
  }

  override def clear(): Unit = {
    throw new UnsupportedOperationException
  }
}

trait FunctionRegistry extends FunctionRegistryBase[Expression] {

  /** Create a copy of this registry with identical functions as this registry. */
  override def clone(): FunctionRegistry = throw new CloneNotSupportedException()
}

class SimpleFunctionRegistry
    extends SimpleFunctionRegistryBase[Expression]
    with FunctionRegistry {

  override def clone(): SimpleFunctionRegistry = synchronized {
    val registry = new SimpleFunctionRegistry
    functionBuilders.iterator.foreach { case (name, (info, builder)) =>
      registry.internalRegisterFunction(name, info, builder)
    }
    registry
  }
}

object EmptyFunctionRegistry
    extends EmptyFunctionRegistryBase[Expression]
    with FunctionRegistry {

  override def clone(): FunctionRegistry = this
}

object FunctionRegistry {

  type FunctionBuilder = Seq[Expression] => Expression

  val FUNC_ALIAS = TreeNodeTag[String]("functionAliasName")

  val expressionsForTimestampNTZSupport: Map[String, (ExpressionInfo, FunctionBuilder)] =
    // SPARK-36227: Remove TimestampNTZ type support in Spark 3.2 with minimal code changes.
    if (Utils.isTesting) {
      Map(
        expression[ParseToTimestampNTZ]("to_timestamp_ntz"),
        expression[ParseToTimestampLTZ]("to_timestamp_ltz"),
        expression[MakeTimestampNTZ]("make_timestamp_ntz"),
        expression[MakeTimestampLTZ]("make_timestamp_ltz"),
        expression[LocalTimestamp]("localtimestamp")
      )
    } else {
      Map.empty
    }

  // Note: Whenever we add a new entry here, make sure we also update ExpressionToSQLSuite
  val expressions: Map[String, (ExpressionInfo, FunctionBuilder)] = Map(
    // misc non-aggregate functions
    expression[Abs]("abs"),
    expression[Coalesce]("coalesce"),
    expression[Explode]("explode"),
    expressionGeneratorOuter[Explode]("explode_outer"),
    expression[Greatest]("greatest"),
    expression[If]("if"),
    expression[Inline]("inline"),
    expressionGeneratorOuter[Inline]("inline_outer"),
    expression[IsNaN]("isnan"),
    expression[IfNull]("ifnull"),
    expression[IsNull]("isnull"),
    expression[IsNotNull]("isnotnull"),
    expression[Least]("least"),
    expression[NaNvl]("nanvl"),
    expression[NullIf]("nullif"),
    expression[Nvl]("nvl"),
    expression[Nvl2]("nvl2"),
    expression[PosExplode]("posexplode"),
    expressionGeneratorOuter[PosExplode]("posexplode_outer"),
    expression[Rand]("rand"),
    expression[Rand]("random", true),
    expression[Randn]("randn"),
    expression[Stack]("stack"),
    expression[CaseWhen]("when"),

    // math functions
    expression[Acos]("acos"),
    expression[Acosh]("acosh"),
    expression[Asin]("asin"),
    expression[Asinh]("asinh"),
    expression[Atan]("atan"),
    expression[Atan2]("atan2"),
    expression[Atanh]("atanh"),
    expression[Bin]("bin"),
    expression[BRound]("bround"),
    expression[Cbrt]("cbrt"),
    expression[Ceil]("ceil"),
    expression[Ceil]("ceiling", true),
    expression[Cos]("cos"),
    expression[Cosh]("cosh"),
    expression[Conv]("conv"),
    expression[ToDegrees]("degrees"),
    expression[EulerNumber]("e"),
    expression[Exp]("exp"),
    expression[Expm1]("expm1"),
    expression[Floor]("floor"),
    expression[Factorial]("factorial"),
    expression[Hex]("hex"),
    expression[Hypot]("hypot"),
    expression[Logarithm]("log"),
    expression[Log10]("log10"),
    expression[Log1p]("log1p"),
    expression[Log2]("log2"),
    expression[Log]("ln"),
    expression[Remainder]("mod", true),
    expression[UnaryMinus]("negative", true),
    expression[Pi]("pi"),
    expression[Pmod]("pmod"),
    expression[UnaryPositive]("positive"),
    expression[Pow]("pow", true),
    expression[Pow]("power"),
    expression[ToRadians]("radians"),
    expression[Rint]("rint"),
    expression[Round]("round"),
    expression[ShiftLeft]("shiftleft"),
    expression[ShiftRight]("shiftright"),
    expression[ShiftRightUnsigned]("shiftrightunsigned"),
    expression[Signum]("sign", true),
    expression[Signum]("signum"),
    expression[Sin]("sin"),
    expression[Sinh]("sinh"),
    expression[StringToMap]("str_to_map"),
    expression[Sqrt]("sqrt"),
    expression[Tan]("tan"),
    expression[Cot]("cot"),
    expression[Tanh]("tanh"),
    expression[WidthBucket]("width_bucket"),

    expression[Add]("+"),
    expression[Subtract]("-"),
    expression[Multiply]("*"),
    expression[Divide]("/"),
    expression[IntegralDivide]("div"),
    expression[Remainder]("%"),
    expression[TryAdd]("try_add"),
    expression[TryDivide]("try_divide"),

    // aggregate functions
    expression[HyperLogLogPlusPlus]("approx_count_distinct"),
    expression[Average]("avg"),
    expression[Corr]("corr"),
    expression[Count]("count"),
    expression[CountIf]("count_if"),
    expression[CovPopulation]("covar_pop"),
    expression[CovSample]("covar_samp"),
    expression[First]("first"),
    expression[First]("first_value", true),
    expression[Kurtosis]("kurtosis"),
    expression[Last]("last"),
    expression[Last]("last_value", true),
    expression[Max]("max"),
    expression[MaxBy]("max_by"),
    expression[Average]("mean", true),
    expression[Min]("min"),
    expression[MinBy]("min_by"),
    expression[Percentile]("percentile"),
    expression[Skewness]("skewness"),
    expression[ApproximatePercentile]("percentile_approx"),
    expression[ApproximatePercentile]("approx_percentile", true),
    expression[StddevSamp]("std", true),
    expression[StddevSamp]("stddev", true),
    expression[StddevPop]("stddev_pop"),
    expression[StddevSamp]("stddev_samp"),
    expression[Sum]("sum"),
    expression[VarianceSamp]("variance", true),
    expression[VariancePop]("var_pop"),
    expression[VarianceSamp]("var_samp"),
    expression[CollectList]("collect_list"),
    expression[CollectSet]("collect_set"),
    expression[CountMinSketchAgg]("count_min_sketch"),
    expression[BoolAnd]("every", true),
    expression[BoolAnd]("bool_and"),
    expression[BoolOr]("any", true),
    expression[BoolOr]("some", true),
    expression[BoolOr]("bool_or"),

    // string functions
    expression[Ascii]("ascii"),
    expression[Chr]("char", true),
    expression[Chr]("chr"),
    expression[Base64]("base64"),
    expression[BitLength]("bit_length"),
    expression[Length]("char_length", true),
    expression[Length]("character_length", true),
    expression[ConcatWs]("concat_ws"),
    expression[Decode]("decode"),
    expression[Elt]("elt"),
    expression[Encode]("encode"),
    expression[FindInSet]("find_in_set"),
    expression[FormatNumber]("format_number"),
    expression[FormatString]("format_string"),
    expression[GetJsonObject]("get_json_object"),
    expression[InitCap]("initcap"),
    expression[StringInstr]("instr"),
    expression[Lower]("lcase", true),
    expression[Length]("length"),
    expression[Levenshtein]("levenshtein"),
    expression[Like]("like"),
    expression[Lower]("lower"),
    expression[OctetLength]("octet_length"),
    expression[StringLocate]("locate"),
    expression[StringLPad]("lpad"),
    expression[StringTrimLeft]("ltrim"),
    expression[JsonTuple]("json_tuple"),
    expression[ParseUrl]("parse_url"),
    expression[StringLocate]("position", true),
    expression[FormatString]("printf", true),
    expression[RegExpExtract]("regexp_extract"),
    expression[RegExpExtractAll]("regexp_extract_all"),
    expression[RegExpReplace]("regexp_replace"),
    expression[StringRepeat]("repeat"),
    expression[StringReplace]("replace"),
    expression[Overlay]("overlay"),
    expression[RLike]("rlike"),
    expression[RLike]("regexp_like", true, Some("3.2.0")),
    expression[RLike]("regexp", true, Some("3.2.0")),
    expression[StringRPad]("rpad"),
    expression[StringTrimRight]("rtrim"),
    expression[Sentences]("sentences"),
    expression[SoundEx]("soundex"),
    expression[StringSpace]("space"),
    expression[StringSplit]("split"),
    expression[StringSplitPart]("splitpart"),
    expression[Substring]("substr", true),
    expression[Substring]("substring"),
    expression[Left]("left"),
    expression[Right]("right"),
    expression[SubstringIndex]("substring_index"),
    expression[StringTranslate]("translate"),
    expression[StringTrim]("trim"),
    expression[StringTrimBoth]("btrim"),
    expression[Upper]("ucase", true),
    expression[UnBase64]("unbase64"),
    expression[Unhex]("unhex"),
    expression[Upper]("upper"),
    expression[XPathList]("xpath"),
    expression[XPathBoolean]("xpath_boolean"),
    expression[XPathDouble]("xpath_double"),
    expression[XPathDouble]("xpath_number", true),
    expression[XPathFloat]("xpath_float"),
    expression[XPathInt]("xpath_int"),
    expression[XPathLong]("xpath_long"),
    expression[XPathShort]("xpath_short"),
    expression[XPathString]("xpath_string"),

    // datetime functions
    expression[AddMonths]("add_months"),
    expression[CurrentDate]("current_date"),
    expression[CurrentTimestamp]("current_timestamp"),
    expression[CurrentTimeZone]("current_timezone"),
    expression[DateDiff]("datediff"),
    expression[DateAdd]("date_add"),
    expression[DateFormatClass]("date_format"),
    expression[DateSub]("date_sub"),
    expression[DayOfMonth]("day", true),
    expression[DayOfYear]("dayofyear"),
    expression[DayOfMonth]("dayofmonth"),
    expression[FromUnixTime]("from_unixtime"),
    expression[FromUTCTimestamp]("from_utc_timestamp"),
    expression[Hour]("hour"),
    expression[LastDay]("last_day"),
    expression[Minute]("minute"),
    expression[Month]("month"),
    expression[MonthsBetween]("months_between"),
    expression[NextDay]("next_day"),
    expression[Now]("now"),
    expression[Quarter]("quarter"),
    expression[Second]("second"),
    expression[ParseToTimestamp]("to_timestamp"),
    expression[ParseToDate]("to_date"),
    expression[ToUnixTimestamp]("to_unix_timestamp"),
    expression[ToUTCTimestamp]("to_utc_timestamp"),
    expression[TruncDate]("trunc"),
    expression[TruncTimestamp]("date_trunc"),
    expression[UnixTimestamp]("unix_timestamp"),
    expression[DayOfWeek]("dayofweek"),
    expression[WeekDay]("weekday"),
    expression[WeekOfYear]("weekofyear"),
    expression[Year]("year"),
    expression[TimeWindow]("window"),
    expression[SessionWindow]("session_window"),
    expression[MakeDate]("make_date"),
    expression[MakeTimestamp]("make_timestamp"),
    expression[MakeInterval]("make_interval"),
    expression[MakeDTInterval]("make_dt_interval"),
    expression[MakeYMInterval]("make_ym_interval"),
    expression[DatePart]("date_part"),
    expression[Extract]("extract"),
    expression[DateFromUnixDate]("date_from_unix_date"),
    expression[UnixDate]("unix_date"),
    expression[SecondsToTimestamp]("timestamp_seconds"),
    expression[MillisToTimestamp]("timestamp_millis"),
    expression[MicrosToTimestamp]("timestamp_micros"),
    expression[UnixSeconds]("unix_seconds"),
    expression[UnixMillis]("unix_millis"),
    expression[UnixMicros]("unix_micros"),

    // collection functions
    expression[CreateArray]("array"),
    expression[ArrayContains]("array_contains"),
    expression[ArraysOverlap]("arrays_overlap"),
    expression[ArrayIntersect]("array_intersect"),
    expression[ArrayJoin]("array_join"),
    expression[ArrayPosition]("array_position"),
    expression[ArraySort]("array_sort"),
    expression[ArrayExcept]("array_except"),
    expression[ArrayUnion]("array_union"),
    expression[CreateMap]("map"),
    expression[CreateNamedStruct]("named_struct"),
    expression[ElementAt]("element_at"),
    expression[MapFromArrays]("map_from_arrays"),
    expression[MapKeys]("map_keys"),
    expression[MapValues]("map_values"),
    expression[MapEntries]("map_entries"),
    expression[MapFromEntries]("map_from_entries"),
    expression[MapConcat]("map_concat"),
    expression[Size]("size"),
    expression[Slice]("slice"),
    expression[Size]("cardinality", true),
    expression[ArraysZip]("arrays_zip"),
    expression[SortArray]("sort_array"),
    expression[Shuffle]("shuffle"),
    expression[ArrayMin]("array_min"),
    expression[ArrayMax]("array_max"),
    expression[Reverse]("reverse"),
    expression[Concat]("concat"),
    expression[Flatten]("flatten"),
    expression[Sequence]("sequence"),
    expression[ArrayRepeat]("array_repeat"),
    expression[ArrayRemove]("array_remove"),
    expression[ArrayDistinct]("array_distinct"),
    expression[ArrayTransform]("transform"),
    expression[MapFilter]("map_filter"),
    expression[ArrayFilter]("filter"),
    expression[ArrayExists]("exists"),
    expression[ArrayForAll]("forall"),
    expression[ArrayAggregate]("aggregate"),
    expression[TransformValues]("transform_values"),
    expression[TransformKeys]("transform_keys"),
    expression[MapZipWith]("map_zip_with"),
    expression[ZipWith]("zip_with"),

    CreateStruct.registryEntry,

    // misc functions
    expression[AssertTrue]("assert_true"),
    expression[RaiseError]("raise_error"),
    expression[Crc32]("crc32"),
    expression[Md5]("md5"),
    expression[Uuid]("uuid"),
    expression[Murmur3Hash]("hash"),
    expression[XxHash64]("xxhash64"),
    expression[Sha1]("sha", true),
    expression[Sha1]("sha1"),
    expression[Sha2]("sha2"),
    expression[SparkPartitionID]("spark_partition_id"),
    expression[InputFileName]("input_file_name"),
    expression[InputFileBlockStart]("input_file_block_start"),
    expression[InputFileBlockLength]("input_file_block_length"),
    expression[MonotonicallyIncreasingID]("monotonically_increasing_id"),
    expression[CurrentDatabase]("current_database"),
    expression[CurrentCatalog]("current_catalog"),
    expression[CurrentUser]("current_user"),
    expression[CallMethodViaReflection]("reflect"),
    expression[CallMethodViaReflection]("java_method", true),
    expression[SparkVersion]("version"),
    expression[TypeOf]("typeof"),

    // grouping sets
    expression[Grouping]("grouping"),
    expression[GroupingID]("grouping_id"),

    // window functions
    expression[Lead]("lead"),
    expression[Lag]("lag"),
    expression[RowNumber]("row_number"),
    expression[CumeDist]("cume_dist"),
    expression[NthValue]("nth_value"),
    expression[NTile]("ntile"),
    expression[Rank]("rank"),
    expression[DenseRank]("dense_rank"),
    expression[PercentRank]("percent_rank"),

    // predicates
    expression[And]("and"),
    expression[In]("in"),
    expression[Not]("not"),
    expression[Or]("or"),

    // comparison operators
    expression[EqualNullSafe]("<=>"),
    expression[EqualTo]("="),
    expression[EqualTo]("=="),
    expression[GreaterThan](">"),
    expression[GreaterThanOrEqual](">="),
    expression[LessThan]("<"),
    expression[LessThanOrEqual]("<="),
    expression[Not]("!"),

    // bitwise
    expression[BitwiseAnd]("&"),
    expression[BitwiseNot]("~"),
    expression[BitwiseOr]("|"),
    expression[BitwiseXor]("^"),
    expression[BitwiseCount]("bit_count"),
    expression[BitAndAgg]("bit_and"),
    expression[BitOrAgg]("bit_or"),
    expression[BitXorAgg]("bit_xor"),
    expression[BitwiseGet]("bit_get"),
    expression[BitwiseGet]("getbit", true),

    // json
    expression[StructsToJson]("to_json"),
    expression[JsonToStructs]("from_json"),
    expression[SchemaOfJson]("schema_of_json"),
    expression[LengthOfJsonArray]("json_array_length"),
    expression[JsonObjectKeys]("json_object_keys"),

    // cast
    expression[Cast]("cast"),
    // Cast aliases (SPARK-16730)
    castAlias("boolean", BooleanType),
    castAlias("tinyint", ByteType),
    castAlias("smallint", ShortType),
    castAlias("int", IntegerType),
    castAlias("bigint", LongType),
    castAlias("float", FloatType),
    castAlias("double", DoubleType),
    castAlias("decimal", DecimalType.USER_DEFAULT),
    castAlias("date", DateType),
    castAlias("timestamp", TimestampType),
    castAlias("binary", BinaryType),
    castAlias("string", StringType),

    // csv
    expression[CsvToStructs]("from_csv"),
    expression[SchemaOfCsv]("schema_of_csv"),
    expression[StructsToCsv]("to_csv")
  ) ++ expressionsForTimestampNTZSupport

  val builtin: SimpleFunctionRegistry = {
    val fr = new SimpleFunctionRegistry
    expressions.foreach {
      case (name, (info, builder)) =>
        fr.internalRegisterFunction(FunctionIdentifier(name), info, builder)
    }
    fr
  }

  val functionSet: Set[FunctionIdentifier] = builtin.listFunction().toSet

  /**
   * Create a SQL function builder and corresponding `ExpressionInfo`.
   * @param name The function name.
   * @param setAlias The alias name used in SQL representation string.
   * @param since The Spark version since the function is added.
   * @tparam T The actual expression class.
   * @return (function name, (expression information, function builder))
   */
  private def expression[T <: Expression : ClassTag](
      name: String,
      setAlias: Boolean = false,
      since: Option[String] = None): (String, (ExpressionInfo, FunctionBuilder)) = {
    val (expressionInfo, builder) = FunctionRegistryBase.build[T](name, since)
    val newBuilder = (expressions: Seq[Expression]) => {
      val expr = builder(expressions)
      if (setAlias) expr.setTagValue(FUNC_ALIAS, name)
      expr
    }
    (name, (expressionInfo, newBuilder))
  }

  /**
   * Creates a function registry lookup entry for cast aliases (SPARK-16730).
   * For example, if name is "int", and dataType is IntegerType, this means int(x) would become
   * an alias for cast(x as IntegerType).
   * See usage above.
   */
  private def castAlias(
      name: String,
      dataType: DataType): (String, (ExpressionInfo, FunctionBuilder)) = {
    val builder = (args: Seq[Expression]) => {
      if (args.size != 1) {
        throw QueryCompilationErrors.functionAcceptsOnlyOneArgumentError(name)
      }
      Cast(args.head, dataType)
    }
    val clazz = scala.reflect.classTag[Cast].runtimeClass
    val usage = "_FUNC_(expr) - Casts the value `expr` to the target data type `_FUNC_`."
    val expressionInfo =
      new ExpressionInfo(clazz.getCanonicalName, null, name, usage, "", "", "",
        "conversion_funcs", "2.0.1", "", "built-in")
    (name, (expressionInfo, builder))
  }

  private def expressionGeneratorOuter[T <: Generator : ClassTag](name: String)
    : (String, (ExpressionInfo, FunctionBuilder)) = {
    val (_, (info, generatorBuilder)) = expression[T](name)
    val outerBuilder = (args: Seq[Expression]) => {
      GeneratorOuter(generatorBuilder(args).asInstanceOf[Generator])
    }
    (name, (info, outerBuilder))
  }
}

/**
 * A catalog for looking up table functions.
 */
trait TableFunctionRegistry extends FunctionRegistryBase[LogicalPlan] {

  /** Create a copy of this registry with identical functions as this registry. */
  override def clone(): TableFunctionRegistry = throw new CloneNotSupportedException()
}

class SimpleTableFunctionRegistry extends SimpleFunctionRegistryBase[LogicalPlan]
    with TableFunctionRegistry {

  override def clone(): SimpleTableFunctionRegistry = synchronized {
    val registry = new SimpleTableFunctionRegistry
    functionBuilders.iterator.foreach { case (name, (info, builder)) =>
      registry.internalRegisterFunction(name, info, builder)
    }
    registry
  }
}

object EmptyTableFunctionRegistry extends EmptyFunctionRegistryBase[LogicalPlan]
    with TableFunctionRegistry {

  override def clone(): TableFunctionRegistry = this
}

object TableFunctionRegistry {

  type TableFunctionBuilder = Seq[Expression] => LogicalPlan

  private def logicalPlan[T <: LogicalPlan : ClassTag](name: String)
      : (String, (ExpressionInfo, TableFunctionBuilder)) = {
    val (info, builder) = FunctionRegistryBase.build[T](name, since = None)
    val newBuilder = (expressions: Seq[Expression]) => {
      try {
        builder(expressions)
      } catch {
        case e: AnalysisException =>
          val argTypes = expressions.map(_.dataType.typeName).mkString(", ")
          throw QueryCompilationErrors.cannotApplyTableValuedFunctionError(
            name, argTypes, info.getUsage, e.getMessage)
      }
    }
    (name, (info, newBuilder))
  }

  val logicalPlans: Map[String, (ExpressionInfo, TableFunctionBuilder)] = Map(
    logicalPlan[Range]("range")
  )

  val builtin: SimpleTableFunctionRegistry = {
    val fr = new SimpleTableFunctionRegistry
    logicalPlans.foreach {
      case (name, (info, builder)) =>
        fr.internalRegisterFunction(FunctionIdentifier(name), info, builder)
    }
    fr
  }

  val functionSet: Set[FunctionIdentifier] = builtin.listFunction().toSet
}
