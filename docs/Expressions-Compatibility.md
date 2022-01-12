## Gazelle Expression Compatibility with Apache Spark

There are some cases that Gazelle behaves differently from Apache Spark. Here, we list the compatibility issues we have not addressed so far.


| No. | Expression                              |                          Incompatibility                            |
| --- | ----------------------------------------|---------------------------------------------------------------------|
|  1  | all expressions                         | Incompatibility issue when ANSI is on (throw exceptions at runtime instead of return null). |
|  2  | get_json_object                         | Single quote mark is not supported, but only support regular double quote mark. <br/>|
|     |                                         | If multiple same keys are contained, null will be returned. But vanilla spark returns the value for the firstly emerged key.|
|  3  | from_unixtime                           | Specifying timezone is not supported. By default, return date for <br/>UTC, not for local timezone like vanilla spark. |                             |
|  4  | date/time related expressions           | Incompatible behaviors for different LEGACY_TIME_PARSER_POLICY <br/>(corrected, exception, legacy). |
|  5  | expressions with date format provided.  | Parsing user-specified date format is not well supported. |
|  6  | castINT/castBIGINT/castFLOAT4/castFLOAT8| Return digital part leading in strings like "123abc" in WSCG, but vanilla spark return null. |


