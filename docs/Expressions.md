| No. | Expression                              |                          Incompatibility                            |
| --- | ----------------------------------------|---------------------------------------------------------------------|
| 1   | get_json_object                         | Single quote is not supported.                                      |
| 2   | from_unixtime                           | Specifying timezone is not supported.                               |
| 3   | date/time related expressions           | Incompatibility issues for different LEGACY_TIME_PARSER_POLICY <br/>(corrected, exception, legacy).|
| 4   | castINT/castBIGINT/castFLOAT4/castFLOAT8| Return digital part leading in strings like "123abc" in WSCG, but vanilla spark return null.|
| 5   | all expressions                         | Incompatibility issue when ANSI is on.                              |
