# Change log
Generated on 2022-06-30

## Release 1.4.0

### Gazelle Plugin

#### Features
|||
|:---|:---|
|[#781](https://github.com/oap-project/gazelle_plugin/issues/781)|Add spark eventlog analyzer for advanced analyzing|
|[#927](https://github.com/oap-project/gazelle_plugin/issues/927)|Column2Row  further enhancement|
|[#913](https://github.com/oap-project/gazelle_plugin/issues/913)|Add Hadoop 3.3 profile to pom.xml|
|[#869](https://github.com/oap-project/gazelle_plugin/issues/869)|implement first agg function|
|[#926](https://github.com/oap-project/gazelle_plugin/issues/926)|Support UDF URLDecoder|
|[#856](https://github.com/oap-project/gazelle_plugin/issues/856)|[SHUFFLE] manually split of Variable length buffer (String likely)|
|[#886](https://github.com/oap-project/gazelle_plugin/issues/886)|Add pmod function support|
|[#855](https://github.com/oap-project/gazelle_plugin/issues/855)|[SHUFFLE] HugePage support in shuffle|
|[#872](https://github.com/oap-project/gazelle_plugin/issues/872)|implement replace function|
|[#867](https://github.com/oap-project/gazelle_plugin/issues/867)|Add substring_index function support|
|[#818](https://github.com/oap-project/gazelle_plugin/issues/818)|Support length, char_length, locate, regexp_extract|
|[#864](https://github.com/oap-project/gazelle_plugin/issues/864)|Enable native parquet write by default|
|[#828](https://github.com/oap-project/gazelle_plugin/issues/828)|CoalesceBatches native implementation|
| [#800](https://github.com/oap-project/gazelle_plugin/issues/800) |Combine datasource and columnar core jar|

#### Performance
|||
|:---|:---|
|[#848](https://github.com/oap-project/gazelle_plugin/issues/848)|Optimize Columnar2Row performance|
|[#943](https://github.com/oap-project/gazelle_plugin/issues/943)|Optimize Row2Columnar performance|
|[#854](https://github.com/oap-project/gazelle_plugin/issues/854)|Enable skipping columnarWSCG for queries with small shuffle size|
|[#857](https://github.com/oap-project/gazelle_plugin/issues/857)|[SHUFFLE] split by reducer by column|

#### Bugs Fixed
|||
|:---|:---|
|[#827](https://github.com/oap-project/gazelle_plugin/issues/827)|Github action is broken|
|[#987](https://github.com/oap-project/gazelle_plugin/issues/987)|TPC-H q7, q8, q9 run failed when using String for Date|
|[#892](https://github.com/oap-project/gazelle_plugin/issues/892)|Q47 and q57 failed on ubuntu 20.04 OS without open-jdk.|
|[#784](https://github.com/oap-project/gazelle_plugin/issues/784)|Improve Sort Spill|
|[#788](https://github.com/oap-project/gazelle_plugin/issues/788)|Spark UT of "randomSplit on reordered partitions"  encountered "Invalid: Map array child array should have no nulls" issue|
|[#821](https://github.com/oap-project/gazelle_plugin/issues/821)|Improve  Wholestage Codegen check|
|[#831](https://github.com/oap-project/gazelle_plugin/issues/831)|Support more expression types in getting attribute|
|[#876](https://github.com/oap-project/gazelle_plugin/issues/876)|Write arrow hang with OutputWriter.path|
|[#891](https://github.com/oap-project/gazelle_plugin/issues/891)|Spark executor lost while DatasetFileWriter failed with speculation|
|[#909](https://github.com/oap-project/gazelle_plugin/issues/909)|"INSERT OVERWRITE x SELECT /*+ REPARTITION(2) */ * FROM y LIMIT 2" drains 4 rows into table x using Arrow write extension|
|[#889](https://github.com/oap-project/gazelle_plugin/issues/889)|Failed to write with ParquetFileFormat while using ArrowWriteExtension|
|[#910](https://github.com/oap-project/gazelle_plugin/issues/910)|TPCDS failed, segfault caused by PR903|
|[#852](https://github.com/oap-project/gazelle_plugin/issues/852)|Unit test fix for NSE-843|
|[#843](https://github.com/oap-project/gazelle_plugin/issues/843)|ArrowDataSouce: Arrow dataset inspect() is called every time a file is read|

#### PRs
|||
|:---|:---|
| [#1002](https://github.com/oap-project/gazelle_plugin/pull/1002) |[NSE-800] Pack the classes into one single jar|
|[#988](https://github.com/oap-project/gazelle_plugin/pull/988)|[NSE-987] fix string date|
|[#977](https://github.com/oap-project/gazelle_plugin/pull/977)|[NSE-126] set default codegen opt to O1|
|[#975](https://github.com/oap-project/gazelle_plugin/pull/975)|[NSE-927] Add macro AVX512BW check for different CPU architecture|
|[#962](https://github.com/oap-project/gazelle_plugin/pull/962)|[NSE-359] disable unit tests on spark32 package|
|[#966](https://github.com/oap-project/gazelle_plugin/pull/966)|[NSE-913] Add support for Hadoop 3.3.1 when packaging|
|[#936](https://github.com/oap-project/gazelle_plugin/pull/936)|[NSE-943] Optimize IsNULL() function for Row2Columnar|
|[#937](https://github.com/oap-project/gazelle_plugin/pull/937)|[NSE-927] Implement AVX512 optimization selection in Runtime and merge two C2R code files into one.|
|[#951](https://github.com/oap-project/gazelle_plugin/pull/951)|[DNM] update sparklog|
|[#938](https://github.com/oap-project/gazelle_plugin/pull/938)|[NSE-581] implement rlike/regexp_like|
|[#946](https://github.com/oap-project/gazelle_plugin/pull/946)|[DNM] update on sparklog script|
|[#939](https://github.com/oap-project/gazelle_plugin/pull/939)|[NSE-581] adding ShortType/FloatType in ColumnarLiteral|
|[#934](https://github.com/oap-project/gazelle_plugin/pull/934)|[NSE-927] Extract and inline functions for native ColumnartoRow|
|[#933](https://github.com/oap-project/gazelle_plugin/pull/933)|[NSE-581] Improve GetArrayItem(Split()) performance|
|[#922](https://github.com/oap-project/gazelle_plugin/pull/922)|[NSE-912] Remove extra handleSafe costs|
|[#925](https://github.com/oap-project/gazelle_plugin/pull/925)|[NSE-926] Support a UDF: URLDecoder|
|[#924](https://github.com/oap-project/gazelle_plugin/pull/924)|[NSE-927] Enable AVX512 in Binary length calculation for native ColumnartoRow|
|[#918](https://github.com/oap-project/gazelle_plugin/pull/918)|[NSE-856] Optimize of string/binary split|
|[#908](https://github.com/oap-project/gazelle_plugin/pull/908)| [NSE-848] Optimize performance for Column2Row|
|[#900](https://github.com/oap-project/gazelle_plugin/pull/900)|[NSE-869] Add 'first' agg function support|
|[#917](https://github.com/oap-project/gazelle_plugin/pull/917)|[NSE-886] Add pmod expression support|
|[#916](https://github.com/oap-project/gazelle_plugin/pull/916)|[NSE-909] fix slow test|
|[#915](https://github.com/oap-project/gazelle_plugin/pull/915)|[NSE-857] Further optimizations of validity buffer split|
|[#912](https://github.com/oap-project/gazelle_plugin/pull/912)|[NSE-909] "INSERT OVERWRITE x SELECT /*+ REPARTITION(2) */ * FROM y L…|
|[#896](https://github.com/oap-project/gazelle_plugin/pull/896)|[NSE-889] Failed to write with ParquetFileFormat while using ArrowWriteExtension|
|[#911](https://github.com/oap-project/gazelle_plugin/pull/911)|[NSE-910] fix bug of PR903|
|[#901](https://github.com/oap-project/gazelle_plugin/pull/901)|[NSE-891] Spark executor lost while DatasetFileWriter failed with speculation|
|[#907](https://github.com/oap-project/gazelle_plugin/pull/907)|[NSE-857] split validity buffer by reducer|
|[#902](https://github.com/oap-project/gazelle_plugin/pull/902)|[NSE-892] Allow to use jar cmd not in PATH|
|[#898](https://github.com/oap-project/gazelle_plugin/pull/898)|[NSE-867][FOLLOWUP] Add substring_index function support|
|[#894](https://github.com/oap-project/gazelle_plugin/pull/894)|[NSE-855] allocate large block of memory for all reducer #881|
|[#880](https://github.com/oap-project/gazelle_plugin/pull/880)|[NSE-857] Fill destination buffer by reducer|
|[#839](https://github.com/oap-project/gazelle_plugin/pull/839)|[DNM] some optimizations to shuffle's split function|
|[#879](https://github.com/oap-project/gazelle_plugin/pull/879)|[NSE-878]Wip get phyplan bugfix|
|[#877](https://github.com/oap-project/gazelle_plugin/pull/877)|[NSE-876] Fix writing arrow hang with OutputWriter.path|
|[#873](https://github.com/oap-project/gazelle_plugin/pull/873)|[NSE-872] implement replace function|
|[#850](https://github.com/oap-project/gazelle_plugin/pull/850)|[NSE-854] Small Shuffle Size disable wholestagecodegen|
|[#868](https://github.com/oap-project/gazelle_plugin/pull/868)|[NSE-867] Add substring_index function support|
|[#847](https://github.com/oap-project/gazelle_plugin/pull/847)|[NSE-818] Support length, char_length, locate & regexp_extract|
|[#865](https://github.com/oap-project/gazelle_plugin/pull/865)|[NSE-864] Enable native parquet write by default|
|[#811](https://github.com/oap-project/gazelle_plugin/pull/811)|[NSE-810] disable codegen for SMJ with local limit|
|[#860](https://github.com/oap-project/gazelle_plugin/pull/860)|remove sensitive info from physical plan|
|[#853](https://github.com/oap-project/gazelle_plugin/pull/853)|[NSE-852] Unit test fix for NSE-843|
|[#844](https://github.com/oap-project/gazelle_plugin/pull/844)|[NSE-843] ArrowDataSouce: Arrow dataset inspect() is called every tim…|
|[#842](https://github.com/oap-project/gazelle_plugin/pull/842)|fix in eventlog script|
|[#841](https://github.com/oap-project/gazelle_plugin/pull/841)|fix bug of script|
|[#829](https://github.com/oap-project/gazelle_plugin/pull/829)|[NSE-828] Add native CoalesceBatches implementation|
|[#830](https://github.com/oap-project/gazelle_plugin/pull/830)|[NSE-831] Support more expression types in getting attribute|
|[#815](https://github.com/oap-project/gazelle_plugin/pull/815)|[NSE-610] Shrink hashmap to use less memory|
|[#822](https://github.com/oap-project/gazelle_plugin/pull/822)|[NSE-821] Fix Wholestage Codegen on unsupported pattern|
|[#824](https://github.com/oap-project/gazelle_plugin/pull/824)|[NSE-823] Use `SPARK_VERSION_SHORT` instead of `SPARK_VERSION` to find SparkShims|
|[#826](https://github.com/oap-project/gazelle_plugin/pull/826)|[NSE-827] fix GHA|
|[#819](https://github.com/oap-project/gazelle_plugin/pull/819)|[DNM] complete sparklog script|
|[#802](https://github.com/oap-project/gazelle_plugin/pull/802)|[NSE-794] Fix count() with decimal value|
|[#801](https://github.com/oap-project/gazelle_plugin/pull/801)|[NSE-786] Adding docs for shim layers|
|[#790](https://github.com/oap-project/gazelle_plugin/pull/790)|[NSE-781]Add eventlog analyzer tool|
|[#789](https://github.com/oap-project/gazelle_plugin/pull/789)|[NSE-788] Quick fix for randomSplit on reordered partitions|
|[#780](https://github.com/oap-project/gazelle_plugin/pull/780)|[NSE-784] fallback Sort after SortHashAgg|


### OAP MLlib

#### Performance
|||
|:---|:---|
|[#204](https://github.com/oap-project/oap-mllib/issues/204)|Intel-MLlib require more memory to run Bayes algorithm.|

#### PRs
|||
|:---|:---|
|[#208](https://github.com/oap-project/oap-mllib/pull/208)|[ML-204][NaiveBayes] Remove cache from NaiveBayes|


## Release 1.3.1

### Gazelle Plugin

#### Features
|||
|:---|:---|
|[#710](https://github.com/oap-project/gazelle_plugin/issues/710)|Add rand expression support|
|[#745](https://github.com/oap-project/gazelle_plugin/issues/745)|improve codegen check|
|[#761](https://github.com/oap-project/gazelle_plugin/issues/761)|Update the document to reflect the changes in build and deployment|
|[#635](https://github.com/oap-project/gazelle_plugin/issues/635)|Document the incompatibility with Spark on Expressions|
|[#702](https://github.com/oap-project/gazelle_plugin/issues/702)|Print output datatype for columnar shuffle on WebUI|
|[#712](https://github.com/oap-project/gazelle_plugin/issues/712)|[Nested type] Optimize Array split and support nested Array |
|[#732](https://github.com/oap-project/gazelle_plugin/issues/732)|[Nested type] Support Struct and Map nested types in Shuffle|
|[#759](https://github.com/oap-project/gazelle_plugin/issues/759)|Add spark 3.1.2 & 3.1.3 as supported versions for 3.1.1 shim layer|

#### Performance
|||
|:---|:---|
|[#610](https://github.com/oap-project/gazelle_plugin/issues/610)|refactor on shuffled hash join/hash agg|

#### Bugs Fixed
|||
|:---|:---|
|[#755](https://github.com/oap-project/gazelle_plugin/issues/755)|GetAttrFromExpr unsupported issue when run TPCDS Q57|
|[#764](https://github.com/oap-project/gazelle_plugin/issues/764)|add java.version to clarify jdk version|
|[#774](https://github.com/oap-project/gazelle_plugin/issues/774)|Fix runtime issues on spark 3.2|
|[#778](https://github.com/oap-project/gazelle_plugin/issues/778)|Failed to find include file while running code gen|
|[#725](https://github.com/oap-project/gazelle_plugin/issues/725)|gazelle failed to run with spark local|
|[#746](https://github.com/oap-project/gazelle_plugin/issues/746)|Improve memory allocation on native row to column operator|
|[#770](https://github.com/oap-project/gazelle_plugin/issues/770)|There are cast exception and null pointer expection in spark-3.2|
|[#772](https://github.com/oap-project/gazelle_plugin/issues/772)|ColumnarBatchScan name missing in UI for Spark321|
|[#740](https://github.com/oap-project/gazelle_plugin/issues/740)|Handle exceptions like std::out_of_range in casting string to numeric types in WSCG|
|[#727](https://github.com/oap-project/gazelle_plugin/issues/727)|Create table failed with TPCH partiton dataset|
|[#719](https://github.com/oap-project/gazelle_plugin/issues/719)|Wrong result on TPC-DS Q38, Q87|
|[#705](https://github.com/oap-project/gazelle_plugin/issues/705)|Two unit tests failed on master branch|

#### PRs
|||
|:---|:---|
|[#834](https://github.com/oap-project/gazelle_plugin/pull/834)|[NSE-746]Fix memory allocation in row to columnar |
|[#809](https://github.com/oap-project/gazelle_plugin/pull/809)|[NSE-746]Fix memory allocation in row to columnar|
|[#817](https://github.com/oap-project/gazelle_plugin/pull/817)|[NSE-761] Update document to reflect spark 3.2.x support|
|[#805](https://github.com/oap-project/gazelle_plugin/pull/805)|[NSE-772] Code refactor for ColumnarBatchScan|
|[#802](https://github.com/oap-project/gazelle_plugin/pull/802)|[NSE-794] Fix count() with decimal value |
|[#779](https://github.com/oap-project/gazelle_plugin/pull/779)|[NSE-778] Failed to find include file while running code gen|
|[#798](https://github.com/oap-project/gazelle_plugin/pull/798)|[NSE-795] Fix a consecutive SMJ issue in wscg|
|[#799](https://github.com/oap-project/gazelle_plugin/pull/799)|[NSE-791] fix xchg reuse in Spark321|
|[#773](https://github.com/oap-project/gazelle_plugin/pull/773)|[NSE-770] [NSE-774] Fix runtime issues on spark 3.2|
|[#787](https://github.com/oap-project/gazelle_plugin/pull/787)|[NSE-774] Fallback broadcast exchange for DPP to reuse|
|[#763](https://github.com/oap-project/gazelle_plugin/pull/763)|[NSE-762] Add complex types support for ColumnarSortExec|
|[#783](https://github.com/oap-project/gazelle_plugin/pull/783)|[NSE-782] prepare 1.3.1 release|
|[#777](https://github.com/oap-project/gazelle_plugin/pull/777)|[NSE-732]Adding new config to enable/disable complex data type support |
|[#776](https://github.com/oap-project/gazelle_plugin/pull/776)|[NSE-770] [NSE-774] Fix runtime issues on spark 3.2|
|[#765](https://github.com/oap-project/gazelle_plugin/pull/765)|[NSE-764] declare java.version for maven|
|[#767](https://github.com/oap-project/gazelle_plugin/pull/767)|[NSE-610] fix unit tests on SHJ|
|[#760](https://github.com/oap-project/gazelle_plugin/pull/760)|[NSE-759] Add spark 3.1.2 & 3.1.3 as supported versions for 3.1.1 shim layer|
|[#757](https://github.com/oap-project/gazelle_plugin/pull/757)|[NSE-746]Fix memory allocation in row to columnar|
|[#724](https://github.com/oap-project/gazelle_plugin/pull/724)|[NSE-725] change the code style for ExecutorManger|
|[#751](https://github.com/oap-project/gazelle_plugin/pull/751)|[NSE-745] Improve codegen check for expression|
|[#742](https://github.com/oap-project/gazelle_plugin/pull/742)|[NSE-359] [NSE-273] Introduce shim layer to fix compatibility issues for gazelle on spark 3.1 & 3.2|
|[#754](https://github.com/oap-project/gazelle_plugin/pull/754)| [NSE-755] Quick fix for ConverterUtils.getAttrFromExpr for TPCDS queries                          |
|[#749](https://github.com/oap-project/gazelle_plugin/pull/749)| [NSE-732] Support Map complex type in Shuffle                                                     |
|[#738](https://github.com/oap-project/gazelle_plugin/pull/738)| [NSE-610] hashjoin opt1                                                                           |
|[#733](https://github.com/oap-project/gazelle_plugin/pull/733)| [NSE-732] Support Struct complex type in Shuffle                                                  |
|[#744](https://github.com/oap-project/gazelle_plugin/pull/744)| [NSE-740] fix codegen with out_of_range check                                                     |
|[#743](https://github.com/oap-project/gazelle_plugin/pull/743)| [NSE-740] Catch out_of_range exception in casting string to numeric types in wscg                 |
|[#735](https://github.com/oap-project/gazelle_plugin/pull/735)| [NSE-610] hashagg opt#2                                                                           |
|[#707](https://github.com/oap-project/gazelle_plugin/pull/707)| [NSE-710] Add rand expression support                                                             |
|[#734](https://github.com/oap-project/gazelle_plugin/pull/734)| [NSE-727] Create table failed with TPCH partiton dataset, patch 2                                 |
|[#715](https://github.com/oap-project/gazelle_plugin/pull/715)| [NSE-610] hashagg opt#1                                                                           |
|[#731](https://github.com/oap-project/gazelle_plugin/pull/731)| [NSE-727] Create table failed with TPCH partiton dataset                                          |
|[#713](https://github.com/oap-project/gazelle_plugin/pull/713)| [NSE-712]  Optimize Array split and support nested Array                                          |
|[#721](https://github.com/oap-project/gazelle_plugin/pull/721)| [NSE-719][backport]fix null check in SMJ                                                          |
|[#720](https://github.com/oap-project/gazelle_plugin/pull/720)| [NSE-719] fix null check in SMJ                                                                   |
|[#718](https://github.com/oap-project/gazelle_plugin/pull/718)| Following NSE-702, fix for AQE enabled case                                                       |
|[#691](https://github.com/oap-project/gazelle_plugin/pull/691)| [NSE-687]Try to upgrade log4j                                                                     |
|[#703](https://github.com/oap-project/gazelle_plugin/pull/703)| [NSE-702] Print output datatype for columnar shuffle on WebUI                                     |
|[#706](https://github.com/oap-project/gazelle_plugin/pull/706)| [NSE-705] Fallback R2C on unsupported cases                                                       |
|[#657](https://github.com/oap-project/gazelle_plugin/pull/657)| [NSE-635] Add document to clarify incompatibility issues in expressions                           |
|[#623](https://github.com/oap-project/gazelle_plugin/pull/623)| [NSE-602] Fix Array type shuffle split segmentation fault                                         |
|[#693](https://github.com/oap-project/gazelle_plugin/pull/693)| [NSE-692] JoinBenchmark is broken                                                                 |


### OAP MLlib

#### Features
|||
|:---|:---|
|[#189](https://github.com/oap-project/oap-mllib/issues/189)|Intel-MLlib not support spark-3.2.1 version|
|[#186](https://github.com/oap-project/oap-mllib/issues/186)|[Core] Support CDH versions|
|[#187](https://github.com/oap-project/oap-mllib/issues/187)|Intel-MLlib not support spark-3.1.3 version.|
|[#180](https://github.com/oap-project/oap-mllib/issues/180)|[CI] Refactor CI and add code checks|

#### Bugs Fixed
|||
|:---|:---|
|[#202](https://github.com/oap-project/oap-mllib/issues/202)|[SDLe] Update oneAPI version to solve vulnerabilities|
|[#171](https://github.com/oap-project/oap-mllib/issues/171)|[Core] detect if spark.dynamicAllocation.enabled is set true and exit gracefully|
|[#185](https://github.com/oap-project/oap-mllib/issues/185)|[Naive Bayes]Big dataset will out of memory errors.|
|[#184](https://github.com/oap-project/oap-mllib/issues/184)|[Core] Fix code style issues|
|[#179](https://github.com/oap-project/oap-mllib/issues/179)|[GPU][PCA] use distributed covariance as the first step for PCA|
|[#178](https://github.com/oap-project/oap-mllib/issues/178)|[ALS] Fix error when converting buffer to CSRNumericTable|
|[#177](https://github.com/oap-project/oap-mllib/issues/177)|[Native Bayes] Fix error when converting Vector to CSRNumericTable|

#### PRs
|||
|:---|:---|
|[#203](https://github.com/oap-project/oap-mllib/pull/203)|[ML-202] Update oneAPI Base Toolkit version and prepare for OAP 1.3.1 release|
|[#197](https://github.com/oap-project/oap-mllib/pull/197)|[ML-187]Support spark 3.1.3 and 3.2.0 and support CDH|
|[#201](https://github.com/oap-project/oap-mllib/pull/201)|[ML-171]When enabled oap mllib, spark.dynamicAllocation.enabled should be set false.|
|[#196](https://github.com/oap-project/oap-mllib/pull/196)|[ML-185]Select label and features columns and cache data|
|[#195](https://github.com/oap-project/oap-mllib/pull/195)|[ML-184]Fix code style issues|
|[#183](https://github.com/oap-project/oap-mllib/pull/183)|[ML-180][CI] Refactor CI and add code checks|
|[#175](https://github.com/oap-project/oap-mllib/pull/175)|[ML-179][GPU] use distributed covariance as the first step for PCA|
|[#182](https://github.com/oap-project/oap-mllib/pull/182)|[ML-178]fix als convert buffer to NumericTable|
|[#176](https://github.com/oap-project/oap-mllib/pull/176)|[ML-177][Native Bayes] Fix error when converting Vector to CSRNumericTable|

## Release 1.3.0

### Gazelle Plugin

#### Features
|||
|:---|:---|
|[#550](https://github.com/oap-project/gazelle_plugin/issues/550)|[ORC] Support ORC Format  Reading|
|[#188](https://github.com/oap-project/gazelle_plugin/issues/188)|Support Dockerfile|
|[#574](https://github.com/oap-project/gazelle_plugin/issues/574)|implement native LocalLimit/GlobalLimit|
|[#684](https://github.com/oap-project/gazelle_plugin/issues/684)|BufferedOutputStream causes massive futex system calls|
|[#465](https://github.com/oap-project/gazelle_plugin/issues/465)|Provide option to rely on JVM GC to release Arrow buffers in Java|
|[#681](https://github.com/oap-project/gazelle_plugin/issues/681)|Enable gazelle to support two math expressions: ceil & floor|
|[#651](https://github.com/oap-project/gazelle_plugin/issues/651)|Set Hadoop 3.2 as default in pom.xml|
|[#126](https://github.com/oap-project/gazelle_plugin/issues/126)|speed up codegen|
|[#596](https://github.com/oap-project/gazelle_plugin/issues/596)|[ORC] Verify whether ORC file format supported complex data types in gazelle|
|[#581](https://github.com/oap-project/gazelle_plugin/issues/581)|implement regex/trim/split expr|
|[#473](https://github.com/oap-project/gazelle_plugin/issues/473)|Optimize the ArrowColumnarToRow performance|
|[#647](https://github.com/oap-project/gazelle_plugin/issues/647)|Leverage buffered write in shuffle|
|[#674](https://github.com/oap-project/gazelle_plugin/issues/674)|Add translate expression support|
|[#675](https://github.com/oap-project/gazelle_plugin/issues/675)|Add instr expression support|
|[#645](https://github.com/oap-project/gazelle_plugin/issues/645)|Add support to cast data in bool type to bigint type or string type|
|[#463](https://github.com/oap-project/gazelle_plugin/issues/463)|version bump on 1.3|
|[#583](https://github.com/oap-project/gazelle_plugin/issues/583)|implement get_json_object|
|[#640](https://github.com/oap-project/gazelle_plugin/issues/640)|Disable compression for tiny payloads in shuffle|
|[#631](https://github.com/oap-project/gazelle_plugin/issues/631)|Do not write schema in shuffle writting|
|[#609](https://github.com/oap-project/gazelle_plugin/issues/609)|Implement date related expression like to_date, date_sub|
|[#629](https://github.com/oap-project/gazelle_plugin/issues/629)|Improve codegen failure handling|
|[#612](https://github.com/oap-project/gazelle_plugin/issues/612)|Add metric "prepare time" for shuffle writer|
|[#576](https://github.com/oap-project/gazelle_plugin/issues/576)|columnar FROM_UNIXTIME|
|[#589](https://github.com/oap-project/gazelle_plugin/issues/589)|[ORC] Add TPCDS and TPCH UTs  for ORC Format Reading|
|[#537](https://github.com/oap-project/gazelle_plugin/issues/537)|Increase partition number adaptively for large SHJ stages|
|[#580](https://github.com/oap-project/gazelle_plugin/issues/580)|document how to create metadata for data source V1 based testing|
|[#555](https://github.com/oap-project/gazelle_plugin/issues/555)|support batch size > 32k|
|[#561](https://github.com/oap-project/gazelle_plugin/issues/561)|document the code generation behavior on driver, suggest to deploy driver on powerful server|
|[#523](https://github.com/oap-project/gazelle_plugin/issues/523)|Support ArrayType in ArrowColumnarToRow operator|
|[#542](https://github.com/oap-project/gazelle_plugin/issues/542)|Add rule to propagate local window for rank + filter pattern|
|[#21](https://github.com/oap-project/gazelle_plugin/issues/21)|JNI: Unexpected behavior when executing codes after calling JNIEnv::ThrowNew|
|[#512](https://github.com/oap-project/gazelle_plugin/issues/512)|Add strategy to force use of SHJ|
|[#518](https://github.com/oap-project/gazelle_plugin/issues/518)|Arrow buffer cleanup: Support both manual release and auto release as a hybrid mode|
|[#525](https://github.com/oap-project/gazelle_plugin/issues/525)|Support AQE in columnWriter|
|[#516](https://github.com/oap-project/gazelle_plugin/issues/516)|Support External Sort in sort kernel|
|[#503](https://github.com/oap-project/gazelle_plugin/issues/503)|能提供下官网性能测试的详细配置吗？|
|[#501](https://github.com/oap-project/gazelle_plugin/issues/501)|Remove ArrowRecordBatchBuilder and its usages|
|[#461](https://github.com/oap-project/gazelle_plugin/issues/461)|Support ArrayType in Gazelle|
|[#479](https://github.com/oap-project/gazelle_plugin/issues/479)|Optimize sort materialization|
|[#449](https://github.com/oap-project/gazelle_plugin/issues/449)|Refactor sort codegen kernel|
|[#667](https://github.com/oap-project/gazelle_plugin/issues/667)|1.3 RC release|
|[#352](https://github.com/oap-project/gazelle_plugin/issues/352)|Map/Array/Struct type support for Parquet reading in Arrow Data Source|



#### Bugs Fixed
|||
|:---|:---|
|[#660](https://github.com/oap-project/gazelle_plugin/issues/660)|support string builder in window output|
|[#636](https://github.com/oap-project/gazelle_plugin/issues/636)|Remove log4j 1.2 Support for security issue|
|[#540](https://github.com/oap-project/gazelle_plugin/issues/540)|reuse subquery in TPC-DS Q14a|
|[#687](https://github.com/oap-project/gazelle_plugin/issues/687)|log4j 1.2.17 in spark-core|
|[#617](https://github.com/oap-project/gazelle_plugin/issues/617)|Exceptions handling for stoi, stol, stof, stod in whole stage code gen|
|[#653](https://github.com/oap-project/gazelle_plugin/issues/653)|Handle special cases for get_json_object in WSCG |
|[#650](https://github.com/oap-project/gazelle_plugin/issues/650)|Scala test ArrowColumnarBatchSerializerSuite is failing|
|[#642](https://github.com/oap-project/gazelle_plugin/issues/642)|Fail to cast unresolved reference to attribute reference|
|[#599](https://github.com/oap-project/gazelle_plugin/issues/599)|data source unit tests are broken|
|[#604](https://github.com/oap-project/gazelle_plugin/issues/604)|Sort with special projection key broken|
|[#627](https://github.com/oap-project/gazelle_plugin/issues/627)|adding security instructions|
|[#615](https://github.com/oap-project/gazelle_plugin/issues/615)|An excpetion in trying to cast attribute in getResultAttrFromExpr of ConverterUtils|
|[#588](https://github.com/oap-project/gazelle_plugin/issues/588)|preallocated memory for shuffle split|
|[#606](https://github.com/oap-project/gazelle_plugin/issues/606)|NullpointerException getting map values from ArrowWritableColumnVector|
|[#569](https://github.com/oap-project/gazelle_plugin/issues/569)|CPU overhead on fine grain / concurrent off-heap acquire operations|
|[#553](https://github.com/oap-project/gazelle_plugin/issues/553)|Support casting string type to types like int, bigint, float, double|
|[#514](https://github.com/oap-project/gazelle_plugin/issues/514)|Fix the core dump issue in Q93 when enable columnar2row |
|[#532](https://github.com/oap-project/gazelle_plugin/issues/532)|Fix the failed UTs in ArrowEvalPythonExecSuite when enable ArrowColumnarToRow|
|[#534](https://github.com/oap-project/gazelle_plugin/issues/534)|Columnar SHJ: Error if probing with empty record batch|
|[#529](https://github.com/oap-project/gazelle_plugin/issues/529)|Wrong build side may be chosen for SemiJoin when forcing use of SHJ|
|[#504](https://github.com/oap-project/gazelle_plugin/issues/504)|Fix non-decimal window function unit test failures|
|[#493](https://github.com/oap-project/gazelle_plugin/issues/493)|Three unit tests newly failed on master branch|

#### PRs
|||
|:---|:---|
|[#690](https://github.com/oap-project/gazelle_plugin/pull/690)|[NSE-667] backport patches to 1.3 branch|
|[#688](https://github.com/oap-project/gazelle_plugin/pull/688)|[NSE-687]remove exclude log4j when running ut|
|[#686](https://github.com/oap-project/gazelle_plugin/pull/686)|[NSE-400] Fix the bug for negative decimal data|
|[#685](https://github.com/oap-project/gazelle_plugin/pull/685)|[NSE-684] BufferedOutputStream causes massive futex system calls|
|[#680](https://github.com/oap-project/gazelle_plugin/pull/680)|[NSE-667] backport patches to 1.3 branch|
|[#683](https://github.com/oap-project/gazelle_plugin/pull/683)|[NSE-400] fix leakage in row to column operator|
|[#637](https://github.com/oap-project/gazelle_plugin/pull/637)|[NSE-400] Native Arrow Row to columnar support|
|[#648](https://github.com/oap-project/gazelle_plugin/pull/648)|[NSE-647] Leverage buffered write in shuffle |
|[#682](https://github.com/oap-project/gazelle_plugin/pull/682)|[NSE-681] Add floor & ceil expression support|
|[#672](https://github.com/oap-project/gazelle_plugin/pull/672)|[NSE-674] Add translate expression support|
|[#676](https://github.com/oap-project/gazelle_plugin/pull/676)|[NSE-675] Add instr expression support|
|[#652](https://github.com/oap-project/gazelle_plugin/pull/652)|[NSE-651]Use Hadoop 3.2 as default hadoop.version|
|[#666](https://github.com/oap-project/gazelle_plugin/pull/666)|[NSE-667] backport patches to 1.3 branch|
|[#644](https://github.com/oap-project/gazelle_plugin/pull/644)|[NSE-645] Add support to cast bool type to bigint type & string type|
|[#659](https://github.com/oap-project/gazelle_plugin/pull/659)|[NSE-650] Scala test ArrowColumnarBatchSerializerSuite is failing |
|[#649](https://github.com/oap-project/gazelle_plugin/pull/649)|[NSE-660] fix window builder with string|
|[#655](https://github.com/oap-project/gazelle_plugin/pull/655)|[NSE-617] Handle exception in cast expression from string to numeric types in WSCG|
|[#654](https://github.com/oap-project/gazelle_plugin/pull/654)|[NSE-653] Add validity checking for get_json_object in WSCG|
|[#641](https://github.com/oap-project/gazelle_plugin/pull/641)|[NSE-640] Disable compression for tiny payloads in shuffle|
|[#646](https://github.com/oap-project/gazelle_plugin/pull/646)|[NSE-636]Remove log4j1 related unit tests|
|[#488](https://github.com/oap-project/gazelle_plugin/pull/488)|[NSE-463] version bump to 1.3.0-SNAPSHOT|
|[#639](https://github.com/oap-project/gazelle_plugin/pull/639)|[NSE-126] improve codegen with pre-compiled header|
|[#638](https://github.com/oap-project/gazelle_plugin/pull/638)|[NSE-642] Correctly get ResultAttrFromExpr for sql with 'case when IN/AND/OR'|
|[#632](https://github.com/oap-project/gazelle_plugin/pull/632)|[NSE-631] Do not write schema in shuffle writting|
|[#633](https://github.com/oap-project/gazelle_plugin/pull/633)|[NSE-601] Fix an issue in the case of group by coalesce|
|[#630](https://github.com/oap-project/gazelle_plugin/pull/630)|[NSE-629] improve codegen failure handling|
|[#622](https://github.com/oap-project/gazelle_plugin/pull/622)|[NSE-609] Complete to_date expression support|
|[#628](https://github.com/oap-project/gazelle_plugin/pull/628)|[NSE-627] Doc: adding security readme|
|[#624](https://github.com/oap-project/gazelle_plugin/pull/624)|[NSE-609] Add support for date_sub expression|
|[#619](https://github.com/oap-project/gazelle_plugin/pull/619)|[NSE-583] impl get_json_object in wscg|
|[#614](https://github.com/oap-project/gazelle_plugin/pull/614)|[NSE-576] Support from_unixtime expression in the case that 'yyyyMMdd' format is required|
|[#616](https://github.com/oap-project/gazelle_plugin/pull/616)|[NSE-615] Add tackling for ColumnarEqualTo type in getResultAttrFromExpr of ConverterUtils|
|[#613](https://github.com/oap-project/gazelle_plugin/pull/613)|[NSE-612] Add metric "prepare time" for shuffle writer|
|[#608](https://github.com/oap-project/gazelle_plugin/pull/608)|[NSE-602] don't enable columnar shuffle on unsupported data types|
|[#601](https://github.com/oap-project/gazelle_plugin/pull/601)|[NSE-604] fix sort w/ proj keys|
|[#607](https://github.com/oap-project/gazelle_plugin/pull/607)|[NSE-606] NullpointerException getting map values from ArrowWritableC…|
|[#584](https://github.com/oap-project/gazelle_plugin/pull/584)|[NSE-583] implement get_json_object|
|[#595](https://github.com/oap-project/gazelle_plugin/pull/595)|[NSE-576] fix from_unixtime|
|[#582](https://github.com/oap-project/gazelle_plugin/pull/582)|[NSE-581]impl regexp_replace|
|[#594](https://github.com/oap-project/gazelle_plugin/pull/594)|[NSE-588] config the pre-allocated memory for shuffle's splitter|
|[#600](https://github.com/oap-project/gazelle_plugin/pull/600)|[NSE-599] fix datasource unit tests|
|[#597](https://github.com/oap-project/gazelle_plugin/pull/597)|[NSE-596] Add complex data types validation for ORC file format in gazelle |
|[#590](https://github.com/oap-project/gazelle_plugin/pull/590)|[NSE-569] CPU overhead on fine grain / concurrent off-heap acquire operations|
|[#586](https://github.com/oap-project/gazelle_plugin/pull/586)|[NSE-581] Add trim, left trim, right trim support in expression|
|[#578](https://github.com/oap-project/gazelle_plugin/pull/578)|[NSE-589] Add TPCDS and TPCH suite for Orc fileformat|
|[#538](https://github.com/oap-project/gazelle_plugin/pull/538)|[NSE-537] Increase partition number adaptively for large SHJ stages |
|[#587](https://github.com/oap-project/gazelle_plugin/pull/587)|[NSE-580] update doc on data source(DS V1/V2 usage)|
|[#575](https://github.com/oap-project/gazelle_plugin/pull/575)|[NSE-574]implement columnar limit|
|[#556](https://github.com/oap-project/gazelle_plugin/pull/556)|[NSE-555] using 32bit selection vector|
|[#577](https://github.com/oap-project/gazelle_plugin/pull/577)|[NSE-576] implement columnar from_unixtime|
|[#572](https://github.com/oap-project/gazelle_plugin/pull/572)|[NSE-561] refine docs on sample configurations and code generation behavior|
|[#552](https://github.com/oap-project/gazelle_plugin/pull/552)|[NSE-553] Complete the support to cast string type to types like int, bigint, float, double |
|[#543](https://github.com/oap-project/gazelle_plugin/pull/543)|[NSE-540] enable reuse subquery|
|[#554](https://github.com/oap-project/gazelle_plugin/pull/554)|[NSE-207] change the fallback condition for Columnar Like|
|[#559](https://github.com/oap-project/gazelle_plugin/pull/559)|[NSE-352] Map/Array/Struct type support for Parquet reading in ArrowData Source|
|[#551](https://github.com/oap-project/gazelle_plugin/pull/551)|[NSE-550] Support ORC Format Reading in Gazelle|
|[#545](https://github.com/oap-project/gazelle_plugin/pull/545)|[NSE-542] Add rule to propagate local window for rank + filter pattern|
|[#541](https://github.com/oap-project/gazelle_plugin/pull/541)|[NSE-207] improve the fix for join optimization|
|[#495](https://github.com/oap-project/gazelle_plugin/pull/495)|[NSE-207] Fix NaN in Max and Min|
|[#533](https://github.com/oap-project/gazelle_plugin/pull/533)|[NSE-532] Fix the failed UTs in ArrowEvalPythonExecSuite when enable ArrowColumnarToRow|
|[#536](https://github.com/oap-project/gazelle_plugin/pull/536)|[NSE-207] Ignore tests causing test stop|
|[#535](https://github.com/oap-project/gazelle_plugin/pull/535)|[NSE-534] Columnar SHJ: Error if probing with empty record batch|
|[#531](https://github.com/oap-project/gazelle_plugin/pull/531)|[NSE-21] JNI: Unexpected behavior when executing codes after calling JNIEnv::ThrowNew|
|[#530](https://github.com/oap-project/gazelle_plugin/pull/530)|[NSE-529] Wrong build side may be chosen for SemiJoin when forcing use of SHJ|
|[#524](https://github.com/oap-project/gazelle_plugin/pull/524)|[NSE-523] Support ArrayType in ArrowColumnarToRow optimization|
|[#513](https://github.com/oap-project/gazelle_plugin/pull/513)|[NSE-512] Add strategy to force use of SHJ|
|[#519](https://github.com/oap-project/gazelle_plugin/pull/519)|[NSE-518] Arrow buffer cleanup: Support both manual release and auto …|
|[#526](https://github.com/oap-project/gazelle_plugin/pull/526)|[NSE-525]Support AQE for ColumnarWriter|
|[#517](https://github.com/oap-project/gazelle_plugin/pull/517)|[NSE-516]Support ExternalSorter to control memory footage|
|[#515](https://github.com/oap-project/gazelle_plugin/pull/515)|[NSE-514] Fix the core dump issue in Q93 with V2 test|
|[#509](https://github.com/oap-project/gazelle_plugin/pull/509)|Update README.md for performance result.|
|[#511](https://github.com/oap-project/gazelle_plugin/pull/511)|[NSE-207] fix full UT test|
|[#502](https://github.com/oap-project/gazelle_plugin/pull/502)|[NSE-501] Remove ArrowRecordBatchBuilder and its usages|
|[#507](https://github.com/oap-project/gazelle_plugin/pull/507)|Previous PR removed this UT, fix here|
|[#496](https://github.com/oap-project/gazelle_plugin/pull/496)|[NSE-461]columnar shuffle support for ArrayType|
|[#480](https://github.com/oap-project/gazelle_plugin/pull/480)|[NSE-479] optimize sort materialization|
|[#474](https://github.com/oap-project/gazelle_plugin/pull/474)|[NSE-473]Optimize ArrowColumnarToRow performance|
|[#505](https://github.com/oap-project/gazelle_plugin/pull/505)|[NSE-504] Fix non-decimal window function unit test|
|[#497](https://github.com/oap-project/gazelle_plugin/pull/497)| [NSE-493] Three unit tests newly failed on master branch (Python UDF Unit Tests)|
|[#466](https://github.com/oap-project/gazelle_plugin/pull/466)|[NSE-465] POC release memory using GC|
|[#462](https://github.com/oap-project/gazelle_plugin/pull/462)|[NSE-461][WIP] Support ArrayType in ArrowWritableColumnVector and ColumarPandasUDF|
|[#450](https://github.com/oap-project/gazelle_plugin/pull/450)|[NSE-449] Refactor codegen sort kernel|
|[#471](https://github.com/oap-project/gazelle_plugin/pull/471)|[NSE-207] Enabling UT report|
|[#445](https://github.com/oap-project/gazelle_plugin/pull/445)|[NSE-444]Support ArrowColumnarToRowExec when the root plan is ColumnarToRowExec|
|[#447](https://github.com/oap-project/gazelle_plugin/pull/447)|[NSE-207] Fix date and timestamp functions|


### OAP MLlib

#### Features
|||
|:---|:---|
|[#158](https://github.com/oap-project/oap-mllib/issues/158)|[GPU] Add convertToSyclHomogen for row merged table for kmeans and pca|
|[#149](https://github.com/oap-project/oap-mllib/issues/149)|[GPU] Add check-gpu utility|
|[#140](https://github.com/oap-project/oap-mllib/issues/140)|[Core] Refactor and support multiple Spark versions in single JAR|
|[#137](https://github.com/oap-project/oap-mllib/issues/137)|[Core] Multiple improvements for build & deploy and integrate oneAPI 2021.4|
|[#133](https://github.com/oap-project/oap-mllib/issues/133)|[Correlation] Add Correlation algorithm|
|[#125](https://github.com/oap-project/oap-mllib/issues/125)|[GPU] Update for Kmeans and PCA|

#### Bugs Fixed
|||
|:---|:---|
|[#161](https://github.com/oap-project/oap-mllib/issues/161)|[SDLe][Snyk] Log4j 1.2.* issues brought from Spark when scanning 3rd-party components for vulnerabilities|
|[#155](https://github.com/oap-project/oap-mllib/issues/155)|[POM] Update scala version to 2.12.15|
|[#135](https://github.com/oap-project/oap-mllib/issues/135)|[Core] Fix ccl::gather and Add ccl::gatherv|

#### PRs
|||
|:---|:---|
|[#162](https://github.com/oap-project/oap-mllib/pull/162)|[ML-161] Excluding log4j 1.x dependency from Spark core to avoid log4…|
|[#159](https://github.com/oap-project/oap-mllib/pull/159)|[GPU] Add convertToSyclHomogen for row merged table for kmeans and pca|
|[#157](https://github.com/oap-project/oap-mllib/pull/157)|[ML-155] [POM] Update scala version to 2.12.15|
|[#150](https://github.com/oap-project/oap-mllib/pull/150)|[ML-149][GPU] Add check-gpu utility|
|[#144](https://github.com/oap-project/oap-mllib/pull/144)|[ML-151] enable Summarizer with OAP|
|[#141](https://github.com/oap-project/oap-mllib/pull/141)|[Core] Refactor and support multiple Spark versions in single JAR|
|[#139](https://github.com/oap-project/oap-mllib/pull/139)|[ML-137] [Core] Multiple improvements for build & deploy and integrate oneAPI 2021.4|
|[#127](https://github.com/oap-project/oap-mllib/pull/127)|[ML-133][Correlation] Add Correlation algorithm|
|[#126](https://github.com/oap-project/oap-mllib/pull/126)|[ML-125][GPU] Update for Kmeans and PCA|


## Release 1.2.0

### Gazelle Plugin

#### Features
|||
|:---|:---|
|[#394](https://github.com/oap-project/gazelle_plugin/issues/394)|Support ColumnarArrowEvalPython operator |
|[#368](https://github.com/oap-project/gazelle_plugin/issues/368)|Encountered Hadoop version (3.2.1) conflict issue on AWS EMR-6.3.0|
|[#375](https://github.com/oap-project/gazelle_plugin/issues/375)|Implement a series of datetime functions|
|[#183](https://github.com/oap-project/gazelle_plugin/issues/183)|Add Date/Timestamp type support|
|[#362](https://github.com/oap-project/gazelle_plugin/issues/362)|make arrow-unsafe allocator as the default|
|[#343](https://github.com/oap-project/gazelle_plugin/issues/343)|configurable codegen opt level|
|[#333](https://github.com/oap-project/gazelle_plugin/issues/333)|Arrow Data Source: CSV format support fix|
|[#223](https://github.com/oap-project/gazelle_plugin/issues/223)|Add Parquet write support to Arrow data source|
|[#320](https://github.com/oap-project/gazelle_plugin/issues/320)|Add build option to enable unsafe Arrow allocator|
|[#337](https://github.com/oap-project/gazelle_plugin/issues/337)|UDF: Add test case for validating basic row-based udf|
|[#326](https://github.com/oap-project/gazelle_plugin/issues/326)|Update Scala unit test to spark-3.1.1|

#### Performance
|||
|:---|:---|
|[#400](https://github.com/oap-project/gazelle_plugin/issues/400)|Optimize ColumnarToRow Operator in NSE.|
|[#411](https://github.com/oap-project/gazelle_plugin/issues/411)|enable ccache on C++ code compiling|

#### Bugs Fixed
|||
|:---|:---|
|[#358](https://github.com/oap-project/gazelle_plugin/issues/358)|Running TPC DS all queries with native-sql-engine for 10 rounds  will have performance degradation problems in the last few rounds|
|[#481](https://github.com/oap-project/gazelle_plugin/issues/481)|JVM heap memory leak on memory leak tracker facilities|
|[#436](https://github.com/oap-project/gazelle_plugin/issues/436)|Fix for Arrow Data Source test suite|
|[#317](https://github.com/oap-project/gazelle_plugin/issues/317)|persistent memory cache issue|
|[#382](https://github.com/oap-project/gazelle_plugin/issues/382)|Hadoop version conflict when supporting to use gazelle_plugin on Google Cloud Dataproc|
|[#384](https://github.com/oap-project/gazelle_plugin/issues/384)|ColumnarBatchScanExec reading parquet failed on java.lang.IllegalArgumentException: not all nodes and buffers were consumed|
|[#370](https://github.com/oap-project/gazelle_plugin/issues/370)|Failed to get time zone: NoSuchElementException: None.get|
|[#360](https://github.com/oap-project/gazelle_plugin/issues/360)|Cannot compile master branch.|
|[#341](https://github.com/oap-project/gazelle_plugin/issues/341)|build failed on v2 with -Phadoop-3.2|

#### PRs
|||
|:---|:---|
|[#489](https://github.com/oap-project/gazelle_plugin/pull/489)|[NSE-481] JVM heap memory leak on memory leak tracker facilities (Arrow Allocator)|
|[#486](https://github.com/oap-project/gazelle_plugin/pull/486)|[NSE-475] restore coalescebatches operator before window|
|[#482](https://github.com/oap-project/gazelle_plugin/pull/482)|[NSE-481] JVM heap memory leak on memory leak tracker facilities|
|[#470](https://github.com/oap-project/gazelle_plugin/pull/470)|[NSE-469] Lazy Read: Iterator objects are not correctly released|
|[#464](https://github.com/oap-project/gazelle_plugin/pull/464)|[NSE-460] fix decimal partial sum in 1.2 branch|
|[#439](https://github.com/oap-project/gazelle_plugin/pull/439)|[NSE-433]Support pre-built Jemalloc|
|[#453](https://github.com/oap-project/gazelle_plugin/pull/453)|[NSE-254] remove arrow-data-source-common from jar with dependency|
|[#452](https://github.com/oap-project/gazelle_plugin/pull/452)|[NSE-254]Fix redundant arrow library issue.|
|[#432](https://github.com/oap-project/gazelle_plugin/pull/432)|[NSE-429] TPC-DS Q14a/b get slowed down within setting spark.oap.sql.columnar.sortmergejoin.lazyread=true|
|[#426](https://github.com/oap-project/gazelle_plugin/pull/426)|[NSE-207] Fix aggregate and refresh UT test script|
|[#442](https://github.com/oap-project/gazelle_plugin/pull/442)|[NSE-254]Issue0410 jar size|
|[#441](https://github.com/oap-project/gazelle_plugin/pull/441)|[NSE-254]Issue0410 jar size|
|[#440](https://github.com/oap-project/gazelle_plugin/pull/440)|[NSE-254]Solve the redundant arrow library issue|
|[#437](https://github.com/oap-project/gazelle_plugin/pull/437)|[NSE-436] Fix for Arrow Data Source test suite|
|[#387](https://github.com/oap-project/gazelle_plugin/pull/387)|[NSE-383] Release SMJ input data immediately after being used|
|[#423](https://github.com/oap-project/gazelle_plugin/pull/423)|[NSE-417] fix sort spill on inplsace sort|
|[#416](https://github.com/oap-project/gazelle_plugin/pull/416)|[NSE-207] fix left/right outer join in SMJ|
|[#422](https://github.com/oap-project/gazelle_plugin/pull/422)|[NSE-421]Disable the wholestagecodegen feature for the ArrowColumnarToRow operator|
|[#369](https://github.com/oap-project/gazelle_plugin/pull/369)|[NSE-417] Sort spill support framework|
|[#401](https://github.com/oap-project/gazelle_plugin/pull/401)|[NSE-400] Optimize ColumnarToRow Operator in NSE.|
|[#413](https://github.com/oap-project/gazelle_plugin/pull/413)|[NSE-411] adding ccache support|
|[#393](https://github.com/oap-project/gazelle_plugin/pull/393)|[NSE-207] fix scala unit tests|
|[#407](https://github.com/oap-project/gazelle_plugin/pull/407)|[NSE-403]Add Dataproc integration section to README|
|[#406](https://github.com/oap-project/gazelle_plugin/pull/406)|[NSE-404]Modify repo name in documents|
|[#402](https://github.com/oap-project/gazelle_plugin/pull/402)|[NSE-368]Update emr-6.3.0 support|
|[#395](https://github.com/oap-project/gazelle_plugin/pull/395)|[NSE-394]Support ColumnarArrowEvalPython operator|
|[#346](https://github.com/oap-project/gazelle_plugin/pull/346)|[NSE-317]fix columnar cache|
|[#392](https://github.com/oap-project/gazelle_plugin/pull/392)|[NSE-382]Support GCP Dataproc 2.0|
|[#388](https://github.com/oap-project/gazelle_plugin/pull/388)|[NSE-382]Fix Hadoop version issue|
|[#385](https://github.com/oap-project/gazelle_plugin/pull/385)|[NSE-384] "Select count(*)" without group by results in error: java.lang.IllegalArgumentException: not all nodes and buffers were consumed|
|[#374](https://github.com/oap-project/gazelle_plugin/pull/374)|[NSE-207] fix left anti join and support filter wo/ project|
|[#376](https://github.com/oap-project/gazelle_plugin/pull/376)|[NSE-375] Implement a series of datetime functions|
|[#373](https://github.com/oap-project/gazelle_plugin/pull/373)|[NSE-183] fix timestamp in native side|
|[#356](https://github.com/oap-project/gazelle_plugin/pull/356)|[NSE-207] fix issues found in scala unit tests|
|[#371](https://github.com/oap-project/gazelle_plugin/pull/371)|[NSE-370] Failed to get time zone: NoSuchElementException: None.get|
|[#347](https://github.com/oap-project/gazelle_plugin/pull/347)|[NSE-183] Add Date/Timestamp type support|
|[#363](https://github.com/oap-project/gazelle_plugin/pull/363)|[NSE-362] use arrow-unsafe allocator by default|
|[#361](https://github.com/oap-project/gazelle_plugin/pull/361)|[NSE-273] Spark shim layer infrastructure|
|[#364](https://github.com/oap-project/gazelle_plugin/pull/364)|[NSE-360] fix ut compile and travis test|
|[#264](https://github.com/oap-project/gazelle_plugin/pull/264)|[NSE-207] fix issues found from join unit tests|
|[#344](https://github.com/oap-project/gazelle_plugin/pull/344)|[NSE-343]allow to config codegen opt level|
|[#342](https://github.com/oap-project/gazelle_plugin/pull/342)|[NSE-341] fix maven build failure|
|[#324](https://github.com/oap-project/gazelle_plugin/pull/324)|[NSE-223] Add Parquet write support to Arrow data source|
|[#321](https://github.com/oap-project/gazelle_plugin/pull/321)|[NSE-320] Add build option to enable unsafe Arrow allocator|
|[#299](https://github.com/oap-project/gazelle_plugin/pull/299)|[NSE-207] fix unsuppored types in aggregate|
|[#338](https://github.com/oap-project/gazelle_plugin/pull/338)|[NSE-337] UDF: Add test case for validating basic row-based udf|
|[#336](https://github.com/oap-project/gazelle_plugin/pull/336)|[NSE-333] Arrow Data Source: CSV format support fix|
|[#327](https://github.com/oap-project/gazelle_plugin/pull/327)|[NSE-326] update scala unit tests to spark-3.1.1|

### OAP MLlib

#### Features
|||
|:---|:---|
|[#110](https://github.com/oap-project/oap-mllib/issues/110)|Update isOAPEnabled for Kmeans, PCA & ALS|
|[#108](https://github.com/oap-project/oap-mllib/issues/108)|Update PCA GPU, LiR CPU and Improve JAR packaging and libs loading|
|[#93](https://github.com/oap-project/oap-mllib/issues/93)|[GPU] Add GPU support for PCA|
|[#101](https://github.com/oap-project/oap-mllib/issues/101)|[Release] Add version update scripts and improve scripts for examples|
|[#76](https://github.com/oap-project/oap-mllib/issues/76)|Reorganize Spark version specific code structure|
|[#82](https://github.com/oap-project/oap-mllib/issues/82)|[Tests] Add NaiveBayes test and refactors|

#### Bugs Fixed
|||
|:---|:---|
|[#119](https://github.com/oap-project/oap-mllib/issues/119)|[SDLe][Klocwork] Security vulnerabilities found by static code scan|
|[#121](https://github.com/oap-project/oap-mllib/issues/121)|Meeting freeing memory issue after the training stage when using Intel-MLlib to run PCA and K-means algorithms.|
|[#122](https://github.com/oap-project/oap-mllib/issues/122)|Cannot run K-means and PCA algorithm with oap-mllib on Google Dataproc|
|[#123](https://github.com/oap-project/oap-mllib/issues/123)|[Core] Improve locality handling for native lib loading|
|[#116](https://github.com/oap-project/oap-mllib/issues/116)|Cannot run ALS algorithm with oap-mllib thanks to the commit "2883d3447d07feb55bf5d4fee8225d74b0b1e2b1"|
|[#114](https://github.com/oap-project/oap-mllib/issues/114)|[Core] Improve native lib loading|
|[#94](https://github.com/oap-project/oap-mllib/issues/94)|Failed to run KMeans workload with oap-mllib in JLSE|
|[#95](https://github.com/oap-project/oap-mllib/issues/95)|Some shared libs are missing in 1.1.1 release|
|[#105](https://github.com/oap-project/oap-mllib/issues/105)|[Core] crash when libfabric version conflict|
|[#98](https://github.com/oap-project/oap-mllib/issues/98)|[SDLe][Klocwork] Security vulnerabilities found by static code scan|
|[#88](https://github.com/oap-project/oap-mllib/issues/88)|[Test] Fix ALS Suite "ALS shuffle cleanup standalone"|
|[#86](https://github.com/oap-project/oap-mllib/issues/86)|[NaiveBayes] Fix isOAPEnabled and add multi-version support|

#### PRs
|||
|:---|:---|
|[#124](https://github.com/oap-project/oap-mllib/pull/124)|[ML-123][Core] Improve locality handling for native lib loading|
|[#118](https://github.com/oap-project/oap-mllib/pull/118)|[ML-116] use getOneCCLIPPort and fix lib loading|
|[#115](https://github.com/oap-project/oap-mllib/pull/115)|[ML-114] [Core] Improve native lib loading|
|[#113](https://github.com/oap-project/oap-mllib/pull/113)|[ML-110] Update isOAPEnabled for Kmeans, PCA & ALS|
|[#112](https://github.com/oap-project/oap-mllib/pull/112)|[ML-105][Core] Fix crash when libfabric version conflict|
|[#111](https://github.com/oap-project/oap-mllib/pull/111)|[ML-108] Update PCA GPU, LiR CPU and Improve JAR packaging and libs loading|
|[#104](https://github.com/oap-project/oap-mllib/pull/104)|[ML-93][GPU] Add GPU support for PCA|
|[#103](https://github.com/oap-project/oap-mllib/pull/103)|[ML-98] [Release] Clean Service.java code|
|[#102](https://github.com/oap-project/oap-mllib/pull/102)|[ML-101] [Release] Add version update scripts and improve scripts for examples|
|[#90](https://github.com/oap-project/oap-mllib/pull/90)|[ML-88][Test] Fix ALS Suite "ALS shuffle cleanup standalone"|
|[#87](https://github.com/oap-project/oap-mllib/pull/87)|[ML-86][NaiveBayes] Fix isOAPEnabled and add multi-version support|
|[#83](https://github.com/oap-project/oap-mllib/pull/83)|[ML-82] [Tests] Add NaiveBayes test and refactors|
|[#75](https://github.com/oap-project/oap-mllib/pull/75)|[ML-53] [CPU] Add Linear & Ridge Regression|
|[#77](https://github.com/oap-project/oap-mllib/pull/77)|[ML-76] Reorganize multiple Spark version support code structure|
|[#68](https://github.com/oap-project/oap-mllib/pull/68)|[ML-55] [CPU] Add Naive Bayes|
|[#64](https://github.com/oap-project/oap-mllib/pull/64)|[ML-42] [PIP] Misc improvements and refactor code|
|[#62](https://github.com/oap-project/oap-mllib/pull/62)|[ML-30][Coding Style] Add code style rules & scripts for Scala, Java and C++|

### SQL DS Cache

#### Features
|||
|:---|:---|
|[#155](https://github.com/oap-project/sql-ds-cache/issues/155)|reorg to support profile based multi spark version|

#### Bugs Fixed
|||
|:---|:---|
|[#190](https://github.com/oap-project/sql-ds-cache/issues/190)|The function of vmem-cache and guava-cache should not be associated with arrow.|
|[#181](https://github.com/oap-project/sql-ds-cache/issues/181)|[SDLe]Vulnerabilities scanned by Snyk|

#### PRs
|||
|:---|:---|
|[#182](https://github.com/oap-project/sql-ds-cache/pull/182)|[SQL-DS-CACHE-181][SDLe]Fix Snyk code scan issues|
|[#191](https://github.com/oap-project/sql-ds-cache/pull/191)|[SQL-DS-CACHE-190]put plasma detector in seperate object to avoid unnecessary dependency of arrow|
|[#189](https://github.com/oap-project/sql-ds-cache/pull/189)|[SQL-DS-CACHE-188][POAE7-1253] improvement of fallback from plasma cache to simple cache|
|[#157](https://github.com/oap-project/sql-ds-cache/pull/157)|[SQL-DS-CACHE-155][POAE7-1187]reorg to support profile based multi spark version|

### PMem Shuffle

#### Bugs Fixed
|||
|:---|:---|
|[#46](https://github.com/oap-project/pmem-shuffle/issues/46)|Cannot run Terasort with pmem-shuffle of branch-1.2|
|[#43](https://github.com/oap-project/pmem-shuffle/issues/43)|Rpmp cannot be compiled due to the lack of boost header file.|

#### PRs
|||
|:---|:---|
|[#51](https://github.com/oap-project/pmem-shuffle/pull/51)|[PMEM-SHUFFLE-50] Remove description about download submodules manually since they can be downloaded automatically.|
|[#49](https://github.com/oap-project/pmem-shuffle/pull/49)|[PMEM-SHUFFLE-48] Fix the bug about mapstatus tracking and add more connections for metastore.|
|[#47](https://github.com/oap-project/pmem-shuffle/pull/47)|[PMEM-SHUFFLE-46] Fix the bug that off-heap memory is over used in shuffle reduce stage. |
|[#40](https://github.com/oap-project/pmem-shuffle/pull/40)|[PMEM-SHUFFLE-39] Fix the bug that pmem-shuffle without RPMP fails to pass Terasort benchmark due to latest patch.|
|[#38](https://github.com/oap-project/pmem-shuffle/pull/38)|[PMEM-SHUFFLE-37] Add start-rpmp.sh and stop-rpmp.sh|
|[#33](https://github.com/oap-project/pmem-shuffle/pull/33)|[PMEM-SHUFFLE-28]Add RPMP with HA support and integrate it with Spark3.1.1|
|[#27](https://github.com/oap-project/pmem-shuffle/pull/27)|[PMEM-SHUFFLE] Change artifact name to make it compatible with naming…|

### Remote Shuffle

#### Bugs Fixed
|||
|:---|:---|
|[#24](https://github.com/oap-project/remote-shuffle/issues/24)|Enhance executor memory release|

#### PRs
|||
|:---|:---|
|[#25](https://github.com/oap-project/remote-shuffle/pull/25)|[REMOTE-SHUFFLE-24] Enhance executor memory release|


## Release 1.1.1

### Native SQL Engine

#### Features
|||
|:---|:---|
|[#304](https://github.com/oap-project/native-sql-engine/issues/304)|Upgrade to Arrow 4.0.0|
|[#285](https://github.com/oap-project/native-sql-engine/issues/285)|ColumnarWindow: Support Date/Timestamp input in MAX/MIN|
|[#297](https://github.com/oap-project/native-sql-engine/issues/297)|Disable incremental compiler in CI|
|[#245](https://github.com/oap-project/native-sql-engine/issues/245)|Support columnar rdd cache|
|[#276](https://github.com/oap-project/native-sql-engine/issues/276)|Add option to switch Hadoop version|
|[#274](https://github.com/oap-project/native-sql-engine/issues/274)|Comment to trigger tpc-h RAM test|
|[#256](https://github.com/oap-project/native-sql-engine/issues/256)|CI: do not run ram report for each PR|

#### Bugs Fixed
|||
|:---|:---|
|[#325](https://github.com/oap-project/native-sql-engine/issues/325)|java.util.ConcurrentModificationException: mutation occurred during iteration|
|[#329](https://github.com/oap-project/native-sql-engine/issues/329)|numPartitions are not the same|
|[#318](https://github.com/oap-project/native-sql-engine/issues/318)|fix Spark 311 on data source v2|
|[#311](https://github.com/oap-project/native-sql-engine/issues/311)|Build reports errors|
|[#302](https://github.com/oap-project/native-sql-engine/issues/302)|test on v2 failed due to an exception|
|[#257](https://github.com/oap-project/native-sql-engine/issues/257)|different version of slf4j-log4j|
|[#293](https://github.com/oap-project/native-sql-engine/issues/293)|Fix BHJ loss if key = 0|
|[#248](https://github.com/oap-project/native-sql-engine/issues/248)|arrow dependency must put after arrow installation|

#### PRs
|||
|:---|:---|
|[#332](https://github.com/oap-project/native-sql-engine/pull/332)|[NSE-325] fix incremental compile issue with 4.5.x scala-maven-plugin|
|[#335](https://github.com/oap-project/native-sql-engine/pull/335)|[NSE-329] fix out partitioning in BHJ and SHJ|
|[#328](https://github.com/oap-project/native-sql-engine/pull/328)|[NSE-318]check schema before reuse exchange|
|[#307](https://github.com/oap-project/native-sql-engine/pull/307)|[NSE-304] Upgrade to Arrow 4.0.0|
|[#312](https://github.com/oap-project/native-sql-engine/pull/312)|[NSE-311] Build reports errors|
|[#272](https://github.com/oap-project/native-sql-engine/pull/272)|[NSE-273] support spark311|
|[#303](https://github.com/oap-project/native-sql-engine/pull/303)|[NSE-302] fix v2 test|
|[#306](https://github.com/oap-project/native-sql-engine/pull/306)|[NSE-304] Upgrade to Arrow 4.0.0: Change basic GHA TPC-H test target …|
|[#286](https://github.com/oap-project/native-sql-engine/pull/286)|[NSE-285] ColumnarWindow: Support Date input in MAX/MIN|
|[#298](https://github.com/oap-project/native-sql-engine/pull/298)|[NSE-297] Disable incremental compiler in GHA CI|
|[#291](https://github.com/oap-project/native-sql-engine/pull/291)|[NSE-257] fix multiple slf4j bindings|
|[#294](https://github.com/oap-project/native-sql-engine/pull/294)|[NSE-293] fix unsafemap with key = '0'|
|[#233](https://github.com/oap-project/native-sql-engine/pull/233)|[NSE-207] fix issues found from aggregate unit tests|
|[#246](https://github.com/oap-project/native-sql-engine/pull/246)|[NSE-245]Adding columnar RDD cache support|
|[#289](https://github.com/oap-project/native-sql-engine/pull/289)|[NSE-206]Update installation guide and configuration guide.|
|[#277](https://github.com/oap-project/native-sql-engine/pull/277)|[NSE-276] Add option to switch Hadoop version|
|[#275](https://github.com/oap-project/native-sql-engine/pull/275)|[NSE-274] Comment to trigger tpc-h RAM test|
|[#271](https://github.com/oap-project/native-sql-engine/pull/271)|[NSE-196] clean up configs in unit tests|
|[#258](https://github.com/oap-project/native-sql-engine/pull/258)|[NSE-257] fix different versions of slf4j-log4j12|
|[#259](https://github.com/oap-project/native-sql-engine/pull/259)|[NSE-248] fix arrow dependency order|
|[#249](https://github.com/oap-project/native-sql-engine/pull/249)|[NSE-241] fix hashagg result length|
|[#255](https://github.com/oap-project/native-sql-engine/pull/255)|[NSE-256] do not run ram report test on each PR|


### SQL DS Cache

#### Features
|||
|:---|:---|
|[#118](https://github.com/oap-project/sql-ds-cache/issues/118)|port to Spark 3.1.1|

#### Bugs Fixed
|||
|:---|:---|
|[#121](https://github.com/oap-project/sql-ds-cache/issues/121)|OAP Index creation stuck issue|

#### PRs
|||
|:---|:---|
|[#132](https://github.com/oap-project/sql-ds-cache/pull/132)|Fix SampleBasedStatisticsSuite UnitTest case|
|[#122](https://github.com/oap-project/sql-ds-cache/pull/122)|[ sql-ds-cache-121] Fix Index stuck issues|
|[#119](https://github.com/oap-project/sql-ds-cache/pull/119)|[SQL-DS-CACHE-118][POAE7-1130] port sql-ds-cache to Spark3.1.1|


### OAP MLlib

#### Features
|||
|:---|:---|
|[#26](https://github.com/oap-project/oap-mllib/issues/26)|[PIP] Support Spark 3.0.1 / 3.0.2 and upcoming 3.1.1|

#### PRs
|||
|:---|:---|
|[#39](https://github.com/oap-project/oap-mllib/pull/39)|[ML-26] Build for different spark version by -Pprofile|


### PMem Spill

#### Features
|||
|:---|:---|
|[#34](https://github.com/oap-project/pmem-spill/issues/34)|Support vanilla  spark 3.1.1|

#### PRs
|||
|:---|:---|
|[#41](https://github.com/oap-project/pmem-spill/pull/41)|[PMEM-SPILL-34][POAE7-1119]Port RDD cache to Spark 3.1.1 as separate module|


### PMem Common

#### Features
|||
|:---|:---|
|[#10](https://github.com/oap-project/pmem-common/issues/10)|add -mclflushopt flag to enable clflushopt for gcc|
|[#8](https://github.com/oap-project/pmem-common/issues/8)|use clflushopt instead of clflush |

#### PRs
|||
|:---|:---|
|[#11](https://github.com/oap-project/pmem-common/pull/11)|[PMEM-COMMON-10][POAE7-1010]Add -mclflushopt flag to enable clflushop…|
|[#9](https://github.com/oap-project/pmem-common/pull/9)|[PMEM-COMMON-8][POAE7-896]use clflush optimize version for clflush|


### PMem Shuffle

#### Features
|||
|:---|:---|
|[#15](https://github.com/oap-project/pmem-shuffle/issues/15)|Doesn't work with Spark3.1.1|

#### PRs
|||
|:---|:---|
|[#16](https://github.com/oap-project/pmem-shuffle/pull/16)|[pmem-shuffle-15] Make pmem-shuffle support Spark3.1.1|


### Remote Shuffle

#### Features
|||
|:---|:---|
|[#18](https://github.com/oap-project/remote-shuffle/issues/18)|upgrade to Spark-3.1.1|
|[#11](https://github.com/oap-project/remote-shuffle/issues/11)|Support DAOS Object Async API|

#### PRs
|||
|:---|:---|
|[#19](https://github.com/oap-project/remote-shuffle/pull/19)|[REMOTE-SHUFFLE-18] upgrade to Spark-3.1.1|
|[#14](https://github.com/oap-project/remote-shuffle/pull/14)|[REMOTE-SHUFFLE-11] Support DAOS Object Async API|



## Release 1.1.0

### Native SQL Engine

#### Features
|||
|:---|:---|
|[#261](https://github.com/oap-project/native-sql-engine/issues/261)|ArrowDataSource: Add S3 Support|
|[#239](https://github.com/oap-project/native-sql-engine/issues/239)|Adopt ARROW-7011|
|[#62](https://github.com/oap-project/native-sql-engine/issues/62)|Support Arrow's Build from Source and Package dependency library in the jar|
|[#145](https://github.com/oap-project/native-sql-engine/issues/145)|Support decimal in columnar window|
|[#31](https://github.com/oap-project/native-sql-engine/issues/31)|Decimal data type support|
|[#128](https://github.com/oap-project/native-sql-engine/issues/128)|Support Decimal in Aggregate|
|[#130](https://github.com/oap-project/native-sql-engine/issues/130)|Support decimal in project|
|[#134](https://github.com/oap-project/native-sql-engine/issues/134)|Update input metrics during reading|
|[#120](https://github.com/oap-project/native-sql-engine/issues/120)|Columnar window: Reduce peak memory usage and fix performance issues|
|[#108](https://github.com/oap-project/native-sql-engine/issues/108)|Add end-to-end test suite against TPC-DS|
|[#68](https://github.com/oap-project/native-sql-engine/issues/68)|Adaptive compression select in Shuffle.|
|[#97](https://github.com/oap-project/native-sql-engine/issues/97)|optimize null check in codegen sort|
|[#29](https://github.com/oap-project/native-sql-engine/issues/29)|Support mutiple-key sort without codegen|
|[#75](https://github.com/oap-project/native-sql-engine/issues/75)|Support HashAggregate in ColumnarWSCG|
|[#73](https://github.com/oap-project/native-sql-engine/issues/73)|improve columnar SMJ|
|[#51](https://github.com/oap-project/native-sql-engine/issues/51)|Decimal fallback|
|[#38](https://github.com/oap-project/native-sql-engine/issues/38)|Supporting expression as join keys in columnar SMJ|
|[#27](https://github.com/oap-project/native-sql-engine/issues/27)|Support REUSE exchange when DPP enabled|
|[#17](https://github.com/oap-project/native-sql-engine/issues/17)|ColumnarWSCG further optimization|

#### Performance
|||
|:---|:---|
|[#194](https://github.com/oap-project/native-sql-engine/issues/194)|Arrow Parameters Update when compiling Arrow|
|[#136](https://github.com/oap-project/native-sql-engine/issues/136)|upgrade to arrow 3.0|
|[#103](https://github.com/oap-project/native-sql-engine/issues/103)|reduce codegen in multiple-key sort|
|[#90](https://github.com/oap-project/native-sql-engine/issues/90)|Refine HashAggregate to do everything in CPP|

#### Bugs Fixed
|||
|:---|:---|
|[#278](https://github.com/oap-project/native-sql-engine/issues/278)|fix arrow dep in 1.1 branch|
|[#265](https://github.com/oap-project/native-sql-engine/issues/265)|TPC-DS Q67 failed with memmove exception in native split code.|
|[#280](https://github.com/oap-project/native-sql-engine/issues/280)|CMake version check|
|[#241](https://github.com/oap-project/native-sql-engine/issues/241)|TPC-DS q67 failed for XXH3_hashLong_64b_withSecret.constprop.0+0x180|
|[#262](https://github.com/oap-project/native-sql-engine/issues/262)|q18 has different digits compared with vanilla spark|
|[#196](https://github.com/oap-project/native-sql-engine/issues/196)|clean up options for native sql engine|
|[#224](https://github.com/oap-project/native-sql-engine/issues/224)|update 3rd party libs|
|[#227](https://github.com/oap-project/native-sql-engine/issues/227)|fix vulnerabilities from klockwork|
|[#237](https://github.com/oap-project/native-sql-engine/issues/237)|Add ARROW_CSV=ON to default C++ build commands|
|[#229](https://github.com/oap-project/native-sql-engine/issues/229)|Fix the deprecated code warning in shuffle_split_test|
|[#119](https://github.com/oap-project/native-sql-engine/issues/119)|consolidate batch size|
|[#217](https://github.com/oap-project/native-sql-engine/issues/217)|TPC-H query20 result not correct when use decimal dataset|
|[#211](https://github.com/oap-project/native-sql-engine/issues/211)|IndexOutOfBoundsException during running TPC-DS Q2|
|[#167](https://github.com/oap-project/native-sql-engine/issues/167)|Cannot successfully run q.14a.sql and q14b.sql when using double format for TPC-DS workload.|
|[#191](https://github.com/oap-project/native-sql-engine/issues/191)|libarrow.so and libgandiva.so not copy into the tmp directory|
|[#179](https://github.com/oap-project/native-sql-engine/issues/179)|Unable to find Arrow headers during build|
|[#153](https://github.com/oap-project/native-sql-engine/issues/153)|Fix incorrect queries after enabled Decimal|
|[#173](https://github.com/oap-project/native-sql-engine/issues/173)|fix the incorrect result of q69|
|[#48](https://github.com/oap-project/native-sql-engine/issues/48)|unit tests for c++ are broken|
|[#101](https://github.com/oap-project/native-sql-engine/issues/101)|ColumnarWindow: Remove obsolete debug code|
|[#100](https://github.com/oap-project/native-sql-engine/issues/100)|Incorrect result in Q45 w/ v2 bhj threshold is 10MB sf500|
|[#81](https://github.com/oap-project/native-sql-engine/issues/81)|Some ArrowVectorWriter implementations doesn't implement setNulls method|
|[#82](https://github.com/oap-project/native-sql-engine/issues/82)|Incorrect result in TPCDS Q72 SF1536|
|[#70](https://github.com/oap-project/native-sql-engine/issues/70)|Duplicate IsNull check in codegen sort|
|[#64](https://github.com/oap-project/native-sql-engine/issues/64)|Memleak in sort when SMJ is disabled|
|[#58](https://github.com/oap-project/native-sql-engine/issues/58)|Issues when running tpcds with DPP enabled and AQE disabled |
|[#52](https://github.com/oap-project/native-sql-engine/issues/52)|memory leakage in columnar SMJ|
|[#53](https://github.com/oap-project/native-sql-engine/issues/53)|Q24a/Q24b SHJ tail task took about 50 secs in SF1500|
|[#42](https://github.com/oap-project/native-sql-engine/issues/42)|reduce columnar sort memory footprint|
|[#40](https://github.com/oap-project/native-sql-engine/issues/40)|columnar sort codegen fallback to executor side|
|[#1](https://github.com/oap-project/native-sql-engine/issues/1)|columnar whole stage codegen failed due to empty results|
|[#23](https://github.com/oap-project/native-sql-engine/issues/23)|TPC-DS Q8 failed due to unsupported operation in columnar sortmergejoin|
|[#22](https://github.com/oap-project/native-sql-engine/issues/22)|TPC-DS Q95 failed due in columnar wscg|
|[#4](https://github.com/oap-project/native-sql-engine/issues/4)|columnar BHJ failed on new memory pool|
|[#5](https://github.com/oap-project/native-sql-engine/issues/5)|columnar BHJ failed on partitioned table with prefercolumnar=false|

#### PRs
|||
|:---|:---|
|[#288](https://github.com/oap-project/native-sql-engine/pull/288)|[NSE-119] clean up on comments|
|[#282](https://github.com/oap-project/native-sql-engine/pull/282)|[NSE-280]fix cmake version check|
|[#281](https://github.com/oap-project/native-sql-engine/pull/281)|[NSE-280] bump cmake to 3.16|
|[#279](https://github.com/oap-project/native-sql-engine/pull/279)|[NSE-278]fix arrow dep in 1.1 branch|
|[#268](https://github.com/oap-project/native-sql-engine/pull/268)|[NSE-186] backport to 1.1 branch|
|[#266](https://github.com/oap-project/native-sql-engine/pull/266)|[NSE-265] Reserve enough memory before UnsafeAppend in builder|
|[#270](https://github.com/oap-project/native-sql-engine/pull/270)|[NSE-261] ArrowDataSource: Add S3 Support|
|[#263](https://github.com/oap-project/native-sql-engine/pull/263)|[NSE-262] fix remainer loss in decimal divide|
|[#215](https://github.com/oap-project/native-sql-engine/pull/215)|[NSE-196] clean up native sql options|
|[#231](https://github.com/oap-project/native-sql-engine/pull/231)|[NSE-176]Arrow install order issue|
|[#242](https://github.com/oap-project/native-sql-engine/pull/242)|[NSE-224] update third party code|
|[#240](https://github.com/oap-project/native-sql-engine/pull/240)|[NSE-239] Adopt ARROW-7011|
|[#238](https://github.com/oap-project/native-sql-engine/pull/238)|[NSE-237] Add ARROW_CSV=ON to default C++ build commands|
|[#230](https://github.com/oap-project/native-sql-engine/pull/230)|[NSE-229] Fix the deprecated code warning in shuffle_split_test|
|[#225](https://github.com/oap-project/native-sql-engine/pull/225)|[NSE-227]fix issues from codescan|
|[#219](https://github.com/oap-project/native-sql-engine/pull/219)|[NSE-217] fix missing decimal check|
|[#212](https://github.com/oap-project/native-sql-engine/pull/212)|[NSE-211] IndexOutOfBoundsException during running TPC-DS Q2|
|[#187](https://github.com/oap-project/native-sql-engine/pull/187)|[NSE-185] Avoid unnecessary copying when simply projecting on fields|
|[#195](https://github.com/oap-project/native-sql-engine/pull/195)|[NSE-194]Turn on several Arrow parameters|
|[#189](https://github.com/oap-project/native-sql-engine/pull/189)|[NSE-153] Following NSE-153, optimize fallback conditions for columnar window|
|[#192](https://github.com/oap-project/native-sql-engine/pull/192)|[NSE-191]Fix issue0191 for .so file copy to tmp.|
|[#181](https://github.com/oap-project/native-sql-engine/pull/181)|[NSE-179]Fix arrow include directory not include when using ARROW_ROOT|
|[#175](https://github.com/oap-project/native-sql-engine/pull/175)|[NSE-153] Fix window results|
|[#174](https://github.com/oap-project/native-sql-engine/pull/174)|[NSE-173] fix incorrect result of q69|
|[#172](https://github.com/oap-project/native-sql-engine/pull/172)|[NSE-62]Fixing issue0062 for package arrow dependencies in jar with refresh2|
|[#171](https://github.com/oap-project/native-sql-engine/pull/171)|[NSE-170]improve sort shuffle code|
|[#165](https://github.com/oap-project/native-sql-engine/pull/165)|[NSE-161] adding format check|
|[#166](https://github.com/oap-project/native-sql-engine/pull/166)|[NSE-130] support decimal round and abs|
|[#164](https://github.com/oap-project/native-sql-engine/pull/164)|[NSE-130] fix precision loss in divide w/ decimal type|
|[#159](https://github.com/oap-project/native-sql-engine/pull/159)|[NSE-31] fix SMJ divide with decimal|
|[#156](https://github.com/oap-project/native-sql-engine/pull/156)|[NSE-130] fix overflow and precision loss|
|[#152](https://github.com/oap-project/native-sql-engine/pull/152)|[NSE-86] Merge Arrow Data Source|
|[#154](https://github.com/oap-project/native-sql-engine/pull/154)|[NSE-153] Fix incorrect quries after enabled Decimal|
|[#151](https://github.com/oap-project/native-sql-engine/pull/151)|[NSE-145] Support decimal in columnar window|
|[#129](https://github.com/oap-project/native-sql-engine/pull/129)|[NSE-128]Support Decimal in Aggregate/HashJoin|
|[#131](https://github.com/oap-project/native-sql-engine/pull/131)|[NSE-130] support decimal in project|
|[#107](https://github.com/oap-project/native-sql-engine/pull/107)|[NSE-136]upgrade to arrow 3.0.0|
|[#135](https://github.com/oap-project/native-sql-engine/pull/135)|[NSE-134] Update input metrics during reading|
|[#121](https://github.com/oap-project/native-sql-engine/pull/121)|[NSE-120] Columnar window: Reduce peak memory usage and fix performance issues|
|[#112](https://github.com/oap-project/native-sql-engine/pull/112)|[NSE-97] optimize null check and refactor sort kernels|
|[#109](https://github.com/oap-project/native-sql-engine/pull/109)|[NSE-108] Add end-to-end test suite against TPC-DS|
|[#69](https://github.com/oap-project/native-sql-engine/pull/69)|[NSE-68][Shuffle] Adaptive compression select in Shuffle.|
|[#98](https://github.com/oap-project/native-sql-engine/pull/98)|[NSE-97] remove isnull when null count is zero|
|[#102](https://github.com/oap-project/native-sql-engine/pull/102)|[NSE-101] ColumnarWindow: Remove obsolete debug code|
|[#105](https://github.com/oap-project/native-sql-engine/pull/105)|[NSE-100]Fix an incorrect result error when using SHJ in Q45|
|[#91](https://github.com/oap-project/native-sql-engine/pull/91)|[NSE-90]Refactor HashAggregateExec and CPP kernels|
|[#79](https://github.com/oap-project/native-sql-engine/pull/79)|[NSE-81] add missing setNulls methods in ArrowWritableColumnVector|
|[#44](https://github.com/oap-project/native-sql-engine/pull/44)|[NSE-29]adding non-codegen framework for multiple-key sort|
|[#76](https://github.com/oap-project/native-sql-engine/pull/76)|[NSE-75]Support ColumnarHashAggregate in ColumnarWSCG|
|[#83](https://github.com/oap-project/native-sql-engine/pull/83)|[NSE-82] Fix Q72 SF1536 incorrect result|
|[#72](https://github.com/oap-project/native-sql-engine/pull/72)|[NSE-51] add more datatype fallback logic in columnar operators|
|[#60](https://github.com/oap-project/native-sql-engine/pull/60)|[NSE-48] fix c++ unit tests|
|[#50](https://github.com/oap-project/native-sql-engine/pull/50)|[NSE-45] BHJ memory leak|
|[#74](https://github.com/oap-project/native-sql-engine/pull/74)|[NSE-73]using data ref in multiple keys based SMJ|
|[#71](https://github.com/oap-project/native-sql-engine/pull/71)|[NSE-70] remove duplicate IsNull check in sort|
|[#65](https://github.com/oap-project/native-sql-engine/pull/65)|[NSE-64] fix memleak in sort when SMJ is disabled|
|[#59](https://github.com/oap-project/native-sql-engine/pull/59)|[NSE-58]Fix empty input issue when DPP enabled|
|[#7](https://github.com/oap-project/native-sql-engine/pull/7)|[OAP-1846][oap-native-sql] add more fallback logic |
|[#57](https://github.com/oap-project/native-sql-engine/pull/57)|[NSE-56]ColumnarSMJ: fallback on full outer join|
|[#55](https://github.com/oap-project/native-sql-engine/pull/55)|[NSE-52]Columnar SMJ: fix memory leak by closing stream batches properly|
|[#54](https://github.com/oap-project/native-sql-engine/pull/54)|[NSE-53]Partial fix Q24a/Q24b tail SHJ task materialization performance issue|
|[#47](https://github.com/oap-project/native-sql-engine/pull/47)|[NSE-17]TPCDS Q72 optimization|
|[#39](https://github.com/oap-project/native-sql-engine/pull/39)|[NSE-38]ColumnarSMJ: support expression as join keys|
|[#43](https://github.com/oap-project/native-sql-engine/pull/43)|[NSE-42] early release sort input|
|[#33](https://github.com/oap-project/native-sql-engine/pull/33)|[NSE-32] Use Spark managed spill in columnar shuffle|
|[#41](https://github.com/oap-project/native-sql-engine/pull/41)|[NSE-40] fixes driver failing to do sort codege|
|[#28](https://github.com/oap-project/native-sql-engine/pull/28)|[NSE-27]Reuse exchage to optimize DPP performance|
|[#36](https://github.com/oap-project/native-sql-engine/pull/36)|[NSE-1]fix columnar wscg on empty recordbatch|
|[#24](https://github.com/oap-project/native-sql-engine/pull/24)|[NSE-23]fix columnar SMJ fallback|
|[#26](https://github.com/oap-project/native-sql-engine/pull/26)|[NSE-22]Fix w/DPP issue when inside wscg smj both sides are smj|
|[#18](https://github.com/oap-project/native-sql-engine/pull/18)|[NSE-17] smjwscg optimization:|
|[#3](https://github.com/oap-project/native-sql-engine/pull/3)|[NSE-4]fix columnar BHJ on new memory pool|
|[#6](https://github.com/oap-project/native-sql-engine/pull/6)|[NSE-5][SCALA] Fix ColumnarBroadcastExchange didn't fallback issue w/ DPP|


### SQL DS Cache

#### Features
|||
|:---|:---|
|[#36](https://github.com/oap-project/sql-ds-cache/issues/36)|HCFS doc for Spark|
|[#38](https://github.com/oap-project/sql-ds-cache/issues/38)|update Plasma dependency for Plasma-based-cache module|
|[#14](https://github.com/oap-project/sql-ds-cache/issues/14)|Add HCFS module|
|[#17](https://github.com/oap-project/sql-ds-cache/issues/17)|replace arrow-plasma dependency for hcfs module|

#### Bugs Fixed
|||
|:---|:---|
|[#62](https://github.com/oap-project/sql-ds-cache/issues/62)|Upgrade hadoop dependencies in HCFS|

#### PRs
|||
|:---|:---|
|[#83](https://github.com/oap-project/sql-ds-cache/pull/83)|[SQL-DS-CACHE-82][SDLe]Upgrade Jetty version|
|[#77](https://github.com/oap-project/sql-ds-cache/pull/77)|[SQL-DS-CACHE-62][POAE7-984] upgrade hadoop version to 3.3.0|
|[#56](https://github.com/oap-project/sql-ds-cache/pull/56)|[SQL-DS-CACHE-47]Add plasma native get timeout|
|[#37](https://github.com/oap-project/sql-ds-cache/pull/37)|[SQL-DS-CACHE-36][POAE7-898]HCFS docs for OAP 1.1|
|[#39](https://github.com/oap-project/sql-ds-cache/pull/39)|[SQL-DS-CACHE-38][POAE7-892]update Plasma dependency|
|[#18](https://github.com/oap-project/sql-ds-cache/pull/18)|[SQL-DS-CACHE-17][POAE7-905]replace intel-arrow with apache-arrow v3.0.0|
|[#13](https://github.com/oap-project/sql-ds-cache/pull/13)|[SQL-DS-CACHE-14][POAE7-847] Port HCFS to OAP|
|[#16](https://github.com/oap-project/sql-ds-cache/pull/16)|[SQL-DS-CACHE-15][POAE7-869]Refactor original code to make it a sub-module|


### OAP MLlib

#### Features
|||
|:---|:---|
|[#35](https://github.com/oap-project/oap-mllib/issues/35)|Restrict printNumericTable to first 10 eigenvalues with first 20 dimensions|
|[#33](https://github.com/oap-project/oap-mllib/issues/33)|Optimize oneCCL port detecting|
|[#28](https://github.com/oap-project/oap-mllib/issues/28)|Use getifaddrs to get host ips for oneCCL kvs|
|[#12](https://github.com/oap-project/oap-mllib/issues/12)|Improve CI and add pseudo cluster testing|
|[#31](https://github.com/oap-project/oap-mllib/issues/31)|Print time duration for each PCA step|
|[#13](https://github.com/oap-project/oap-mllib/issues/13)|Add ALS with new oneCCL APIs|
|[#18](https://github.com/oap-project/oap-mllib/issues/18)|Auto detect KVS port for oneCCL to avoid port conflict|
|[#10](https://github.com/oap-project/oap-mllib/issues/10)|Porting Kmeans and PCA to new oneCCL API|

#### Bugs Fixed
|||
|:---|:---|
|[#43](https://github.com/oap-project/oap-mllib/issues/43)|[Release] Error when installing intel-oneapi-dal-devel-2021.1.1 intel-oneapi-tbb-devel-2021.1.1|
|[#46](https://github.com/oap-project/oap-mllib/issues/46)|[Release] Meet hang issue when running PCA algorithm.|
|[#48](https://github.com/oap-project/oap-mllib/issues/48)|[Release] No performance benefit when using Intel-MLlib to run ALS algorithm.|
|[#25](https://github.com/oap-project/oap-mllib/issues/25)|Fix oneCCL KVS port auto detect and improve logging|

#### PRs
|||
|:---|:---|
|[#51](https://github.com/oap-project/oap-mllib/pull/51)|[ML-50] Merge #47 and prepare for OAP 1.1|
|[#49](https://github.com/oap-project/oap-mllib/pull/49)|Revert "[ML-41] Revert to old oneCCL and Prepare for OAP 1.1"|
|[#47](https://github.com/oap-project/oap-mllib/pull/47)|[ML-44] [PIP] Update to oneAPI 2021.2 and Rework examples for validation|
|[#40](https://github.com/oap-project/oap-mllib/pull/40)|[ML-41] Revert to old oneCCL and Prepare for OAP 1.1|
|[#36](https://github.com/oap-project/oap-mllib/pull/36)|[ML-35] Restrict printNumericTable to first 10 eigenvalues with first 20 dimensions|
|[#34](https://github.com/oap-project/oap-mllib/pull/34)|[ML-33] Optimize oneCCL port detecting|
|[#20](https://github.com/oap-project/oap-mllib/pull/20)|[ML-12] Improve CI and add pseudo cluster testing|
|[#32](https://github.com/oap-project/oap-mllib/pull/32)|[ML-31] Print time duration for each PCA step|
|[#14](https://github.com/oap-project/oap-mllib/pull/14)|[ML-13] Add ALS with new oneCCL APIs|
|[#24](https://github.com/oap-project/oap-mllib/pull/24)|[ML-25] Fix oneCCL KVS port auto detect and improve logging|
|[#19](https://github.com/oap-project/oap-mllib/pull/19)|[ML-18]  Auto detect KVS port for oneCCL to avoid port conflict|


### PMem Spill

#### Bugs Fixed
|||
|:---|:---|
|[#22](https://github.com/oap-project/pmem-spill/issues/22)|[SDLe][Snyk]Upgrade Jetty version to fix vulnerability scanned by Snyk|
|[#13](https://github.com/oap-project/pmem-spill/issues/13)|The compiled code failed because the variable name was not changed|

#### PRs
|||
|:---|:---|
|[#27](https://github.com/oap-project/pmem-spill/pull/27)|[PMEM-SPILL-22][SDLe]Upgrade Jetty version|
|[#21](https://github.com/oap-project/pmem-spill/pull/21)|[POAE7-961] fix null pointer issue when offheap enabled.|
|[#18](https://github.com/oap-project/pmem-spill/pull/18)|[POAE7-858] disable RDD cache related PMem intialization as default and add PMem related logic in SparkEnv|
|[#19](https://github.com/oap-project/pmem-spill/pull/19)|[PMEM-SPILL-20][POAE7-912]add vanilla SparkEnv.scala for future update|
|[#15](https://github.com/oap-project/pmem-spill/pull/15)|[POAE7-858] port memory extension options to OAP 1.1|
|[#12](https://github.com/oap-project/pmem-spill/pull/12)|Change the variable name so that the passed parameters are correct|
|[#10](https://github.com/oap-project/pmem-spill/pull/10)|Fixing one pmem path on AppDirect mode may cause the pmem initialization path to be empty Path|


### PMem Shuffle

#### Features
|||
|:---|:---|
|[#7](https://github.com/oap-project/pmem-shuffle/issues/7)|Enable running in  fsdax mode|

#### Bugs Fixed
|||
|:---|:---|
|[#10](https://github.com/oap-project/pmem-shuffle/issues/10)|[pmem-shuffle] There are potential issues reported by Klockwork. |

#### PRs
|||
|:---|:---|
|[#13](https://github.com/oap-project/pmem-shuffle/pull/13)|[PMEM-SHUFFLE-10] Fix potential issues reported by klockwork for branch 1.1. |
|[#6](https://github.com/oap-project/pmem-shuffle/pull/6)|[PMEM-SHUFFLE-7] enable fsdax mode in pmem-shuffle|


### Remote Shuffle

#### Features
|||
|:---|:---|
|[#6](https://github.com/oap-project/remote-shuffle/issues/6)|refactor shuffle-daos by abstracting shuffle IO for supporting both synchronous and asynchronous DAOS Object API|
|[#4](https://github.com/oap-project/remote-shuffle/issues/4)|check-in remote shuffle based on DAOS Object API|

#### Bugs Fixed
|||
|:---|:---|
|[#12](https://github.com/oap-project/remote-shuffle/issues/12)|[SDLe][Snyk]Upgrade org.mock-server:mockserver-netty to fix vulnerability scanned by Snyk|

#### PRs
|||
|:---|:---|
|[#13](https://github.com/oap-project/remote-shuffle/pull/13)|[REMOTE-SHUFFLE-12][SDle][Snyk]Upgrade org.mock-server:mockserver-net…|
|[#5](https://github.com/oap-project/remote-shuffle/pull/5)|check-in remote shuffle based on DAOS Object API|


## Release 1.0.0

### Features
|||
|:---|:---|
|[#1823](https://github.com/Intel-bigdata/OAP/issues/1823)|[oap-native-sql][doc] Spark Native SQL Engine installation guide is obsolete and thus broken.|
|[#1545](https://github.com/Intel-bigdata/OAP/issues/1545)|[oap-data-source][arrow] Add metric: output_batches|
|[#1588](https://github.com/Intel-bigdata/OAP/issues/1588)|[OAP-CACHE] Make Parquet file splitable|
|[#1337](https://github.com/Intel-bigdata/OAP/issues/1337)|[oap-cacnhe] Discard OAP data format|
|[#1679](https://github.com/Intel-bigdata/OAP/issues/1679)|[OAP-CACHE]Remove the code related to reading and writing OAP data format|
|[#1680](https://github.com/Intel-bigdata/OAP/issues/1680)|[OAP-CACHE]Decouple spark code includes FileFormatDataWriter, FileFormatWriter and OutputWriter|
|[#1846](https://github.com/Intel-bigdata/OAP/issues/1846)|[oap-native-sql] spark sql unit test|
|[#1811](https://github.com/Intel-bigdata/OAP/issues/1811)|[OAP-cache]provide one-step starting scripts like plasma-sever redis-server|
|[#1519](https://github.com/Intel-bigdata/OAP/issues/1519)|[oap-native-sql] upgrade cmake|
|[#1873](https://github.com/Intel-bigdata/OAP/issues/1873)|[oap-native-sql] Columnar shuffle split variable length use UnsafeAppend|
|[#1835](https://github.com/Intel-bigdata/OAP/issues/1835)|[oap-native-sql] Support ColumnarBHJ to Build and Broadcast HashRelation in driver side|
|[#1848](https://github.com/Intel-bigdata/OAP/issues/1848)|[OAP-CACHE]Decouple spark code include OneApplicationResource.scala|
|[#1824](https://github.com/Intel-bigdata/OAP/issues/1824)|[OAP-CACHE]Decouple spark code includes DataSourceScanExec.scala.|
|[#1838](https://github.com/Intel-bigdata/OAP/issues/1838)|[OAP-CACHE]Decouple spark code includes VectorizedColumnReader.java, VectorizedPlainValuesReader.java, VectorizedRleValuesReader.java and OnHeapColumnVector.java|
|[#1839](https://github.com/Intel-bigdata/OAP/issues/1839)|[oap-native-sql] Add prefetch to columnar shuffle split|
|[#1756](https://github.com/Intel-bigdata/OAP/issues/1756)|[Intel MLlib] Add Kmeans "tolerance" support and test cases|
|[#1818](https://github.com/Intel-bigdata/OAP/issues/1818)|[OAP-Cache]Make Spark webUI OAP Tab more user friendly|
|[#1831](https://github.com/Intel-bigdata/OAP/issues/1831)|[oap-native-sql] ColumnarWindow: Support reusing same window spec in multiple functions|
|[#1653](https://github.com/Intel-bigdata/OAP/issues/1653)|[SQL Data Source Cache]Consistency issue on "enable" and "enabled" configuration |
|[#1765](https://github.com/Intel-bigdata/OAP/issues/1765)|[oap-native-sql] Support WSCG in nativesql|
|[#1517](https://github.com/Intel-bigdata/OAP/issues/1517)|[oap-native-sql] implement SortMergeJoin|
|[#1535](https://github.com/Intel-bigdata/OAP/issues/1535)|[oap-native-sql] Add ColumnarWindowExec|
|[#1654](https://github.com/Intel-bigdata/OAP/issues/1654)|[oap-native-sql] Columnar shuffle TPCDS enabling|
|[#1700](https://github.com/Intel-bigdata/OAP/issues/1700)|[oap-native-sql] Support inside join condition project|
|[#1717](https://github.com/Intel-bigdata/OAP/issues/1717)|[oap-native-sql] support null in columnar literal and subquery|
|[#1704](https://github.com/Intel-bigdata/OAP/issues/1704)|[oap-native-sql] Add ColumnarUnion and ColumnarExpand|
|[#1647](https://github.com/Intel-bigdata/OAP/issues/1647)|[oap-native-sql] row to columnar for decimal|
|[#1638](https://github.com/Intel-bigdata/OAP/issues/1638)|[oap-native-sql] adding full TPC-DS support|
|[#1498](https://github.com/Intel-bigdata/OAP/issues/1498)|[oap-native-sql] stddev_samp support|
|[#1547](https://github.com/Intel-bigdata/OAP/issues/1547)|[oap-native-sql] adding metrics for input/output batches|

### Performance
|||
|:---|:---|
|[#1956](https://github.com/Intel-bigdata/OAP/issues/1956)|[OAP-MLlib]Cannot get 5x performance benefit comparing with vanilla spark.|
|[#1955](https://github.com/Intel-bigdata/OAP/issues/1955)|[OAP-CACHE] Plasma shows lower performance comparing with vanilla spark.|
|[#2023](https://github.com/Intel-bigdata/OAP/issues/2023)|[OAP-MLlib] Use oneAPI official release instead of beta versions|
|[#1829](https://github.com/Intel-bigdata/OAP/issues/1829)|[oap-native-sql] Optimize columnar shuffle and option to use AVX512|
|[#1734](https://github.com/Intel-bigdata/OAP/issues/1734)|[oap-native-sql] use non-codegen for sort with one key|
|[#1706](https://github.com/Intel-bigdata/OAP/issues/1706)|[oap-native-sql] Optimize columnar shuffle write|

### Bugs Fixed
|||
|:---|:---|
|[#2054](https://github.com/Intel-bigdata/OAP/issues/2054)|[OAP-MLlib] Faild run Intel mllib after updating the version of oneapi.|
|[#2012](https://github.com/Intel-bigdata/OAP/issues/2012)|[SQL Data Source Cache] The task will be suspended when using plasma cache.|
|[#1640](https://github.com/Intel-bigdata/OAP/issues/1640)|[SQL Data Source Cache] The task will be suspended when using plasma cache and starting 2 executors per worker.|
|[#2028](https://github.com/Intel-bigdata/OAP/issues/2028)|[OAP-Cache]When using Plasma Spark webUI OAP Tab cache metrics are not right  |
|[#1979](https://github.com/Intel-bigdata/OAP/issues/1979)|[SDLe][native-sql-engine] Issues from Static Code Analysis with Klocwork need to be fixed |
|[#1938](https://github.com/Intel-bigdata/OAP/issues/1938)|[oap-native-sql] Stability test failed when running TPCH for 10 rounds.|
|[#1924](https://github.com/Intel-bigdata/OAP/issues/1924)|[OAP-CACHE] Decouple hearbeat message and use conf to determine whether to report locailty information|
|[#1937](https://github.com/Intel-bigdata/OAP/issues/1937)|[rpmem-shuffle] Cannot pass q64.sql of TPC-DS when enable RPmem shuffle. |
|[#1951](https://github.com/Intel-bigdata/OAP/issues/1951)|[SDLe][PMem-Shuffle]Specify Scala version above 2.12.4 in pom.xml|
|[#1921](https://github.com/Intel-bigdata/OAP/issues/1921)|[SDLe][rpmem-shuffle] The master branch and branch-1.0-spark-3.0 can't pass BDBA analysis with libsqlitejdbc dependency.|
|[#1743](https://github.com/Intel-bigdata/OAP/issues/1743)|[oap-native-sql] Error not reported when creating CodeGenerator instance|
|[#1864](https://github.com/Intel-bigdata/OAP/issues/1864)|[oap-native-sql] hash conflict in hashagg|
|[#1934](https://github.com/Intel-bigdata/OAP/issues/1934)|[oap-native-sql] backport to 1.0|
|[#1929](https://github.com/Intel-bigdata/OAP/issues/1929)|[oap-native-sql] memleak in non-codegen aggregate|
|[#1907](https://github.com/Intel-bigdata/OAP/issues/1907)|[OAP-cache]Cannot find the class of redis-client|
|[#1888](https://github.com/Intel-bigdata/OAP/issues/1888)|[oap-native-sql] Add hash collision check for all HashJoins and hashAggr|
|[#1903](https://github.com/Intel-bigdata/OAP/issues/1903)|[oap-native-sql] BHJ related UT fix|
|[#1881](https://github.com/Intel-bigdata/OAP/issues/1881)|[oap-native-sql] Fix split use avx512|
|[#1742](https://github.com/Intel-bigdata/OAP/issues/1742)|[oap-native-sql] SortArraysToIndicesKernel: incorrect null ordering with multiple sort keys|
|[#1553](https://github.com/Intel-bigdata/OAP/issues/1553)|[oap-native-sql] TPCH-Q7 fails in throughput tests|
|[#1854](https://github.com/Intel-bigdata/OAP/issues/1854)|[oap-native-sql] Fix columnar shuffle file not deleted|
|[#1844](https://github.com/Intel-bigdata/OAP/issues/1844)|[oap-native-sql] Fix columnar shuffle spilled file not deleted|
|[#1580](https://github.com/Intel-bigdata/OAP/issues/1580)|[oap-native-sql] Hash Collision in multiple keys scenario|
|[#1754](https://github.com/Intel-bigdata/OAP/issues/1754)|[Intel MLlib] Improve LibLoader creating temp dir name with UUID|
|[#1815](https://github.com/Intel-bigdata/OAP/issues/1815)|[oap-native-sql] Memory management: Error on task end if there are unclosed child allocators|
|[#1808](https://github.com/Intel-bigdata/OAP/issues/1808)|[oap-native-sql] ColumnarWindow: Memory leak on converting input/output batches|
|[#1806](https://github.com/Intel-bigdata/OAP/issues/1806)|[oap-native-sql] Fix Columnar Shuffle Memory Leak|
|[#1783](https://github.com/Intel-bigdata/OAP/issues/1783)|[oap-native-sql] ColumnarWindow: Rank() returns wrong result when input row number >= 65536|
|[#1776](https://github.com/Intel-bigdata/OAP/issues/1776)|[oap-native-sql] memory leakage in native code|
|[#1760](https://github.com/Intel-bigdata/OAP/issues/1760)|[oap-native-sql] fix columnar sorting on string|
|[#1733](https://github.com/Intel-bigdata/OAP/issues/1733)|[oap-native-sql]TPCH Q18 memory leakage |
|[#1694](https://github.com/Intel-bigdata/OAP/issues/1694)|[oap-native-sql] TPC-H q15 failed for ConditionedProbeArraysVisitorImpl MakeResultIterator does not support dependency type other than Batch|
|[#1682](https://github.com/Intel-bigdata/OAP/issues/1682)|[oap-native-sql] fix aggregate without codegen|
|[#1707](https://github.com/Intel-bigdata/OAP/issues/1707)|[oap-native-sql] Fix collect batch metric|
|[#1642](https://github.com/Intel-bigdata/OAP/issues/1642)|[oap-native-sql] Support expression key in Join|
|[#1669](https://github.com/Intel-bigdata/OAP/issues/1669)|[oap-native-sql] TPCH Q1 results is not correct w/ hashagg codegen off|
|[#1629](https://github.com/Intel-bigdata/OAP/issues/1629)|[oap-native-sql] clean up building steps|
|[#1602](https://github.com/Intel-bigdata/OAP/issues/1602)|[oap-native-sql] rework copyfromjar function|
|[#1599](https://github.com/Intel-bigdata/OAP/issues/1599)|[oap-native-sql] Columnar BHJ fail on TPCH-Q15|
|[#1567](https://github.com/Intel-bigdata/OAP/issues/1567)|[oap-native-sql] Spark thrift-server does not honor LIBARROW_DIR env|
|[#1541](https://github.com/Intel-bigdata/OAP/issues/1541)|[oap-native-sql] TreeNode children not replaced by columnar operators|

### PRs
|||
|:---|:---|
|[#2056](https://github.com/Intel-bigdata/OAP/pull/2056)|[OAP-2054][OAP-MLlib] Fix oneDAL libJavaAPI.so packaging for oneAPI 2021.1 production release|
|[#2039](https://github.com/Intel-bigdata/OAP/pull/2039)|[OAP-2023][OAP-MLlib] Switch to oneAPI 2021.1.1 official release for OAP 1.0|
|[#2043](https://github.com/Intel-bigdata/OAP/pull/2043)|[OAP-1981][OAP-CACHE][POAE7-617]fix binary cache core dump issue|
|[#2002](https://github.com/Intel-bigdata/OAP/pull/2002)|[OAP-2001][oap-native-sql]fix coding style|
|[#2035](https://github.com/Intel-bigdata/OAP/pull/2035)|[OAP-2028][OAP-cache][POAE7-635] Fix set concurrent access bug|
|[#2037](https://github.com/Intel-bigdata/OAP/pull/2037)|[OAP-1640][OAP-CACHE][POAE7-593]Fix plasma hang due to threshold|
|[#2036](https://github.com/Intel-bigdata/OAP/pull/2036)|[OAP-1955][OAP-CACHE][POAE7-660]preferLocation low hit rate fix master branch|
|[#2013](https://github.com/Intel-bigdata/OAP/pull/2013)|[OAP-CACHE][POAE7-628]port missing commits from branch 0.8/0.9 |
|[#2015](https://github.com/Intel-bigdata/OAP/pull/2015)|[OAP-2016] fix klocwork issues in oap-common/oap-spark|
|[#2022](https://github.com/Intel-bigdata/OAP/pull/2022)|[OAP-1980][rpmem-shuffle] Fix Klockwork issues for spark3.x version|
|[#2011](https://github.com/Intel-bigdata/OAP/pull/2011)|[OAP-2010][oap-native-sql] Add abs support in wscg|
|[#1996](https://github.com/Intel-bigdata/OAP/pull/1996)|[OAP-1998][oap-native-sql] Add support to do numa binding for Columnar Operations|
|[#2004](https://github.com/Intel-bigdata/OAP/pull/2004)|[OAP-2012][OAP-CACHE][POAE7-635]bug fix: plasma hang - use java thread-safe set|
|[#1988](https://github.com/Intel-bigdata/OAP/pull/1988)|[OAP-1983][oap-native-sql] Fix Q38 and Q87 when unsafeRow contains null|
|[#1976](https://github.com/Intel-bigdata/OAP/pull/1976)|[OAP-1983][oap-native-sql] Fix hashCheck performance issue|
|[#1970](https://github.com/Intel-bigdata/OAP/pull/1970)|[OAP-1947][oap-native-sql][C++] reduce sort kernel memory footprint|
|[#1961](https://github.com/Intel-bigdata/OAP/pull/1961)|[OAP-1924][OAP-CACHE]Decouple hearbeat message and use conf to determine whether to report locailty information for branch branch-1.0-spark-3.x|
|[#1982](https://github.com/Intel-bigdata/OAP/pull/1982)|[OAP-1981][OAP-CACHE][POAE7-617]Bug fix binary docache|
|[#1952](https://github.com/Intel-bigdata/OAP/pull/1952)|[OAP-1951][PMem-Shuffle][SDLe]Specify Scala version in pom.xml|
|[#1919](https://github.com/Intel-bigdata/OAP/pull/1919)|[OAP-1918][OAP-CACHE][POAE7-563]bug fix: plasma get an invalid value|
|[#1589](https://github.com/Intel-bigdata/OAP/pull/1589)|[OAP-1588][OAP-CACHE][POAE7-363] Make Parquet splitable|
|[#1954](https://github.com/Intel-bigdata/OAP/pull/1954)|[OAP-1884][OAP-dev]Small fix for arrow build in prepare_oap_env.sh.|
|[#1933](https://github.com/Intel-bigdata/OAP/pull/1933)|[OAP-1934][oap-native-sql]Backport NativeSQL code to 1.0|
|[#1889](https://github.com/Intel-bigdata/OAP/pull/1889)|[OAP-1888][oap-native-sql]Add hash collision check for all HashJoins and hashAggr|
|[#1904](https://github.com/Intel-bigdata/OAP/pull/1904)|[OAP-1903][oap-native-sql] Fix Local Mode BHJ related UT fail issue|
|[#1916](https://github.com/Intel-bigdata/OAP/pull/1916)|[OAP-1846][oap-native-sql] clean up travis test|
|[#1923](https://github.com/Intel-bigdata/OAP/pull/1923)|[OAP-1921][rpmem-shuffle] For BDBA analysis to exclude unused library|
|[#1890](https://github.com/Intel-bigdata/OAP/pull/1890)|[OAP-1846][oap-native-sql] add script for running unit test|
|[#1905](https://github.com/Intel-bigdata/OAP/pull/1905)|[OAP-1813][POAE7-555] [OAP-CACHE] package redis related dependency|
|[#1908](https://github.com/Intel-bigdata/OAP/pull/1908)|[OAP-1884][OAP-dev]Add cxx-compiler in oap conda recipes for native-sql.|
|[#1901](https://github.com/Intel-bigdata/OAP/pull/1901)|[OAP-1884][OAP-dev]Add c-compiler in oap conda recipes for native-sql.|
|[#1895](https://github.com/Intel-bigdata/OAP/pull/1895)|[OAP-1884][OAP-dev] Checkout arrow branch in case arrow in other branch|
|[#1876](https://github.com/Intel-bigdata/OAP/pull/1876)|[OAP-1875]Generating changelog automatically for new releases|
|[#1812](https://github.com/Intel-bigdata/OAP/pull/1812)|[OAP-1811][OAP-cache][POAE7-486]add sbin folder|
|[#1882](https://github.com/Intel-bigdata/OAP/pull/1882)|[OAP-1881][oap-native-sql] Fix split use avx512|
|[#1847](https://github.com/Intel-bigdata/OAP/pull/1847)|[OAP-1846][oap-native-sql] add unit tests from spark to native sql|
|[#1836](https://github.com/Intel-bigdata/OAP/pull/1836)|[OAP-1835][oap-native-sql] Support ColumnarBHJ to build and broadcast hashrelation|
|[#1885](https://github.com/Intel-bigdata/OAP/pull/1885)|[OAP-1884][OAP-dev]Add oap-mllib to parent pom and fix error when git clone oneccl.|
|[#1868](https://github.com/Intel-bigdata/OAP/pull/1868)|[OAP-1653][OAP-Cache]Modify enabled and enable compatibility check|
|[#1853](https://github.com/Intel-bigdata/OAP/pull/1853)|[OAP-1852][oap-native-sql] Memory Management: Use Arrow C++ memory po…|
|[#1859](https://github.com/Intel-bigdata/OAP/pull/1859)|[OAP-1858][OAP-cache][POAE7-518] Decouple FilePartition.scala|
|[#1857](https://github.com/Intel-bigdata/OAP/pull/1857)|[OAP-1833][oap-native-sql] Fix HashAggr hasNext won't stop issue|
|[#1855](https://github.com/Intel-bigdata/OAP/pull/1855)|[OAP-1854][oap-native-sql] Fix columnar shuffle file not deleted|
|[#1840](https://github.com/Intel-bigdata/OAP/pull/1840)|[OAP-1839][oap-native-sql] Add prefetch to columnar shuffle split|
|[#1843](https://github.com/Intel-bigdata/OAP/pull/1843)|[OAP-1842][OAP-dev]Add arrow conda build action job.|
|[#1849](https://github.com/Intel-bigdata/OAP/pull/1849)|[OAP-1848][SQL Data Source Cache] Decouple OneApplicationResource.scala|
|[#1837](https://github.com/Intel-bigdata/OAP/pull/1837)|[OAP-1838][SQL Data Source Cache] Decouple VectorizedColumnReader.java, VectorizedPlainValuesReader.java, VectorizedRleValuesReader.java and OnHeapColumnVector.java.|
|[#1757](https://github.com/Intel-bigdata/OAP/pull/1757)|[OAP-1756][Intel MLlib] Add Kmeans "tolerance" support and test cases|
|[#1845](https://github.com/Intel-bigdata/OAP/pull/1845)|[OAP-1844][oap-native-sql] Fix columnar shuffle spilled file not deleted|
|[#1827](https://github.com/Intel-bigdata/OAP/pull/1827)|[OAP-1818][SQL-Data-Source-Cache]Modify Spark webUI OAP Tab expressio…|
|[#1832](https://github.com/Intel-bigdata/OAP/pull/1832)|[OAP-1831][oap-native-sql] ColumnarWindow: Support reusing same windo…|
|[#1834](https://github.com/Intel-bigdata/OAP/pull/1834)|[OAP-1833][oap-native-sql][Scala] fix CoalesceBatchs after HashAgg|
|[#1830](https://github.com/Intel-bigdata/OAP/pull/1830)|[OAP-1829][oap-native-sql] Optimize columnar shuffle and option to use AVX-512|
|[#1803](https://github.com/Intel-bigdata/OAP/pull/1803)|[OAP-1751][oap-native-sql]fix sort on TPC-DS|
|[#1755](https://github.com/Intel-bigdata/OAP/pull/1755)|[OAP-1754][Intel MLlib] Improve LibLoader creating temp dir name with UUID|
|[#1826](https://github.com/Intel-bigdata/OAP/pull/1826)|[OAP-1825] disable pmemblk test|
|[#1802](https://github.com/Intel-bigdata/OAP/pull/1802)|[OAP-1653][OAP-Cache]Keep consistency on 'enabled' of OapConf configu…|
|[#1810](https://github.com/Intel-bigdata/OAP/pull/1810)|[OAP-1771]Fix README for Arrow Data Source|
|[#1816](https://github.com/Intel-bigdata/OAP/pull/1816)|[OAP-1815][oap-native-sql] Memory management: Error on task end if th…|
|[#1809](https://github.com/Intel-bigdata/OAP/pull/1809)|[OAP-1808][oap-native-sql] ColumnarWindow: Memory leak on converting input/output batches|
|[#1467](https://github.com/Intel-bigdata/OAP/pull/1467)|[OAP-1457][oap-native-sql] Reserve Spark off-heap execution memory after buffer allocation|
|[#1807](https://github.com/Intel-bigdata/OAP/pull/1807)|[OAP-1806][oap-native-sql] Fix Columnar Shuffle Memory Leak|
|[#1788](https://github.com/Intel-bigdata/OAP/pull/1788)|[OAP-1765][oap-native-sql] Fix for dropped CoalecseBatches before ColumnarBroadcastExchange|
|[#1799](https://github.com/Intel-bigdata/OAP/pull/1799)|[OAP-CACHE][OAP-1690][POAE7-430] Cache backend fall back detect bug fix branch master|
|[#1744](https://github.com/Intel-bigdata/OAP/pull/1744)|[OAP-CACHE][OAP-1748][POAE7-462] Enable externalDB to store CacheMetaInfo branch master|
|[#1787](https://github.com/Intel-bigdata/OAP/pull/1787)|[OAP-1786][oap-native-sql] ColumnarWindow: Avoid unnecessary mem copies|
|[#1773](https://github.com/Intel-bigdata/OAP/pull/1773)|[POAE7-471]Handle oap-common build issue about PMemKV|
|[#1782](https://github.com/Intel-bigdata/OAP/pull/1782)|[OAP-1631]Update compile scripts from 0.9|
|[#1785](https://github.com/Intel-bigdata/OAP/pull/1785)|[OAP-1765][oap-native-sql] Support WSCG for nativesql(PART 2)|
|[#1781](https://github.com/Intel-bigdata/OAP/pull/1781)|[OAP-1765][oap-native-sql] fix codegen for SMJ and HashAgg|
|[#1775](https://github.com/Intel-bigdata/OAP/pull/1775)|[OAP-1776][oap-native-sql]fix sort memleak|
|[#1766](https://github.com/Intel-bigdata/OAP/pull/1766)|[OAP-1765][oap-native-sql] Support WSCG for nativesql and use non-codegen join for remainings|
|[#1774](https://github.com/Intel-bigdata/OAP/pull/1774)|[OAP-1631]Add prepare_oap_env.sh.|
|[#1769](https://github.com/Intel-bigdata/OAP/pull/1769)|[OAP-1768][POAE7-163][OAP-SPARK] Integrate block manager with chunk api|
|[#1763](https://github.com/Intel-bigdata/OAP/pull/1763)|[OAP-1759][oap-native-sql] ColumnarWindow: Add execution metrics|
|[#1656](https://github.com/Intel-bigdata/OAP/pull/1656)|[OAP-1517][oap-native-sql] Improve SortMergeJoin Part2|
|[#1761](https://github.com/Intel-bigdata/OAP/pull/1761)|[oap-native-sql] quick fix sort on string by fallback to row|
|[#1536](https://github.com/Intel-bigdata/OAP/pull/1536)|[OAP-1535][oap-native-sql] Add ColumnarWindowExec|
|[#1735](https://github.com/Intel-bigdata/OAP/pull/1735)|[OAP-1734][oap-native-sql]use non-codegen for sort with single key|
|[#1747](https://github.com/Intel-bigdata/OAP/pull/1747)|[OAP-1741][rpmem-shuffle]To make java side load native library from jar directly|
|[#1725](https://github.com/Intel-bigdata/OAP/pull/1725)|[OAP-1727][POAE7-358] Spark integration: Memory Spill to PMem|
|[#1738](https://github.com/Intel-bigdata/OAP/pull/1738)|[OAP-1733][oap-native-sql][Scala] fix mem leak|
|[#1701](https://github.com/Intel-bigdata/OAP/pull/1701)|[OAP-1700][oap-native-sql] support join-inside condition project|
|[#1736](https://github.com/Intel-bigdata/OAP/pull/1736)|[oap-1727][POAE7-358] Add native spark files for memory spill module|
|[#1719](https://github.com/Intel-bigdata/OAP/pull/1719)|[oap-common][POAE7-347]Stream API for PMem storage store|
|[#1723](https://github.com/Intel-bigdata/OAP/pull/1723)|[OAP-1679][OAP-CACHE] Remove the code related to reading and writing OAP data format |
|[#1716](https://github.com/Intel-bigdata/OAP/pull/1716)|[OAP-1717][oap-native-sql] support null in columnar literal and subquery|
|[#1713](https://github.com/Intel-bigdata/OAP/pull/1713)|[OAP-1712] [OAP-SPARK] Remove file change list from dev directory|
|[#1711](https://github.com/Intel-bigdata/OAP/pull/1711)|[OAP-1694][oap-native-sql][Scala] fix hash join w/ empty batch|
|[#1710](https://github.com/Intel-bigdata/OAP/pull/1710)|[OAP-1706][oap-native-sql] Optimize shuffle write|
|[#1705](https://github.com/Intel-bigdata/OAP/pull/1705)|[OAP-1704][oap-native-sql] Support ColumnarUnion and ColumnarExpand|
|[#1683](https://github.com/Intel-bigdata/OAP/pull/1683)|[OAP-1682][oap-native-sql] fix aggregate without codegen|
|[#1708](https://github.com/Intel-bigdata/OAP/pull/1708)|[OAP-1707][oap-native-sql] Fix collect batch metric|
|[#1675](https://github.com/Intel-bigdata/OAP/pull/1675)|[OAP-1651][oap-native-sql] Adding fallback rules for join/shuffle|
|[#1674](https://github.com/Intel-bigdata/OAP/pull/1674)|[OAP-1673][oap-native-sql] Adding native double round function|
|[#1632](https://github.com/Intel-bigdata/OAP/pull/1632)|[OAP-1631][Doc] Add Commit Message Requirements|
|[#1672](https://github.com/Intel-bigdata/OAP/pull/1672)|[OAP-1610][Intel-MLlib]Upgrade the mahout-hdfs to version 14.1|
|[#1641](https://github.com/Intel-bigdata/OAP/pull/1641)|[OAP-1651][OAP-1642][oap-native-sql] support TPCDS w/ AQE|
|[#1670](https://github.com/Intel-bigdata/OAP/pull/1670)|[OAP-1669][oap-native-sql] use distinct ordinal list|
|[#1655](https://github.com/Intel-bigdata/OAP/pull/1655)|[OAP-1654][oap-native-sql]Columnar shuffle tpcds enabling|
|[#1630](https://github.com/Intel-bigdata/OAP/pull/1630)|[OAP-1629][oap-native-sql] clean up building scripts|
|[#1601](https://github.com/Intel-bigdata/OAP/pull/1601)|[OAP-1602][oap-native-sql][Java] fix exract resource from jar|
|[#1639](https://github.com/Intel-bigdata/OAP/pull/1639)|[OAP-1638][oap-native-sql] tpcds enabling (part2)|
|[#1586](https://github.com/Intel-bigdata/OAP/pull/1586)|[OAP-1587][oap-native-sql] tpcds enabling (part1)|
|[#1600](https://github.com/Intel-bigdata/OAP/pull/1600)|[oap-1599][oap-native-sql][Scala] fix broadcasthashjoin|
|[#1555](https://github.com/Intel-bigdata/OAP/pull/1555)|[OAP-1541][oap-native-sql] TreeNode children not replaced by columnar…|
|[#1546](https://github.com/Intel-bigdata/OAP/pull/1546)|[OAP-1547][oap-native-sql][Scala] Adding metrics for input/output batches|
|[#1472](https://github.com/Intel-bigdata/OAP/pull/1472)|[OAP-1466] [RDD Cache] [POAE-354] Initialize pmem with AppDirect and KMemDax mode in block manager |

## Release 0.8.4

### Features
|||
|:---|:---|
|[#1865](https://github.com/Intel-bigdata/OAP/issues/1865)|[OAP-CACHE]Decouple spark code include DataSourceScanExec.scala, OneApplicationResource.scala, Decouple VectorizedColumnReader.java, VectorizedPlainValuesReader.java, VectorizedRleValuesReader.java and OnHeapColumnVector.java for OAP-0.8.4.|
|[#1813](https://github.com/Intel-bigdata/OAP/issues/1813)|[OAP-cache] package redis client jar into oap-cache|

### Bugs Fixed
|||
|:---|:---|
|[#2044](https://github.com/Intel-bigdata/OAP/issues/2044)|[OAP-CACHE] Build error due to synchronizedSet on branch 0.8|
|[#2027](https://github.com/Intel-bigdata/OAP/issues/2027)|[oap-shuffle] Should load native library from jar directly|
|[#1981](https://github.com/Intel-bigdata/OAP/issues/1981)|[OAP-CACHE] Error runing q32 binary cache|
|[#1980](https://github.com/Intel-bigdata/OAP/issues/1980)|[SDLe][RPMem-Shuffle]Issues from Static Code Analysis with Klocwork need to be fixed|
|[#1918](https://github.com/Intel-bigdata/OAP/issues/1918)|[OAP-CACHE] Plasma throw exception:get an invalid value- branch 0.8|

### PRs
|||
|:---|:---|
|[#2045](https://github.com/Intel-bigdata/OAP/pull/2045)|[OAP-2044][OAP-CACHE]bug fix: build error due to synchronizedSet|
|[#2031](https://github.com/Intel-bigdata/OAP/pull/2031)|[OAP-1955][OAP-CACHE][POAE7-667]preferLocation low hit rate fix branch 0.8|
|[#2029](https://github.com/Intel-bigdata/OAP/pull/2029)|[OAP-2027][rpmem-shuffle] Load native libraries from jar|
|[#2018](https://github.com/Intel-bigdata/OAP/pull/2018)|[OAP-1980][SDLe][rpmem-shuffle] Fix potential risk issues reported by Klockwork|
|[#1920](https://github.com/Intel-bigdata/OAP/pull/1920)|[OAP-1924][OAP-CACHE]Decouple hearbeat message and use conf to determine whether to report locailty information|
|[#1949](https://github.com/Intel-bigdata/OAP/pull/1949)|[OAP-1948][rpmem-shuffle] Fix several vulnerabilities reported by BDBA|
|[#1900](https://github.com/Intel-bigdata/OAP/pull/1900)|[OAP-1680][OAP-CACHE] Decouple FileFormatDataWriter, FileFormatWriter and OutputWriter|
|[#1899](https://github.com/Intel-bigdata/OAP/pull/1899)|[OAP-1679][OAP-CACHE] Remove the code related to reading and writing OAP data format  (#1723)|
|[#1897](https://github.com/Intel-bigdata/OAP/pull/1897)|[OAP-1884][OAP-dev] Update memkind version and copy arrow plasma jar to conda package build path|
|[#1883](https://github.com/Intel-bigdata/OAP/pull/1883)|[OAP-1568][OAP-CACHE] Cleanup Oap data format read/write related test cases|
|[#1863](https://github.com/Intel-bigdata/OAP/pull/1863)|[OAP-1865][SQL Data Source Cache]Decouple spark code include DataSourceScanExec.scala, OneApplicationResource.scala, Decouple VectorizedColumnReader.java, VectorizedPlainValuesReader.java, VectorizedRleValuesReader.java and OnHeapColumnVector.java for OAP-0.8.4.|
|[#1841](https://github.com/Intel-bigdata/OAP/pull/1841)|[OAP-1579][OAP-cache]Fix web UI to show cache size|
|[#1814](https://github.com/Intel-bigdata/OAP/pull/1814)|[OAP-cache][OAP-1813][POAE7-481]package redis client related dependency|
|[#1790](https://github.com/Intel-bigdata/OAP/pull/1790)|[OAP-CACHE][OAP-1690][POAE7-430] Cache backend fallback bugfix|
|[#1740](https://github.com/Intel-bigdata/OAP/pull/1740)|[OAP-CACHE][OAP-1748][POAE7-453]Enable externalDB to store CacheMetaInfo branch 0.8|
|[#1731](https://github.com/Intel-bigdata/OAP/pull/1731)|[OAP-CACHE] [OAP-1730] [POAE-428] Add OAP cache runtime enable|
