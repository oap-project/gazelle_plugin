# Change log
Generated on 2021-06-02

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


### PMEM Spill

#### Features
|||
|:---|:---|
|[#34](https://github.com/oap-project/pmem-spill/issues/34)|Support vanilla  spark 3.1.1|

#### PRs
|||
|:---|:---|
|[#41](https://github.com/oap-project/pmem-spill/pull/41)|[PMEM-SPILL-34][POAE7-1119]Port RDD cache to Spark 3.1.1 as separate module|


### PMEM Common

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


### PMEM Shuffle

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


### PMEM Spill

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


### PMEM Shuffle

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
