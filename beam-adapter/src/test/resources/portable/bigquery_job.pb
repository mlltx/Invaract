
 «
°
Eref_AppliedPTransform_Load-BigQueryBatchFileLoads-GroupShardedRows_86◊
 
beam:transform:group_by_key:v1&
Noneref_PCollection_PCollection_55"&
Noneref_PCollection_PCollection_56*,Load/BigQueryBatchFileLoads/GroupShardedRowsB5
python_type&apache_beam.transforms.core.GroupByKey
≥
!ref_AppliedPTransform_ReadTable_2ç
%
#beam:transform:generic_composite:v16ref_AppliedPTransform_ReadTable-FilesToRemoveImpulse_32ref_AppliedPTransform_ReadTable-MapFilesToRemove_8&ref_AppliedPTransform_ReadTable-Read_9:ref_AppliedPTransform_ReadTable-_PassThroughThenCleanup_14"%
Noneref_PCollection_PCollection_8*	ReadTable:%ref_Environment_default_environment_1B;
python_type,apache_beam.io.gcp.bigquery.ReadFromBigQuery
å
gref_AppliedPTransform_Load-BigQueryBatchFileLoads-ImpulseSingleElementPC-FlatMap-lambda-at-core-py-4_74†

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_43"&
Noneref_PCollection_PCollection_44*TLoad/BigQueryBatchFileLoads/ImpulseSingleElementPC/FlatMap(<lambda at core.py:4094>)2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2t
beam:display_data:labelled:v1S
Transform Function<lambda>2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
¸
aref_AppliedPTransform_ReadQuery-_PassThroughThenCleanup-Create-FlatMap-lambda-at-core-py-4094-_61ñ

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_36"&
Noneref_PCollection_PCollection_37*JReadQuery/_PassThroughThenCleanup/Create/FlatMap(<lambda at core.py:4094>)2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2t
beam:display_data:labelled:v1S
Transform Function<lambda>2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
è
Zref_AppliedPTransform_Load-BigQueryBatchFileLoads-ImpulseSingleElementPC-MaybeReshuffle_75∞
"
 beam:transform:pickled_python:v1&
Noneref_PCollection_PCollection_44"&
Noneref_PCollection_PCollection_44*ALoad/BigQueryBatchFileLoads/ImpulseSingleElementPC/MaybeReshuffle:%ref_Environment_default_environment_1BP
python_typeAapache_beam.transforms.core.Create.expand.<locals>.MaybeReshuffle
˚
Pref_AppliedPTransform_ReadQuery-_PassThroughThenCleanup-Create-MaybeReshuffle_62¶
"
 beam:transform:pickled_python:v1&
Noneref_PCollection_PCollection_37"&
Noneref_PCollection_PCollection_37*7ReadQuery/_PassThroughThenCleanup/Create/MaybeReshuffle:%ref_Environment_default_environment_1BP
python_typeAapache_beam.transforms.core.Create.expand.<locals>.MaybeReshuffle
‚
Zref_AppliedPTransform_Stream-_StreamToBigQuery-StreamInsertRows-ParDo-BigQueryWriteFn-_137É

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_99"7
FailedRowsWithErrorsref_PCollection_PCollection_101"-

FailedRowsref_PCollection_PCollection_100"'
Noneref_PCollection_PCollection_102*@Stream/_StreamToBigQuery/StreamInsertRows/ParDo(BigQueryWriteFn)2â
beam:display_data:labelled:v1h
Transform Function+apache_beam.io.gcp.bigquery.BigQueryWriteFn2fn:!apache_beam.transforms.core.ParDo2q
beam:display_data:labelled:v1P
max_batch_size(Ù2max_batch_size:+apache_beam.io.gcp.bigquery.BigQueryWriteFn2w
beam:display_data:labelled:v1V
max_buffered_rows(–2max_buffered_rows:+apache_beam.io.gcp.bigquery.BigQueryWriteFn2|
beam:display_data:labelled:v1[
retry_strategyRETRY_ALWAYS2retry_strategy:+apache_beam.io.gcp.bigquery.BigQueryWriteFn2à
beam:display_data:labelled:v1g
create_dispositionCREATE_IF_NEEDED2create_disposition:+apache_beam.io.gcp.bigquery.BigQueryWriteFn2Ç
beam:display_data:labelled:v1a
write_dispositionWRITE_APPEND2write_disposition:+apache_beam.io.gcp.bigquery.BigQueryWriteFn2Ü
beam:display_data:labelled:v1e
additional_bq_parameters{}2additional_bq_parameters:+apache_beam.io.gcp.bigquery.BigQueryWriteFn2{
beam:display_data:labelled:v1Z
ignore_insert_idsFalse2ignore_insert_ids:+apache_beam.io.gcp.bigquery.BigQueryWriteFn2Ö
beam:display_data:labelled:v1d
ignore_unknown_columnsFalse2ignore_unknown_columns:+apache_beam.io.gcp.bigquery.BigQueryWriteFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
¬
href_AppliedPTransform_Load-BigQueryBatchFileLoads-ReshuffleBeforeLoadWithoutTempTables-ReshufflePerK_104’

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_70"&
Noneref_PCollection_PCollection_71*sLoad/BigQueryBatchFileLoads/ReshuffleBeforeLoadWithoutTempTables/ReshufflePerKey/Map(reify_metadata_default_window)2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2â
beam:display_data:labelled:v1h
Transform Functionreify_metadata_default_window2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
¶
2ref_AppliedPTransform_ReadTable-MapFilesToRemove_8Ô

beam:transform:pardo:v1%
Noneref_PCollection_PCollection_3"%
Noneref_PCollection_PCollection_4*ReadTable/MapFilesToRemove2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2
beam:display_data:labelled:v1^
Transform Functionfile_path_to_remove2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
µ
Sref_AppliedPTransform_Load-BigQueryBatchFileLoads-TriggerLoadJobsWithTempTables_108›
%
#beam:transform:generic_composite:v1href_AppliedPTransform_Load-BigQueryBatchFileLoads-TriggerLoadJobsWithTempTables-ParDo-TriggerLoadJob_109&
Noneref_PCollection_PCollection_69"1
TemporaryTablesref_PCollection_PCollection_77"-
OngoingJobsref_PCollection_PCollection_76"&
Noneref_PCollection_PCollection_75*9Load/BigQueryBatchFileLoads/TriggerLoadJobsWithTempTables:%ref_Environment_default_environment_1B6
python_type'apache_beam.transforms.core._MultiParDo
ö
2ref_AppliedPTransform_ReadTable-Read-EmitSource_11„

beam:transform:pardo:v1%
Noneref_PCollection_PCollection_5"%
Noneref_PCollection_PCollection_6*ReadTable/Read/EmitSource2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2t
beam:display_data:labelled:v1S
Transform Function<lambda>2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
Ö
Nref_AppliedPTransform_Load-BigQueryBatchFileLoads-ParDo-WriteRecordsToFile-_83≤
%
#beam:transform:generic_composite:v1gref_AppliedPTransform_Load-BigQueryBatchFileLoads-ParDo-WriteRecordsToFile-ParDo-WriteRecordsToFile-_84&
Noneref_PCollection_PCollection_51"2
UnwrittenRecordsref_PCollection_PCollection_53".
WrittenFilesref_PCollection_PCollection_54*5Load/BigQueryBatchFileLoads/ParDo(WriteRecordsToFile):%ref_Environment_default_environment_1B6
python_type'apache_beam.transforms.core._MultiParDo
≥
=ref_AppliedPTransform_Dynamic-_StreamToBigQuery-DropShard_148Ò

beam:transform:pardo:v1'
Noneref_PCollection_PCollection_109"'
Noneref_PCollection_PCollection_110*#Dynamic/_StreamToBigQuery/DropShard2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2t
beam:display_data:labelled:v1S
Transform Function<lambda>2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
¢
href_AppliedPTransform_Stream-_StreamToBigQuery-CommitInsertIds-FlatMap-restore_metadata_default_wind_133µ

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_96"&
Noneref_PCollection_PCollection_97*QStream/_StreamToBigQuery/CommitInsertIds/FlatMap(restore_metadata_default_window)2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2ã
beam:display_data:labelled:v1j
Transform Functionrestore_metadata_default_window2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
û
3ref_AppliedPTransform_ReadDirect-Read-EmitSource_32Ê

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_18"&
Noneref_PCollection_PCollection_19*ReadDirect/Read/EmitSource2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2t
beam:display_data:labelled:v1S
Transform Function<lambda>2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo

ref_AppliedPTransform_Load_65Œ
&
$beam:transform:write_to_big_query:v04ref_AppliedPTransform_Load-BigQueryBatchFileLoads_66%
Noneref_PCollection_PCollection_8*Load2l
beam:display_data:labelled:v1K
Tableproj:ds.report2table:+apache_beam.io.gcp.bigquery.WriteToBigQuery2p
beam:display_data:labelled:v1O
Validation Enabled2
validation:+apache_beam.io.gcp.bigquery.WriteToBigQuery:%ref_Environment_default_environment_1B:
python_type+apache_beam.io.gcp.bigquery.WriteToBigQuery
Ï
Jref_AppliedPTransform_Load-BigQueryBatchFileLoads-DestinationFilesUnion_89ù

beam:transform:flatten:v1#
1ref_PCollection_PCollection_58#
0ref_PCollection_PCollection_54"&
Noneref_PCollection_PCollection_59*1Load/BigQueryBatchFileLoads/DestinationFilesUnion:%ref_Environment_default_environment_1B2
python_type#apache_beam.transforms.core.Flatten
ß
?ref_AppliedPTransform_ReadDirect-Read-SDFBoundedSourceReader_33„
%
#beam:transform:generic_composite:v1[ref_AppliedPTransform_ReadDirect-Read-SDFBoundedSourceReader-ParDo-SDFBoundedSourceDoFn-_34&
Noneref_PCollection_PCollection_19"&
Noneref_PCollection_PCollection_20*&ReadDirect/Read/SDFBoundedSourceReader2l
beam:display_data:labelled:v1K
methodDIRECT_READ2method:,apache_beam.io.iobase.SDFBoundedSourceReader2s
beam:display_data:labelled:v1R
output_formatAVRO2output_format:,apache_beam.io.iobase.SDFBoundedSourceReader2g
beam:display_data:labelled:v1F
projectNone2project:,apache_beam.io.iobase.SDFBoundedSourceReader2æ
beam:display_data:labelled:v1ú
table_referenceJ<TableReference
 datasetId: 'ds'
 projectId: 'proj'
 tableId: 'customers'>2table_reference:,apache_beam.io.iobase.SDFBoundedSourceReader2c
beam:display_data:labelled:v1B
queryNone2query:,apache_beam.io.iobase.SDFBoundedSourceReader2q
beam:display_data:labelled:v1P
use_legacy_sql2use_legacy_sql:,apache_beam.io.iobase.SDFBoundedSourceReader2{
beam:display_data:labelled:v1Z
use_native_datetime 2use_native_datetime:,apache_beam.io.iobase.SDFBoundedSourceReader2w
beam:display_data:labelled:v1V
selected_fieldsNone2selected_fields:,apache_beam.io.iobase.SDFBoundedSourceReader2w
beam:display_data:labelled:v1V
row_restrictionNone2row_restriction:,apache_beam.io.iobase.SDFBoundedSourceReader2ê
beam:display_data:labelled:v1o
)This Dataflow job launches bigquery jobs.2launchesBigQueryJobs:*apache_beam.io.iobase.SDFBoundedSourceDoFn2ô
beam:display_data:labelled:v1x
source8apache_beam.io.gcp.bigquery._CustomBigQueryStorageSource2source:,apache_beam.io.iobase.SDFBoundedSourceReader:%ref_Environment_default_environment_1B;
python_type,apache_beam.io.iobase.SDFBoundedSourceReader
¿
@ref_AppliedPTransform_Dynamic-_StreamToBigQuery-AddInsertIds_141˚

beam:transform:pardo:v1'
Noneref_PCollection_PCollection_103"'
Noneref_PCollection_PCollection_104*&Dynamic/_StreamToBigQuery/AddInsertIds2ä
beam:display_data:labelled:v1i
Transform Function,apache_beam.io.gcp.bigquery.InsertIdPrefixFn2fn:!apache_beam.transforms.core.ParDo:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
ç
Sref_AppliedPTransform_Load-BigQueryBatchFileLoads-ImpulseSingleElementPC-Impulse_73µ

beam:transform:impulse:v1"&
Noneref_PCollection_PCollection_43*:Load/BigQueryBatchFileLoads/ImpulseSingleElementPC/ImpulseB2
python_type#apache_beam.transforms.core.Impulse
∞
Mref_AppliedPTransform_Stream-_StreamToBigQuery-CommitInsertIds-GroupByKey_132ﬁ
 
beam:transform:group_by_key:v1&
Noneref_PCollection_PCollection_95"&
Noneref_PCollection_PCollection_96*3Stream/_StreamToBigQuery/CommitInsertIds/GroupByKeyB5
python_type&apache_beam.transforms.core.GroupByKey
Ä
dref_AppliedPTransform_Load-BigQueryBatchFileLoads-ReshuffleBeforeLoadWithTempTables-AddRandomKeys_95ó

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_62"&
Noneref_PCollection_PCollection_65*KLoad/BigQueryBatchFileLoads/ReshuffleBeforeLoadWithTempTables/AddRandomKeys2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2t
beam:display_data:labelled:v1S
Transform Function<lambda>2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
‘
Oref_AppliedPTransform_Load-BigQueryBatchFileLoads-ImpulseEmptyPC-Map-decode-_71Ä

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_41"&
Noneref_PCollection_PCollection_42*6Load/BigQueryBatchFileLoads/ImpulseEmptyPC/Map(decode)2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2r
beam:display_data:labelled:v1Q
Transform Functiondecode2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
ª
Rref_AppliedPTransform_Load-BigQueryBatchFileLoads-GroupFilesByTableDestinations_91‰
 
beam:transform:group_by_key:v1&
Noneref_PCollection_PCollection_60"&
Noneref_PCollection_PCollection_61*9Load/BigQueryBatchFileLoads/GroupFilesByTableDestinationsB5
python_type&apache_beam.transforms.core.GroupByKey
Â
Aref_AppliedPTransform_ReadQuery-_PassThroughThenCleanup-Create_59ü
"
 beam:transform:pickled_python:v1Iref_AppliedPTransform_ReadQuery-_PassThroughThenCleanup-Create-Impulse_60aref_AppliedPTransform_ReadQuery-_PassThroughThenCleanup-Create-FlatMap-lambda-at-core-py-4094-_61Pref_AppliedPTransform_ReadQuery-_PassThroughThenCleanup-Create-MaybeReshuffle_62Mref_AppliedPTransform_ReadQuery-_PassThroughThenCleanup-Create-Map-decode-_63"&
Noneref_PCollection_PCollection_38*(ReadQuery/_PassThroughThenCleanup/Create:%ref_Environment_default_environment_1B1
python_type"apache_beam.transforms.core.Create
”
Fref_AppliedPTransform_Dynamic-_StreamToBigQuery-ToHashableTableRef_142à

beam:transform:pardo:v1'
Noneref_PCollection_PCollection_104"'
Noneref_PCollection_PCollection_105*,Dynamic/_StreamToBigQuery/ToHashableTableRef2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2Å
beam:display_data:labelled:v1`
Transform Functionto_hashable_table_ref2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
™
Zref_AppliedPTransform_ReadQuery-Read-SDFBoundedSourceReader-ParDo-SDFBoundedSourceDoFn-_55À

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_32"&
Noneref_PCollection_PCollection_33*AReadQuery/Read/SDFBoundedSourceReader/ParDo(SDFBoundedSourceDoFn)2à
beam:display_data:labelled:v1g
Transform Function*apache_beam.io.iobase.SDFBoundedSourceDoFn2fn:!apache_beam.transforms.core.ParDo2e
beam:display_data:labelled:v1D
methodEXPORT2method:*apache_beam.io.iobase.SDFBoundedSourceDoFn2a
beam:display_data:labelled:v1@
tableNone2table:*apache_beam.io.iobase.SDFBoundedSourceDoFn2u
beam:display_data:labelled:v1T
querySELECT id FROM ds.orders2query:*apache_beam.io.iobase.SDFBoundedSourceDoFn2e
beam:display_data:labelled:v1D
projectNone2project:*apache_beam.io.iobase.SDFBoundedSourceDoFn2o
beam:display_data:labelled:v1N
use_legacy_sql 2use_legacy_sql:*apache_beam.io.iobase.SDFBoundedSourceDoFn2{
beam:display_data:labelled:v1Z
bigquery_job_labels{}2bigquery_job_labels:*apache_beam.io.iobase.SDFBoundedSourceDoFn2{
beam:display_data:labelled:v1Z
export_file_formatAVRO2export_file_format:*apache_beam.io.iobase.SDFBoundedSourceDoFn2ê
beam:display_data:labelled:v1o
)This Dataflow job launches bigquery jobs.2launchesBigQueryJobs:*apache_beam.io.iobase.SDFBoundedSourceDoFn2ê
beam:display_data:labelled:v1o
source1apache_beam.io.gcp.bigquery._CustomBigQuerySource2source:*apache_beam.io.iobase.SDFBoundedSourceDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
º
Cref_AppliedPTransform_ReadQuery-FilesToRemoveImpulse-Map-decode-_49Ù

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_28"&
Noneref_PCollection_PCollection_29**ReadQuery/FilesToRemoveImpulse/Map(decode)2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2r
beam:display_data:labelled:v1Q
Transform Functiondecode2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
ƒ
Fref_AppliedPTransform_Load-BigQueryBatchFileLoads-LoadJobNamePrefix_77˘

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_45"&
Noneref_PCollection_PCollection_46*-Load/BigQueryBatchFileLoads/LoadJobNamePrefix2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2t
beam:display_data:labelled:v1S
Transform Function<lambda>2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
∆
gref_AppliedPTransform_Load-BigQueryBatchFileLoads-ReshuffleBeforeLoadWithTempTables-ReshufflePerKey-_99⁄

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_67"&
Noneref_PCollection_PCollection_68*vLoad/BigQueryBatchFileLoads/ReshuffleBeforeLoadWithTempTables/ReshufflePerKey/FlatMap(restore_metadata_default_window)2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2ã
beam:display_data:labelled:v1j
Transform Functionrestore_metadata_default_window2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
Ÿ
Zref_AppliedPTransform_ReadTable-Read-SDFBoundedSourceReader-ParDo-SDFBoundedSourceDoFn-_13˙

beam:transform:pardo:v1%
Noneref_PCollection_PCollection_6"%
Noneref_PCollection_PCollection_7*AReadTable/Read/SDFBoundedSourceReader/ParDo(SDFBoundedSourceDoFn)2à
beam:display_data:labelled:v1g
Transform Function*apache_beam.io.iobase.SDFBoundedSourceDoFn2fn:!apache_beam.transforms.core.ParDo2e
beam:display_data:labelled:v1D
methodEXPORT2method:*apache_beam.io.iobase.SDFBoundedSourceDoFn2•
beam:display_data:labelled:v1É
tableG<TableReference
 datasetId: 'ds'
 projectId: 'proj'
 tableId: 'orders'>2table:*apache_beam.io.iobase.SDFBoundedSourceDoFn2a
beam:display_data:labelled:v1@
queryNone2query:*apache_beam.io.iobase.SDFBoundedSourceDoFn2e
beam:display_data:labelled:v1D
projectNone2project:*apache_beam.io.iobase.SDFBoundedSourceDoFn2o
beam:display_data:labelled:v1N
use_legacy_sql2use_legacy_sql:*apache_beam.io.iobase.SDFBoundedSourceDoFn2{
beam:display_data:labelled:v1Z
bigquery_job_labels{}2bigquery_job_labels:*apache_beam.io.iobase.SDFBoundedSourceDoFn2{
beam:display_data:labelled:v1Z
export_file_formatAVRO2export_file_format:*apache_beam.io.iobase.SDFBoundedSourceDoFn2ê
beam:display_data:labelled:v1o
)This Dataflow job launches bigquery jobs.2launchesBigQueryJobs:*apache_beam.io.iobase.SDFBoundedSourceDoFn2ê
beam:display_data:labelled:v1o
source1apache_beam.io.gcp.bigquery._CustomBigQuerySource2source:*apache_beam.io.iobase.SDFBoundedSourceDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
√
3ref_AppliedPTransform_Dynamic-_StreamToBigQuery_139ã
%
#beam:transform:generic_composite:v1Eref_AppliedPTransform_Dynamic-_StreamToBigQuery-AppendDestination_140@ref_AppliedPTransform_Dynamic-_StreamToBigQuery-AddInsertIds_141Fref_AppliedPTransform_Dynamic-_StreamToBigQuery-ToHashableTableRef_142Eref_AppliedPTransform_Dynamic-_StreamToBigQuery-WithFixedSharding_143Cref_AppliedPTransform_Dynamic-_StreamToBigQuery-CommitInsertIds_144=ref_AppliedPTransform_Dynamic-_StreamToBigQuery-DropShard_148Href_AppliedPTransform_Dynamic-_StreamToBigQuery-FromHashableTableRef_149Dref_AppliedPTransform_Dynamic-_StreamToBigQuery-StreamInsertRows_150%
Noneref_PCollection_PCollection_8"7
FailedRowsWithErrorsref_PCollection_PCollection_113"-

FailedRowsref_PCollection_PCollection_112"'
Noneref_PCollection_PCollection_114*Dynamic/_StreamToBigQuery:%ref_Environment_default_environment_1B<
python_type-apache_beam.io.gcp.bigquery._StreamToBigQuery
º
:ref_AppliedPTransform_ReadQuery-_PassThroughThenCleanup_56˝
%
#beam:transform:generic_composite:v1Mref_AppliedPTransform_ReadQuery-_PassThroughThenCleanup-ParDo-PassThrough-_57Aref_AppliedPTransform_ReadQuery-_PassThroughThenCleanup-Create_59Vref_AppliedPTransform_ReadQuery-_PassThroughThenCleanup-ParDo-RemoveExtractedFiles-_64&
Noneref_PCollection_PCollection_33"&
Noneref_PCollection_PCollection_34*!ReadQuery/_PassThroughThenCleanup:%ref_Environment_default_environment_1BP
python_typeAapache_beam.io.gcp.bigquery_read_internal._PassThroughThenCleanup
¨
Nref_AppliedPTransform_ReadDirect-_PassThroughThenCleanupTempDatasets-Create_38Ÿ
"
 beam:transform:pickled_python:v1Vref_AppliedPTransform_ReadDirect-_PassThroughThenCleanupTempDatasets-Create-Impulse_39gref_AppliedPTransform_ReadDirect-_PassThroughThenCleanupTempDatasets-Create-FlatMap-lambda-at-core-p_40]ref_AppliedPTransform_ReadDirect-_PassThroughThenCleanupTempDatasets-Create-MaybeReshuffle_41Zref_AppliedPTransform_ReadDirect-_PassThroughThenCleanupTempDatasets-Create-Map-decode-_42"&
Noneref_PCollection_PCollection_25*5ReadDirect/_PassThroughThenCleanupTempDatasets/Create:%ref_Environment_default_environment_1B1
python_type"apache_beam.transforms.core.Create
¿
Dref_AppliedPTransform_Load-BigQueryBatchFileLoads-DropShardNumber_87˜

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_56"&
Noneref_PCollection_PCollection_57*+Load/BigQueryBatchFileLoads/DropShardNumber2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2t
beam:display_data:labelled:v1S
Transform Function<lambda>2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
ä
href_AppliedPTransform_Load-BigQueryBatchFileLoads-ReshuffleBeforeLoadWithoutTempTables-RemoveRandomK_107ù

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_73"&
Noneref_PCollection_PCollection_74*QLoad/BigQueryBatchFileLoads/ReshuffleBeforeLoadWithoutTempTables/RemoveRandomKeys2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2t
beam:display_data:labelled:v1S
Transform Function<lambda>2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
ô
>ref_AppliedPTransform_ReadTable-Read-SDFBoundedSourceReader_12÷
%
#beam:transform:generic_composite:v1Zref_AppliedPTransform_ReadTable-Read-SDFBoundedSourceReader-ParDo-SDFBoundedSourceDoFn-_13%
Noneref_PCollection_PCollection_6"%
Noneref_PCollection_PCollection_7*%ReadTable/Read/SDFBoundedSourceReader2g
beam:display_data:labelled:v1F
methodEXPORT2method:,apache_beam.io.iobase.SDFBoundedSourceReader2ß
beam:display_data:labelled:v1Ö
tableG<TableReference
 datasetId: 'ds'
 projectId: 'proj'
 tableId: 'orders'>2table:,apache_beam.io.iobase.SDFBoundedSourceReader2c
beam:display_data:labelled:v1B
queryNone2query:,apache_beam.io.iobase.SDFBoundedSourceReader2g
beam:display_data:labelled:v1F
projectNone2project:,apache_beam.io.iobase.SDFBoundedSourceReader2q
beam:display_data:labelled:v1P
use_legacy_sql2use_legacy_sql:,apache_beam.io.iobase.SDFBoundedSourceReader2}
beam:display_data:labelled:v1\
bigquery_job_labels{}2bigquery_job_labels:,apache_beam.io.iobase.SDFBoundedSourceReader2}
beam:display_data:labelled:v1\
export_file_formatAVRO2export_file_format:,apache_beam.io.iobase.SDFBoundedSourceReader2ê
beam:display_data:labelled:v1o
)This Dataflow job launches bigquery jobs.2launchesBigQueryJobs:*apache_beam.io.iobase.SDFBoundedSourceDoFn2í
beam:display_data:labelled:v1q
source1apache_beam.io.gcp.bigquery._CustomBigQuerySource2source:,apache_beam.io.iobase.SDFBoundedSourceReader:%ref_Environment_default_environment_1B;
python_type,apache_beam.io.iobase.SDFBoundedSourceReader
Â
?ref_AppliedPTransform_ReadQuery-FilesToRemoveImpulse-Impulse_46°

beam:transform:impulse:v1"&
Noneref_PCollection_PCollection_27*&ReadQuery/FilesToRemoveImpulse/ImpulseB2
python_type#apache_beam.transforms.core.Impulse
›
'ref_AppliedPTransform_ReadQuery-Read_51±

beam:transform:read:v1/ref_AppliedPTransform_ReadQuery-Read-Impulse_522ref_AppliedPTransform_ReadQuery-Read-EmitSource_53>ref_AppliedPTransform_ReadQuery-Read-SDFBoundedSourceReader_54"&
Noneref_PCollection_PCollection_33*ReadQuery/Read2Ö
beam:display_data:labelled:v1d
Read Source1apache_beam.io.gcp.bigquery._CustomBigQuerySource2source:apache_beam.io.iobase.Read2l
beam:display_data:labelled:v1K
methodEXPORT2method:1apache_beam.io.gcp.bigquery._CustomBigQuerySource2h
beam:display_data:labelled:v1G
tableNone2table:1apache_beam.io.gcp.bigquery._CustomBigQuerySource2|
beam:display_data:labelled:v1[
querySELECT id FROM ds.orders2query:1apache_beam.io.gcp.bigquery._CustomBigQuerySource2l
beam:display_data:labelled:v1K
projectNone2project:1apache_beam.io.gcp.bigquery._CustomBigQuerySource2v
beam:display_data:labelled:v1U
use_legacy_sql 2use_legacy_sql:1apache_beam.io.gcp.bigquery._CustomBigQuerySource2Ç
beam:display_data:labelled:v1a
bigquery_job_labels{}2bigquery_job_labels:1apache_beam.io.gcp.bigquery._CustomBigQuerySource2Ç
beam:display_data:labelled:v1a
export_file_formatAVRO2export_file_format:1apache_beam.io.gcp.bigquery._CustomBigQuerySource2ó
beam:display_data:labelled:v1v
)This Dataflow job launches bigquery jobs.2launchesBigQueryJobs:1apache_beam.io.gcp.bigquery._CustomBigQuerySource:%ref_Environment_default_environment_1B)
python_typeapache_beam.io.iobase.Read
˘
Iref_AppliedPTransform_ReadTable-_PassThroughThenCleanup-Create-Impulse_18´

beam:transform:impulse:v1"&
Noneref_PCollection_PCollection_10*0ReadTable/_PassThroughThenCleanup/Create/ImpulseB2
python_type#apache_beam.transforms.core.Impulse
´
Zref_AppliedPTransform_Load-BigQueryBatchFileLoads-ReshuffleBeforeLoadWithoutTempTables_101Ã

beam:transform:reshuffle:v1href_AppliedPTransform_Load-BigQueryBatchFileLoads-ReshuffleBeforeLoadWithoutTempTables-AddRandomKeys_102href_AppliedPTransform_Load-BigQueryBatchFileLoads-ReshuffleBeforeLoadWithoutTempTables-ReshufflePerK_103href_AppliedPTransform_Load-BigQueryBatchFileLoads-ReshuffleBeforeLoadWithoutTempTables-RemoveRandomK_107&
Noneref_PCollection_PCollection_64"&
Noneref_PCollection_PCollection_74*@Load/BigQueryBatchFileLoads/ReshuffleBeforeLoadWithoutTempTables:%ref_Environment_default_environment_1B4
python_type%apache_beam.transforms.util.Reshuffle
Ë
Nref_AppliedPTransform_Load-BigQueryBatchFileLoads-ParDo-_ShardDestinations-_85ï

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_53"&
Noneref_PCollection_PCollection_55*5Load/BigQueryBatchFileLoads/ParDo(_ShardDestinations)2ó
beam:display_data:labelled:v1v
Transform Function9apache_beam.io.gcp.bigquery_file_loads._ShardDestinations2fn:!apache_beam.transforms.core.ParDo:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
Â
Vref_AppliedPTransform_ReadTable-FilesToRemoveImpulse-FlatMap-lambda-at-core-py-4094-_5ä

beam:transform:pardo:v1%
Noneref_PCollection_PCollection_1"%
Noneref_PCollection_PCollection_2*@ReadTable/FilesToRemoveImpulse/FlatMap(<lambda at core.py:4094>)2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2t
beam:display_data:labelled:v1S
Transform Function<lambda>2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
è
gref_AppliedPTransform_ReadDirect-_PassThroughThenCleanupTempDatasets-Create-FlatMap-lambda-at-core-p_40£

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_23"&
Noneref_PCollection_PCollection_24*WReadDirect/_PassThroughThenCleanupTempDatasets/Create/FlatMap(<lambda at core.py:4094>)2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2t
beam:display_data:labelled:v1S
Transform Function<lambda>2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
é
Cref_AppliedPTransform_Stream-_StreamToBigQuery-StreamInsertRows_136∆
%
#beam:transform:generic_composite:v1Zref_AppliedPTransform_Stream-_StreamToBigQuery-StreamInsertRows-ParDo-BigQueryWriteFn-_137&
Noneref_PCollection_PCollection_99"7
FailedRowsWithErrorsref_PCollection_PCollection_101"-

FailedRowsref_PCollection_PCollection_100"'
Noneref_PCollection_PCollection_102*)Stream/_StreamToBigQuery/StreamInsertRows:%ref_Environment_default_environment_1B6
python_type'apache_beam.transforms.core._MultiParDo
ƒ
Fref_AppliedPTransform_Load-BigQueryBatchFileLoads-CopyJobNamePrefix_79˘

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_45"&
Noneref_PCollection_PCollection_48*-Load/BigQueryBatchFileLoads/CopyJobNamePrefix2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2t
beam:display_data:labelled:v1S
Transform Function<lambda>2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
Ò
Cref_AppliedPTransform_Load-BigQueryBatchFileLoads-ImpulseEmptyPC_67©
"
 beam:transform:pickled_python:v1Kref_AppliedPTransform_Load-BigQueryBatchFileLoads-ImpulseEmptyPC-Impulse_68cref_AppliedPTransform_Load-BigQueryBatchFileLoads-ImpulseEmptyPC-FlatMap-lambda-at-core-py-4094-_69Rref_AppliedPTransform_Load-BigQueryBatchFileLoads-ImpulseEmptyPC-MaybeReshuffle_70Oref_AppliedPTransform_Load-BigQueryBatchFileLoads-ImpulseEmptyPC-Map-decode-_71"&
Noneref_PCollection_PCollection_42**Load/BigQueryBatchFileLoads/ImpulseEmptyPC:%ref_Environment_default_environment_1B1
python_type"apache_beam.transforms.core.Create
ù
Kref_AppliedPTransform_Load-BigQueryBatchFileLoads-ImpulseSingleElementPC_72Õ
"
 beam:transform:pickled_python:v1Sref_AppliedPTransform_Load-BigQueryBatchFileLoads-ImpulseSingleElementPC-Impulse_73gref_AppliedPTransform_Load-BigQueryBatchFileLoads-ImpulseSingleElementPC-FlatMap-lambda-at-core-py-4_74Zref_AppliedPTransform_Load-BigQueryBatchFileLoads-ImpulseSingleElementPC-MaybeReshuffle_75Wref_AppliedPTransform_Load-BigQueryBatchFileLoads-ImpulseSingleElementPC-Map-decode-_76"&
Noneref_PCollection_PCollection_45*2Load/BigQueryBatchFileLoads/ImpulseSingleElementPC:%ref_Environment_default_environment_1B1
python_type"apache_beam.transforms.core.Create
ˇ
Rref_AppliedPTransform_Load-BigQueryBatchFileLoads-ImpulseEmptyPC-MaybeReshuffle_70®
"
 beam:transform:pickled_python:v1&
Noneref_PCollection_PCollection_41"&
Noneref_PCollection_PCollection_41*9Load/BigQueryBatchFileLoads/ImpulseEmptyPC/MaybeReshuffle:%ref_Environment_default_environment_1BP
python_typeAapache_beam.transforms.core.Create.expand.<locals>.MaybeReshuffle
Í
Zref_AppliedPTransform_ReadDirect-_PassThroughThenCleanupTempDatasets-Create-Map-decode-_42ã

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_24"&
Noneref_PCollection_PCollection_25*AReadDirect/_PassThroughThenCleanupTempDatasets/Create/Map(decode)2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2r
beam:display_data:labelled:v1Q
Transform Functiondecode2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
Ó
Mref_AppliedPTransform_ReadTable-_PassThroughThenCleanup-ParDo-PassThrough-_15ú
%
#beam:transform:generic_composite:v1_ref_AppliedPTransform_ReadTable-_PassThroughThenCleanup-ParDo-PassThrough-ParDo-PassThrough-_16%
Noneref_PCollection_PCollection_7"/
cleanup_signalref_PCollection_PCollection_9"%
Noneref_PCollection_PCollection_8*4ReadTable/_PassThroughThenCleanup/ParDo(PassThrough):%ref_Environment_default_environment_1B6
python_type'apache_beam.transforms.core._MultiParDo
π
Bref_AppliedPTransform_ReadTable-FilesToRemoveImpulse-Map-decode-_7Ú

beam:transform:pardo:v1%
Noneref_PCollection_PCollection_2"%
Noneref_PCollection_PCollection_3**ReadTable/FilesToRemoveImpulse/Map(decode)2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2r
beam:display_data:labelled:v1Q
Transform Functiondecode2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
˚
Pref_AppliedPTransform_ReadTable-_PassThroughThenCleanup-Create-MaybeReshuffle_20¶
"
 beam:transform:pickled_python:v1&
Noneref_PCollection_PCollection_11"&
Noneref_PCollection_PCollection_11*7ReadTable/_PassThroughThenCleanup/Create/MaybeReshuffle:%ref_Environment_default_environment_1BP
python_typeAapache_beam.transforms.core.Create.expand.<locals>.MaybeReshuffle
Â
[ref_AppliedPTransform_Dynamic-_StreamToBigQuery-StreamInsertRows-ParDo-BigQueryWriteFn-_151Ö

beam:transform:pardo:v1'
Noneref_PCollection_PCollection_111"7
FailedRowsWithErrorsref_PCollection_PCollection_113"-

FailedRowsref_PCollection_PCollection_112"'
Noneref_PCollection_PCollection_114*ADynamic/_StreamToBigQuery/StreamInsertRows/ParDo(BigQueryWriteFn)2â
beam:display_data:labelled:v1h
Transform Function+apache_beam.io.gcp.bigquery.BigQueryWriteFn2fn:!apache_beam.transforms.core.ParDo2q
beam:display_data:labelled:v1P
max_batch_size(Ù2max_batch_size:+apache_beam.io.gcp.bigquery.BigQueryWriteFn2w
beam:display_data:labelled:v1V
max_buffered_rows(–2max_buffered_rows:+apache_beam.io.gcp.bigquery.BigQueryWriteFn2|
beam:display_data:labelled:v1[
retry_strategyRETRY_ALWAYS2retry_strategy:+apache_beam.io.gcp.bigquery.BigQueryWriteFn2à
beam:display_data:labelled:v1g
create_dispositionCREATE_IF_NEEDED2create_disposition:+apache_beam.io.gcp.bigquery.BigQueryWriteFn2Ç
beam:display_data:labelled:v1a
write_dispositionWRITE_APPEND2write_disposition:+apache_beam.io.gcp.bigquery.BigQueryWriteFn2Ü
beam:display_data:labelled:v1e
additional_bq_parameters{}2additional_bq_parameters:+apache_beam.io.gcp.bigquery.BigQueryWriteFn2{
beam:display_data:labelled:v1Z
ignore_insert_idsFalse2ignore_insert_ids:+apache_beam.io.gcp.bigquery.BigQueryWriteFn2Ö
beam:display_data:labelled:v1d
ignore_unknown_columnsFalse2ignore_unknown_columns:+apache_beam.io.gcp.bigquery.BigQueryWriteFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
û
Vref_AppliedPTransform_Load-BigQueryBatchFileLoads-ReshuffleBeforeLoadWithTempTables_94√

beam:transform:reshuffle:v1dref_AppliedPTransform_Load-BigQueryBatchFileLoads-ReshuffleBeforeLoadWithTempTables-AddRandomKeys_95fref_AppliedPTransform_Load-BigQueryBatchFileLoads-ReshuffleBeforeLoadWithTempTables-ReshufflePerKey_96href_AppliedPTransform_Load-BigQueryBatchFileLoads-ReshuffleBeforeLoadWithTempTables-RemoveRandomKeys_100&
Noneref_PCollection_PCollection_62"&
Noneref_PCollection_PCollection_69*=Load/BigQueryBatchFileLoads/ReshuffleBeforeLoadWithTempTables:%ref_Environment_default_environment_1B4
python_type%apache_beam.transforms.util.Reshuffle
ì
Vref_AppliedPTransform_ReadDirect-_PassThroughThenCleanupTempDatasets-Create-Impulse_39∏

beam:transform:impulse:v1"&
Noneref_PCollection_PCollection_23*=ReadDirect/_PassThroughThenCleanupTempDatasets/Create/ImpulseB2
python_type#apache_beam.transforms.core.Impulse

[ref_AppliedPTransform_ReadDirect-ProjectToCleanupImpulse-FlatMap-lambda-at-core-py-4094-_26ê

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_14"&
Noneref_PCollection_PCollection_15*DReadDirect/ProjectToCleanupImpulse/FlatMap(<lambda at core.py:4094>)2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2t
beam:display_data:labelled:v1S
Transform Function<lambda>2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
∂
_ref_AppliedPTransform_ReadTable-_PassThroughThenCleanup-ParDo-PassThrough-ParDo-PassThrough-_16“

beam:transform:pardo:v1%
Noneref_PCollection_PCollection_7"/
cleanup_signalref_PCollection_PCollection_9"%
Noneref_PCollection_PCollection_8*GReadTable/_PassThroughThenCleanup/ParDo(PassThrough)/ParDo(PassThrough)2ì
beam:display_data:labelled:v1r
Transform Function5apache_beam.io.gcp.bigquery_read_internal.PassThrough2fn:!apache_beam.transforms.core.ParDo:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
¥
Nref_AppliedPTransform_Dynamic-_StreamToBigQuery-CommitInsertIds-GroupByKey_146·
 
beam:transform:group_by_key:v1'
Noneref_PCollection_PCollection_107"'
Noneref_PCollection_PCollection_108*4Dynamic/_StreamToBigQuery/CommitInsertIds/GroupByKeyB5
python_type&apache_beam.transforms.core.GroupByKey
‚
[ref_AppliedPTransform_ReadDirect-Read-SDFBoundedSourceReader-ParDo-SDFBoundedSourceDoFn-_34Ç

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_19"&
Noneref_PCollection_PCollection_20*BReadDirect/Read/SDFBoundedSourceReader/ParDo(SDFBoundedSourceDoFn)2à
beam:display_data:labelled:v1g
Transform Function*apache_beam.io.iobase.SDFBoundedSourceDoFn2fn:!apache_beam.transforms.core.ParDo2j
beam:display_data:labelled:v1I
methodDIRECT_READ2method:*apache_beam.io.iobase.SDFBoundedSourceDoFn2q
beam:display_data:labelled:v1P
output_formatAVRO2output_format:*apache_beam.io.iobase.SDFBoundedSourceDoFn2e
beam:display_data:labelled:v1D
projectNone2project:*apache_beam.io.iobase.SDFBoundedSourceDoFn2º
beam:display_data:labelled:v1ö
table_referenceJ<TableReference
 datasetId: 'ds'
 projectId: 'proj'
 tableId: 'customers'>2table_reference:*apache_beam.io.iobase.SDFBoundedSourceDoFn2a
beam:display_data:labelled:v1@
queryNone2query:*apache_beam.io.iobase.SDFBoundedSourceDoFn2o
beam:display_data:labelled:v1N
use_legacy_sql2use_legacy_sql:*apache_beam.io.iobase.SDFBoundedSourceDoFn2y
beam:display_data:labelled:v1X
use_native_datetime 2use_native_datetime:*apache_beam.io.iobase.SDFBoundedSourceDoFn2u
beam:display_data:labelled:v1T
selected_fieldsNone2selected_fields:*apache_beam.io.iobase.SDFBoundedSourceDoFn2u
beam:display_data:labelled:v1T
row_restrictionNone2row_restriction:*apache_beam.io.iobase.SDFBoundedSourceDoFn2ê
beam:display_data:labelled:v1o
)This Dataflow job launches bigquery jobs.2launchesBigQueryJobs:*apache_beam.io.iobase.SDFBoundedSourceDoFn2ó
beam:display_data:labelled:v1v
source8apache_beam.io.gcp.bigquery._CustomBigQueryStorageSource2source:*apache_beam.io.iobase.SDFBoundedSourceDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
£
6ref_AppliedPTransform_ReadTable-FilesToRemoveImpulse_3Ë
"
 beam:transform:pickled_python:v1>ref_AppliedPTransform_ReadTable-FilesToRemoveImpulse-Impulse_4Vref_AppliedPTransform_ReadTable-FilesToRemoveImpulse-FlatMap-lambda-at-core-py-4094-_5Eref_AppliedPTransform_ReadTable-FilesToRemoveImpulse-MaybeReshuffle_6Bref_AppliedPTransform_ReadTable-FilesToRemoveImpulse-Map-decode-_7"%
Noneref_PCollection_PCollection_3*ReadTable/FilesToRemoveImpulse:%ref_Environment_default_environment_1B1
python_type"apache_beam.transforms.core.Create
æ
gref_AppliedPTransform_Load-BigQueryBatchFileLoads-ReshuffleBeforeLoadWithTempTables-ReshufflePerKey-_97“

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_65"&
Noneref_PCollection_PCollection_66*pLoad/BigQueryBatchFileLoads/ReshuffleBeforeLoadWithTempTables/ReshufflePerKey/Map(reify_metadata_default_window)2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2â
beam:display_data:labelled:v1h
Transform Functionreify_metadata_default_window2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
ƒ
Tref_AppliedPTransform_Load-BigQueryBatchFileLoads-RemoveTempTables-GetTableNames_118Î
%
#beam:transform:generic_composite:v1Yref_AppliedPTransform_Load-BigQueryBatchFileLoads-RemoveTempTables-GetTableNames-Keys_119&
Noneref_PCollection_PCollection_85"&
Noneref_PCollection_PCollection_86*:Load/BigQueryBatchFileLoads/RemoveTempTables/GetTableNames2j
beam:display_data:labelled:v1I
fnKeys2fn:9apache_beam.transforms.ptransform._PTransformFnPTransform:%ref_Environment_default_environment_1BH
python_type9apache_beam.transforms.ptransform._PTransformFnPTransform
—
=ref_AppliedPTransform_Load-BigQueryBatchFileLoads-Flatten_123è

beam:transform:flatten:v1#
1ref_PCollection_PCollection_89#
0ref_PCollection_PCollection_76"&
Noneref_PCollection_PCollection_90*#Load/BigQueryBatchFileLoads/Flatten:%ref_Environment_default_environment_1B2
python_type#apache_beam.transforms.core.Flatten
ﬂ
gref_AppliedPTransform_Load-BigQueryBatchFileLoads-ParDo-WriteRecordsToFile-ParDo-WriteRecordsToFile-_84Û

beam:transform:pardo:v14
python_side_input0ref_PCollection_PCollection_49&
Noneref_PCollection_PCollection_51"&
Noneref_PCollection_PCollection_52"2
UnwrittenRecordsref_PCollection_PCollection_53".
WrittenFilesref_PCollection_PCollection_54*OLoad/BigQueryBatchFileLoads/ParDo(WriteRecordsToFile)/ParDo(WriteRecordsToFile)2ó
beam:display_data:labelled:v1v
Transform Function9apache_beam.io.gcp.bigquery_file_loads.WriteRecordsToFile2fn:!apache_beam.transforms.core.ParDo2ä
beam:display_data:labelled:v1i
max_files_per_bundle(2max_files_per_bundle:9apache_beam.io.gcp.bigquery_file_loads.WriteRecordsToFile2â
beam:display_data:labelled:v1h
max_file_size43980465111042max_file_size:9apache_beam.io.gcp.bigquery_file_loads.WriteRecordsToFile2é
beam:display_data:labelled:v1m
file_formatNEWLINE_DELIMITED_JSON2file_format:9apache_beam.io.gcp.bigquery_file_loads.WriteRecordsToFile:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
π
_ref_AppliedPTransform_ReadQuery-_PassThroughThenCleanup-ParDo-PassThrough-ParDo-PassThrough-_58’

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_33"0
cleanup_signalref_PCollection_PCollection_35"&
Noneref_PCollection_PCollection_34*GReadQuery/_PassThroughThenCleanup/ParDo(PassThrough)/ParDo(PassThrough)2ì
beam:display_data:labelled:v1r
Transform Function5apache_beam.io.gcp.bigquery_read_internal.PassThrough2fn:!apache_beam.transforms.core.ParDo:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
Ù
^ref_AppliedPTransform_ReadDirect-_PassThroughThenCleanupTempDatasets-ParDo-CleanUpProjects-_43ë

beam:transform:pardo:v14
python_side_input0ref_PCollection_PCollection_224
python_side_input1ref_PCollection_PCollection_17&
Noneref_PCollection_PCollection_25"&
Noneref_PCollection_PCollection_26*EReadDirect/_PassThroughThenCleanupTempDatasets/ParDo(CleanUpProjects)2ó
beam:display_data:labelled:v1v
Transform Function9apache_beam.io.gcp.bigquery_read_internal.CleanUpProjects2fn:!apache_beam.transforms.core.ParDo:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
ÿ
Gref_AppliedPTransform_Load-BigQueryBatchFileLoads-RewindowIntoGlobal_81å

beam:transform:window_into:v1%
Noneref_PCollection_PCollection_8"&
Noneref_PCollection_PCollection_50*.Load/BigQueryBatchFileLoads/RewindowIntoGlobal2ã
beam:display_data:labelled:v1j
Transform Function(apache_beam.transforms.core.WindowIntoFn2fn:&apache_beam.transforms.core.WindowInto:%ref_Environment_default_environment_1B5
python_type&apache_beam.transforms.core.WindowInto
ï
Fref_AppliedPTransform_Load-BigQueryBatchFileLoads-AppendDestination_82 

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_50"&
Noneref_PCollection_PCollection_51*-Load/BigQueryBatchFileLoads/AppendDestination2î
beam:display_data:labelled:v1s
Transform Function6apache_beam.io.gcp.bigquery_tools.AppendDestinationsFn2fn:!apache_beam.transforms.core.ParDo2Ω
beam:display_data:labelled:v1õ
destinationG<TableReference
 datasetId: 'ds'
 projectId: 'proj'
 tableId: 'report'>2destination:6apache_beam.io.gcp.bigquery_tools.AppendDestinationsFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
É
eref_AppliedPTransform_Load-BigQueryBatchFileLoads-MapTuple-lambda-at-bigquery_file_loads-py-1244-_113ô

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_80"&
Noneref_PCollection_PCollection_81*MLoad/BigQueryBatchFileLoads/MapTuple(<lambda at bigquery_file_loads.py:1244>)2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2t
beam:display_data:labelled:v1S
Transform Function<lambda>2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
Ø
<ref_AppliedPTransform_Stream-_StreamToBigQuery-DropShard_134Ó

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_97"&
Noneref_PCollection_PCollection_98*"Stream/_StreamToBigQuery/DropShard2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2t
beam:display_data:labelled:v1S
Transform Function<lambda>2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
í
Dref_AppliedPTransform_Dynamic-_StreamToBigQuery-StreamInsertRows_150…
%
#beam:transform:generic_composite:v1[ref_AppliedPTransform_Dynamic-_StreamToBigQuery-StreamInsertRows-ParDo-BigQueryWriteFn-_151'
Noneref_PCollection_PCollection_111"7
FailedRowsWithErrorsref_PCollection_PCollection_113"-

FailedRowsref_PCollection_PCollection_112"'
Noneref_PCollection_PCollection_114**Dynamic/_StreamToBigQuery/StreamInsertRows:%ref_Environment_default_environment_1B6
python_type'apache_beam.transforms.core._MultiParDo
Û
 ref_AppliedPTransform_Stream_124Œ
&
$beam:transform:write_to_big_query:v02ref_AppliedPTransform_Stream-_StreamToBigQuery_125%
Noneref_PCollection_PCollection_8*Stream2l
beam:display_data:labelled:v1K
Tableproj:ds.events2table:+apache_beam.io.gcp.bigquery.WriteToBigQuery2p
beam:display_data:labelled:v1O
Validation Enabled2
validation:+apache_beam.io.gcp.bigquery.WriteToBigQuery:%ref_Environment_default_environment_1B:
python_type+apache_beam.io.gcp.bigquery.WriteToBigQuery
∆
Xref_AppliedPTransform_Load-BigQueryBatchFileLoads-RemoveTempTables-DeduplicateTables_117È
 
beam:transform:group_by_key:v1&
Noneref_PCollection_PCollection_84"&
Noneref_PCollection_PCollection_85*>Load/BigQueryBatchFileLoads/RemoveTempTables/DeduplicateTablesB5
python_type&apache_beam.transforms.core.GroupByKey
”
Href_AppliedPTransform_Dynamic-_StreamToBigQuery-FromHashableTableRef_149Ü

beam:transform:pardo:v1'
Noneref_PCollection_PCollection_110"'
Noneref_PCollection_PCollection_111*.Dynamic/_StreamToBigQuery/FromHashableTableRef2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2~
beam:display_data:labelled:v1]
Transform Function_restore_table_ref2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
–
Mref_AppliedPTransform_ReadTable-_PassThroughThenCleanup-Create-Map-decode-_21˛

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_11"&
Noneref_PCollection_PCollection_12*4ReadTable/_PassThroughThenCleanup/Create/Map(decode)2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2r
beam:display_data:labelled:v1Q
Transform Functiondecode2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
È
Vref_AppliedPTransform_ReadQuery-_PassThroughThenCleanup-ParDo-RemoveExtractedFiles-_64é

beam:transform:pardo:v14
python_side_input0ref_PCollection_PCollection_354
python_side_input1ref_PCollection_PCollection_30&
Noneref_PCollection_PCollection_38"&
Noneref_PCollection_PCollection_39*=ReadQuery/_PassThroughThenCleanup/ParDo(RemoveExtractedFiles)2ú
beam:display_data:labelled:v1{
Transform Function>apache_beam.io.gcp.bigquery_read_internal.RemoveExtractedFiles2fn:!apache_beam.transforms.core.ParDo:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
õ
fref_AppliedPTransform_Dynamic-_StreamToBigQuery-CommitInsertIds-Map-reify_metadata_default_window-_145∞

beam:transform:pardo:v1'
Noneref_PCollection_PCollection_106"'
Noneref_PCollection_PCollection_107*LDynamic/_StreamToBigQuery/CommitInsertIds/Map(reify_metadata_default_window)2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2â
beam:display_data:labelled:v1h
Transform Functionreify_metadata_default_window2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
˘
4ref_AppliedPTransform_Load-BigQueryBatchFileLoads_66¿
%
#beam:transform:generic_composite:v1Cref_AppliedPTransform_Load-BigQueryBatchFileLoads-ImpulseEmptyPC_67Kref_AppliedPTransform_Load-BigQueryBatchFileLoads-ImpulseSingleElementPC_72Fref_AppliedPTransform_Load-BigQueryBatchFileLoads-LoadJobNamePrefix_77Kref_AppliedPTransform_Load-BigQueryBatchFileLoads-SchemaModJobNamePrefix_78Fref_AppliedPTransform_Load-BigQueryBatchFileLoads-CopyJobNamePrefix_79Gref_AppliedPTransform_Load-BigQueryBatchFileLoads-GenerateFilePrefix_80Gref_AppliedPTransform_Load-BigQueryBatchFileLoads-RewindowIntoGlobal_81Fref_AppliedPTransform_Load-BigQueryBatchFileLoads-AppendDestination_82Nref_AppliedPTransform_Load-BigQueryBatchFileLoads-ParDo-WriteRecordsToFile-_83Nref_AppliedPTransform_Load-BigQueryBatchFileLoads-ParDo-_ShardDestinations-_85Eref_AppliedPTransform_Load-BigQueryBatchFileLoads-GroupShardedRows_86Dref_AppliedPTransform_Load-BigQueryBatchFileLoads-DropShardNumber_87Nref_AppliedPTransform_Load-BigQueryBatchFileLoads-WriteGroupedRecordsToFile_88Jref_AppliedPTransform_Load-BigQueryBatchFileLoads-DestinationFilesUnion_89Gref_AppliedPTransform_Load-BigQueryBatchFileLoads-IdentityWorkaround_90Rref_AppliedPTransform_Load-BigQueryBatchFileLoads-GroupFilesByTableDestinations_91Jref_AppliedPTransform_Load-BigQueryBatchFileLoads-ParDo-PartitionFiles-_92Vref_AppliedPTransform_Load-BigQueryBatchFileLoads-ReshuffleBeforeLoadWithTempTables_94Zref_AppliedPTransform_Load-BigQueryBatchFileLoads-ReshuffleBeforeLoadWithoutTempTables_101Sref_AppliedPTransform_Load-BigQueryBatchFileLoads-TriggerLoadJobsWithTempTables_108Tref_AppliedPTransform_Load-BigQueryBatchFileLoads-ParDo-UpdateDestinationSchema-_110eref_AppliedPTransform_Load-BigQueryBatchFileLoads-MapTuple-lambda-at-bigquery_file_loads-py-1240-_111@ref_AppliedPTransform_Load-BigQueryBatchFileLoads-GroupByKey_112eref_AppliedPTransform_Load-BigQueryBatchFileLoads-MapTuple-lambda-at-bigquery_file_loads-py-1244-_113Lref_AppliedPTransform_Load-BigQueryBatchFileLoads-ParDo-TriggerCopyJobs-_114Vref_AppliedPTransform_Load-BigQueryBatchFileLoads-RemoveTempTables-AddUselessValue_116Xref_AppliedPTransform_Load-BigQueryBatchFileLoads-RemoveTempTables-DeduplicateTables_117Tref_AppliedPTransform_Load-BigQueryBatchFileLoads-RemoveTempTables-GetTableNames_118Mref_AppliedPTransform_Load-BigQueryBatchFileLoads-RemoveTempTables-Delete_120Vref_AppliedPTransform_Load-BigQueryBatchFileLoads-TriggerLoadJobsWithoutTempTables_121=ref_AppliedPTransform_Load-BigQueryBatchFileLoads-Flatten_123%
Noneref_PCollection_PCollection_8">
destination_copy_jobid_pairsref_PCollection_PCollection_82"8
destination_file_pairsref_PCollection_PCollection_60">
destination_load_jobid_pairsref_PCollection_PCollection_90*Load/BigQueryBatchFileLoads:%ref_Environment_default_environment_1BL
python_type=apache_beam.io.gcp.bigquery_file_loads.BigQueryBatchFileLoads
œ
fref_AppliedPTransform_Load-BigQueryBatchFileLoads-ReshuffleBeforeLoadWithTempTables-ReshufflePerKey_96‰
%
#beam:transform:generic_composite:v1gref_AppliedPTransform_Load-BigQueryBatchFileLoads-ReshuffleBeforeLoadWithTempTables-ReshufflePerKey-_97gref_AppliedPTransform_Load-BigQueryBatchFileLoads-ReshuffleBeforeLoadWithTempTables-ReshufflePerKey-_98gref_AppliedPTransform_Load-BigQueryBatchFileLoads-ReshuffleBeforeLoadWithTempTables-ReshufflePerKey-_99&
Noneref_PCollection_PCollection_65"&
Noneref_PCollection_PCollection_68*MLoad/BigQueryBatchFileLoads/ReshuffleBeforeLoadWithTempTables/ReshufflePerKey:%ref_Environment_default_environment_1B:
python_type+apache_beam.transforms.util.ReshufflePerKey
à
Vref_AppliedPTransform_Load-BigQueryBatchFileLoads-TriggerLoadJobsWithoutTempTables_121≠
%
#beam:transform:generic_composite:v1href_AppliedPTransform_Load-BigQueryBatchFileLoads-TriggerLoadJobsWithoutTempTables-ParDo-TriggerLoad_122&
Noneref_PCollection_PCollection_74"-
OngoingJobsref_PCollection_PCollection_89"&
Noneref_PCollection_PCollection_88*<Load/BigQueryBatchFileLoads/TriggerLoadJobsWithoutTempTables:%ref_Environment_default_environment_1B6
python_type'apache_beam.transforms.core._MultiParDo
©
7ref_AppliedPTransform_ReadQuery-FilesToRemoveImpulse_45Ì
"
 beam:transform:pickled_python:v1?ref_AppliedPTransform_ReadQuery-FilesToRemoveImpulse-Impulse_46Wref_AppliedPTransform_ReadQuery-FilesToRemoveImpulse-FlatMap-lambda-at-core-py-4094-_47Fref_AppliedPTransform_ReadQuery-FilesToRemoveImpulse-MaybeReshuffle_48Cref_AppliedPTransform_ReadQuery-FilesToRemoveImpulse-Map-decode-_49"&
Noneref_PCollection_PCollection_29*ReadQuery/FilesToRemoveImpulse:%ref_Environment_default_environment_1B1
python_type"apache_beam.transforms.core.Create
‰
Eref_AppliedPTransform_ReadTable-FilesToRemoveImpulse-MaybeReshuffle_6ö
"
 beam:transform:pickled_python:v1%
Noneref_PCollection_PCollection_2"%
Noneref_PCollection_PCollection_2*-ReadTable/FilesToRemoveImpulse/MaybeReshuffle:%ref_Environment_default_environment_1BP
python_typeAapache_beam.transforms.core.Create.expand.<locals>.MaybeReshuffle
Ô
gref_AppliedPTransform_Load-BigQueryBatchFileLoads-ReshuffleBeforeLoadWithTempTables-ReshufflePerKey-_98É
 
beam:transform:group_by_key:v1&
Noneref_PCollection_PCollection_66"&
Noneref_PCollection_PCollection_67*XLoad/BigQueryBatchFileLoads/ReshuffleBeforeLoadWithTempTables/ReshufflePerKey/GroupByKeyB5
python_type&apache_beam.transforms.core.GroupByKey
•
href_AppliedPTransform_Dynamic-_StreamToBigQuery-CommitInsertIds-FlatMap-restore_metadata_default_win_147∏

beam:transform:pardo:v1'
Noneref_PCollection_PCollection_108"'
Noneref_PCollection_PCollection_109*RDynamic/_StreamToBigQuery/CommitInsertIds/FlatMap(restore_metadata_default_window)2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2ã
beam:display_data:labelled:v1j
Transform Functionrestore_metadata_default_window2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
Á
Vref_AppliedPTransform_ReadTable-_PassThroughThenCleanup-ParDo-RemoveExtractedFiles-_22å

beam:transform:pardo:v13
python_side_input0ref_PCollection_PCollection_93
python_side_input1ref_PCollection_PCollection_4&
Noneref_PCollection_PCollection_12"&
Noneref_PCollection_PCollection_13*=ReadTable/_PassThroughThenCleanup/ParDo(RemoveExtractedFiles)2ú
beam:display_data:labelled:v1{
Transform Function>apache_beam.io.gcp.bigquery_read_internal.RemoveExtractedFiles2fn:!apache_beam.transforms.core.ParDo:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
Ì
(ref_AppliedPTransform_ReadDirect-Read_30¿

beam:transform:read:v10ref_AppliedPTransform_ReadDirect-Read-Impulse_313ref_AppliedPTransform_ReadDirect-Read-EmitSource_32?ref_AppliedPTransform_ReadDirect-Read-SDFBoundedSourceReader_33"&
Noneref_PCollection_PCollection_20*ReadDirect/Read2å
beam:display_data:labelled:v1k
Read Source8apache_beam.io.gcp.bigquery._CustomBigQueryStorageSource2source:apache_beam.io.iobase.Read2x
beam:display_data:labelled:v1W
methodDIRECT_READ2method:8apache_beam.io.gcp.bigquery._CustomBigQueryStorageSource2
beam:display_data:labelled:v1^
output_formatAVRO2output_format:8apache_beam.io.gcp.bigquery._CustomBigQueryStorageSource2s
beam:display_data:labelled:v1R
projectNone2project:8apache_beam.io.gcp.bigquery._CustomBigQueryStorageSource2 
beam:display_data:labelled:v1®
table_referenceJ<TableReference
 datasetId: 'ds'
 projectId: 'proj'
 tableId: 'customers'>2table_reference:8apache_beam.io.gcp.bigquery._CustomBigQueryStorageSource2o
beam:display_data:labelled:v1N
queryNone2query:8apache_beam.io.gcp.bigquery._CustomBigQueryStorageSource2}
beam:display_data:labelled:v1\
use_legacy_sql2use_legacy_sql:8apache_beam.io.gcp.bigquery._CustomBigQueryStorageSource2á
beam:display_data:labelled:v1f
use_native_datetime 2use_native_datetime:8apache_beam.io.gcp.bigquery._CustomBigQueryStorageSource2É
beam:display_data:labelled:v1b
selected_fieldsNone2selected_fields:8apache_beam.io.gcp.bigquery._CustomBigQueryStorageSource2É
beam:display_data:labelled:v1b
row_restrictionNone2row_restriction:8apache_beam.io.gcp.bigquery._CustomBigQueryStorageSource2û
beam:display_data:labelled:v1}
)This Dataflow job launches bigquery jobs.2launchesBigQueryJobs:8apache_beam.io.gcp.bigquery._CustomBigQueryStorageSource:%ref_Environment_default_environment_1B)
python_typeapache_beam.io.iobase.Read
–
Mref_AppliedPTransform_ReadQuery-_PassThroughThenCleanup-Create-Map-decode-_63˛

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_37"&
Noneref_PCollection_PCollection_38*4ReadQuery/_PassThroughThenCleanup/Create/Map(decode)2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2r
beam:display_data:labelled:v1Q
Transform Functiondecode2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
“
)ref_AppliedPTransform_AppliedPTransform_1§
 !ref_AppliedPTransform_ReadTable_2#ref_AppliedPTransform_ReadDirect_23"ref_AppliedPTransform_ReadQuery_44ref_AppliedPTransform_Load_65 ref_AppliedPTransform_Stream_124!ref_AppliedPTransform_Dynamic_138*)ref_AppliedPTransform_AppliedPTransform_1:%ref_Environment_default_environment_1
©
3ref_AppliedPTransform_ReadQuery-MapFilesToRemove_50Ò

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_29"&
Noneref_PCollection_PCollection_30*ReadQuery/MapFilesToRemove2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2
beam:display_data:labelled:v1^
Transform Functionfile_path_to_remove2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
¸
aref_AppliedPTransform_ReadTable-_PassThroughThenCleanup-Create-FlatMap-lambda-at-core-py-4094-_19ñ

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_10"&
Noneref_PCollection_PCollection_11*JReadTable/_PassThroughThenCleanup/Create/FlatMap(<lambda at core.py:4094>)2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2t
beam:display_data:labelled:v1S
Transform Function<lambda>2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
·
Mref_AppliedPTransform_Load-BigQueryBatchFileLoads-RemoveTempTables-Delete_120è

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_86"&
Noneref_PCollection_PCollection_87*3Load/BigQueryBatchFileLoads/RemoveTempTables/Delete2ì
beam:display_data:labelled:v1r
Transform Function5apache_beam.io.gcp.bigquery_file_loads.DeleteTablesFn2fn:!apache_beam.transforms.core.ParDo:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
∫
:ref_AppliedPTransform_ReadTable-_PassThroughThenCleanup_14˚
%
#beam:transform:generic_composite:v1Mref_AppliedPTransform_ReadTable-_PassThroughThenCleanup-ParDo-PassThrough-_15Aref_AppliedPTransform_ReadTable-_PassThroughThenCleanup-Create_17Vref_AppliedPTransform_ReadTable-_PassThroughThenCleanup-ParDo-RemoveExtractedFiles-_22%
Noneref_PCollection_PCollection_7"%
Noneref_PCollection_PCollection_8*!ReadTable/_PassThroughThenCleanup:%ref_Environment_default_environment_1BP
python_typeAapache_beam.io.gcp.bigquery_read_internal._PassThroughThenCleanup
œ
href_AppliedPTransform_Load-BigQueryBatchFileLoads-TriggerLoadJobsWithoutTempTables-ParDo-TriggerLoad_122‚

beam:transform:pardo:v14
python_side_input0ref_PCollection_PCollection_46&
Noneref_PCollection_PCollection_74"-
OngoingJobsref_PCollection_PCollection_89"&
Noneref_PCollection_PCollection_88*SLoad/BigQueryBatchFileLoads/TriggerLoadJobsWithoutTempTables/ParDo(TriggerLoadJobs)2î
beam:display_data:labelled:v1s
Transform Function6apache_beam.io.gcp.bigquery_file_loads.TriggerLoadJobs2fn:!apache_beam.transforms.core.ParDo2ì
beam:display_data:labelled:v1r
create_dispositionCREATE_IF_NEEDED2create_disposition:6apache_beam.io.gcp.bigquery_file_loads.TriggerLoadJobs2è
beam:display_data:labelled:v1n
write_dispositionWRITE_TRUNCATE2write_disposition:6apache_beam.io.gcp.bigquery_file_loads.TriggerLoadJobs2â
beam:display_data:labelled:v1h
additional_bq_params{}2additional_bq_params:6apache_beam.io.gcp.bigquery_file_loads.TriggerLoadJobs2Â
beam:display_data:labelled:v1√
schemay{'fields': [{'name': 'id', 'type': 'INT64', 'mode': 'REQUIRED'}, {'name': 'tags', 'type': 'STRING', 'mode': 'REPEATED'}]}2schema:6apache_beam.io.gcp.bigquery_file_loads.TriggerLoadJobs2ú
beam:display_data:labelled:v1{
)This Dataflow job launches bigquery jobs.2launchesBigQueryJobs:6apache_beam.io.gcp.bigquery_file_loads.TriggerLoadJobs2è
beam:display_data:labelled:v1n
source_formatNEWLINE_DELIMITED_JSON2source_format:6apache_beam.io.gcp.bigquery_file_loads.TriggerLoadJobs:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
Í
>ref_AppliedPTransform_ReadQuery-Read-SDFBoundedSourceReader_54ß
%
#beam:transform:generic_composite:v1Zref_AppliedPTransform_ReadQuery-Read-SDFBoundedSourceReader-ParDo-SDFBoundedSourceDoFn-_55&
Noneref_PCollection_PCollection_32"&
Noneref_PCollection_PCollection_33*%ReadQuery/Read/SDFBoundedSourceReader2g
beam:display_data:labelled:v1F
methodEXPORT2method:,apache_beam.io.iobase.SDFBoundedSourceReader2c
beam:display_data:labelled:v1B
tableNone2table:,apache_beam.io.iobase.SDFBoundedSourceReader2w
beam:display_data:labelled:v1V
querySELECT id FROM ds.orders2query:,apache_beam.io.iobase.SDFBoundedSourceReader2g
beam:display_data:labelled:v1F
projectNone2project:,apache_beam.io.iobase.SDFBoundedSourceReader2q
beam:display_data:labelled:v1P
use_legacy_sql 2use_legacy_sql:,apache_beam.io.iobase.SDFBoundedSourceReader2}
beam:display_data:labelled:v1\
bigquery_job_labels{}2bigquery_job_labels:,apache_beam.io.iobase.SDFBoundedSourceReader2}
beam:display_data:labelled:v1\
export_file_formatAVRO2export_file_format:,apache_beam.io.iobase.SDFBoundedSourceReader2ê
beam:display_data:labelled:v1o
)This Dataflow job launches bigquery jobs.2launchesBigQueryJobs:*apache_beam.io.iobase.SDFBoundedSourceDoFn2í
beam:display_data:labelled:v1q
source1apache_beam.io.gcp.bigquery._CustomBigQuerySource2source:,apache_beam.io.iobase.SDFBoundedSourceReader:%ref_Environment_default_environment_1B;
python_type,apache_beam.io.iobase.SDFBoundedSourceReader
Ä
cref_AppliedPTransform_Load-BigQueryBatchFileLoads-ImpulseEmptyPC-FlatMap-lambda-at-core-py-4094-_69ò

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_40"&
Noneref_PCollection_PCollection_41*LLoad/BigQueryBatchFileLoads/ImpulseEmptyPC/FlatMap(<lambda at core.py:4094>)2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2t
beam:display_data:labelled:v1S
Transform Function<lambda>2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
◊
href_AppliedPTransform_Load-BigQueryBatchFileLoads-ReshuffleBeforeLoadWithoutTempTables-ReshufflePerK_103Í
%
#beam:transform:generic_composite:v1href_AppliedPTransform_Load-BigQueryBatchFileLoads-ReshuffleBeforeLoadWithoutTempTables-ReshufflePerK_104href_AppliedPTransform_Load-BigQueryBatchFileLoads-ReshuffleBeforeLoadWithoutTempTables-ReshufflePerK_105href_AppliedPTransform_Load-BigQueryBatchFileLoads-ReshuffleBeforeLoadWithoutTempTables-ReshufflePerK_106&
Noneref_PCollection_PCollection_70"&
Noneref_PCollection_PCollection_73*PLoad/BigQueryBatchFileLoads/ReshuffleBeforeLoadWithoutTempTables/ReshufflePerKey:%ref_Environment_default_environment_1B:
python_type+apache_beam.transforms.util.ReshufflePerKey
È
href_AppliedPTransform_Load-BigQueryBatchFileLoads-TriggerLoadJobsWithTempTables-ParDo-TriggerLoadJob_109¸

beam:transform:pardo:v14
python_side_input0ref_PCollection_PCollection_46&
Noneref_PCollection_PCollection_69"1
TemporaryTablesref_PCollection_PCollection_77"-
OngoingJobsref_PCollection_PCollection_76"&
Noneref_PCollection_PCollection_75*PLoad/BigQueryBatchFileLoads/TriggerLoadJobsWithTempTables/ParDo(TriggerLoadJobs)2î
beam:display_data:labelled:v1s
Transform Function6apache_beam.io.gcp.bigquery_file_loads.TriggerLoadJobs2fn:!apache_beam.transforms.core.ParDo2á
beam:display_data:labelled:v1f
create_dispositionNone2create_disposition:6apache_beam.io.gcp.bigquery_file_loads.TriggerLoadJobs2Ö
beam:display_data:labelled:v1d
write_dispositionNone2write_disposition:6apache_beam.io.gcp.bigquery_file_loads.TriggerLoadJobs2â
beam:display_data:labelled:v1h
additional_bq_params{}2additional_bq_params:6apache_beam.io.gcp.bigquery_file_loads.TriggerLoadJobs2Â
beam:display_data:labelled:v1√
schemay{'fields': [{'name': 'id', 'type': 'INT64', 'mode': 'REQUIRED'}, {'name': 'tags', 'type': 'STRING', 'mode': 'REPEATED'}]}2schema:6apache_beam.io.gcp.bigquery_file_loads.TriggerLoadJobs2ú
beam:display_data:labelled:v1{
)This Dataflow job launches bigquery jobs.2launchesBigQueryJobs:6apache_beam.io.gcp.bigquery_file_loads.TriggerLoadJobs2è
beam:display_data:labelled:v1n
source_formatNEWLINE_DELIMITED_JSON2source_format:6apache_beam.io.gcp.bigquery_file_loads.TriggerLoadJobs:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
º
?ref_AppliedPTransform_Stream-_StreamToBigQuery-AddInsertIds_127¯

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_91"&
Noneref_PCollection_PCollection_92*%Stream/_StreamToBigQuery/AddInsertIds2ä
beam:display_data:labelled:v1i
Transform Function,apache_beam.io.gcp.bigquery.InsertIdPrefixFn2fn:!apache_beam.transforms.core.ParDo:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
Û
href_AppliedPTransform_Load-BigQueryBatchFileLoads-ReshuffleBeforeLoadWithoutTempTables-ReshufflePerK_105Ü
 
beam:transform:group_by_key:v1&
Noneref_PCollection_PCollection_71"&
Noneref_PCollection_PCollection_72*[Load/BigQueryBatchFileLoads/ReshuffleBeforeLoadWithoutTempTables/ReshufflePerKey/GroupByKeyB5
python_type&apache_beam.transforms.core.GroupByKey
å
&ref_AppliedPTransform_ReadTable-Read_9·

beam:transform:read:v1/ref_AppliedPTransform_ReadTable-Read-Impulse_102ref_AppliedPTransform_ReadTable-Read-EmitSource_11>ref_AppliedPTransform_ReadTable-Read-SDFBoundedSourceReader_12"%
Noneref_PCollection_PCollection_7*ReadTable/Read2Ö
beam:display_data:labelled:v1d
Read Source1apache_beam.io.gcp.bigquery._CustomBigQuerySource2source:apache_beam.io.iobase.Read2l
beam:display_data:labelled:v1K
methodEXPORT2method:1apache_beam.io.gcp.bigquery._CustomBigQuerySource2¨
beam:display_data:labelled:v1ä
tableG<TableReference
 datasetId: 'ds'
 projectId: 'proj'
 tableId: 'orders'>2table:1apache_beam.io.gcp.bigquery._CustomBigQuerySource2h
beam:display_data:labelled:v1G
queryNone2query:1apache_beam.io.gcp.bigquery._CustomBigQuerySource2l
beam:display_data:labelled:v1K
projectNone2project:1apache_beam.io.gcp.bigquery._CustomBigQuerySource2v
beam:display_data:labelled:v1U
use_legacy_sql2use_legacy_sql:1apache_beam.io.gcp.bigquery._CustomBigQuerySource2Ç
beam:display_data:labelled:v1a
bigquery_job_labels{}2bigquery_job_labels:1apache_beam.io.gcp.bigquery._CustomBigQuerySource2Ç
beam:display_data:labelled:v1a
export_file_formatAVRO2export_file_format:1apache_beam.io.gcp.bigquery._CustomBigQuerySource2ó
beam:display_data:labelled:v1v
)This Dataflow job launches bigquery jobs.2launchesBigQueryJobs:1apache_beam.io.gcp.bigquery._CustomBigQuerySource:%ref_Environment_default_environment_1B)
python_typeapache_beam.io.iobase.Read
Ñ
Gref_AppliedPTransform_ReadDirect-_PassThroughThenCleanupTempDatasets_35∏
%
#beam:transform:generic_composite:v1Zref_AppliedPTransform_ReadDirect-_PassThroughThenCleanupTempDatasets-ParDo-PassThrough-_36Nref_AppliedPTransform_ReadDirect-_PassThroughThenCleanupTempDatasets-Create_38^ref_AppliedPTransform_ReadDirect-_PassThroughThenCleanupTempDatasets-ParDo-CleanUpProjects-_43&
Noneref_PCollection_PCollection_20"&
Noneref_PCollection_PCollection_21*.ReadDirect/_PassThroughThenCleanupTempDatasets:%ref_Environment_default_environment_1B\
python_typeMapache_beam.io.gcp.bigquery_read_internal._PassThroughThenCleanupTempDatasets
≈
/ref_AppliedPTransform_ReadQuery-Read-Impulse_52ë

beam:transform:impulse:v1"&
Noneref_PCollection_PCollection_31*ReadQuery/Read/ImpulseB2
python_type#apache_beam.transforms.core.Impulse
 
href_AppliedPTransform_Load-BigQueryBatchFileLoads-ReshuffleBeforeLoadWithoutTempTables-ReshufflePerK_106›

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_72"&
Noneref_PCollection_PCollection_73*yLoad/BigQueryBatchFileLoads/ReshuffleBeforeLoadWithoutTempTables/ReshufflePerKey/FlatMap(restore_metadata_default_window)2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2ã
beam:display_data:labelled:v1j
Transform Functionrestore_metadata_default_window2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
ú
2ref_AppliedPTransform_ReadQuery-Read-EmitSource_53Â

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_31"&
Noneref_PCollection_PCollection_32*ReadQuery/Read/EmitSource2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2t
beam:display_data:labelled:v1S
Transform Function<lambda>2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
ô
Vref_AppliedPTransform_Load-BigQueryBatchFileLoads-RemoveTempTables-AddUselessValue_116æ

beam:transform:pardo:v14
python_side_input0ref_PCollection_PCollection_83&
Noneref_PCollection_PCollection_77"&
Noneref_PCollection_PCollection_84*<Load/BigQueryBatchFileLoads/RemoveTempTables/AddUselessValue2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2t
beam:display_data:labelled:v1S
Transform Function<lambda>2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
à
!ref_AppliedPTransform_Dynamic_138‚
&
$beam:transform:write_to_big_query:v03ref_AppliedPTransform_Dynamic-_StreamToBigQuery_139%
Noneref_PCollection_PCollection_8*Dynamic2p
beam:display_data:labelled:v1O
Validation Enabled2
validation:+apache_beam.io.gcp.bigquery.WriteToBigQuery:%ref_Environment_default_environment_1B:
python_type+apache_beam.io.gcp.bigquery.WriteToBigQuery
É
eref_AppliedPTransform_Load-BigQueryBatchFileLoads-MapTuple-lambda-at-bigquery_file_loads-py-1240-_111ô

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_75"&
Noneref_PCollection_PCollection_79*MLoad/BigQueryBatchFileLoads/MapTuple(<lambda at bigquery_file_loads.py:1240>)2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2t
beam:display_data:labelled:v1S
Transform Function<lambda>2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
ñ
@ref_AppliedPTransform_Load-BigQueryBatchFileLoads-GroupByKey_112—
 
beam:transform:group_by_key:v1&
Noneref_PCollection_PCollection_79"&
Noneref_PCollection_PCollection_80*&Load/BigQueryBatchFileLoads/GroupByKeyB5
python_type&apache_beam.transforms.core.GroupByKey
‘
Gref_AppliedPTransform_Load-BigQueryBatchFileLoads-GenerateFilePrefix_80à

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_45"&
Noneref_PCollection_PCollection_49*.Load/BigQueryBatchFileLoads/GenerateFilePrefix2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2Å
beam:display_data:labelled:v1`
Transform Function_generate_file_prefix2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
„
>ref_AppliedPTransform_ReadTable-FilesToRemoveImpulse-Impulse_4†

beam:transform:impulse:v1"%
Noneref_PCollection_PCollection_1*&ReadTable/FilesToRemoveImpulse/ImpulseB2
python_type#apache_beam.transforms.core.Impulse
Ì
Cref_AppliedPTransform_ReadDirect-ProjectToCleanupImpulse-Impulse_25•

beam:transform:impulse:v1"&
Noneref_PCollection_PCollection_14**ReadDirect/ProjectToCleanupImpulse/ImpulseB2
python_type#apache_beam.transforms.core.Impulse
è
Dref_AppliedPTransform_Stream-_StreamToBigQuery-AppendDestination_126∆

beam:transform:pardo:v1%
Noneref_PCollection_PCollection_8"&
Noneref_PCollection_PCollection_91**Stream/_StreamToBigQuery/AppendDestination2î
beam:display_data:labelled:v1s
Transform Function6apache_beam.io.gcp.bigquery_tools.AppendDestinationsFn2fn:!apache_beam.transforms.core.ParDo2Ω
beam:display_data:labelled:v1õ
destinationG<TableReference
 datasetId: 'ds'
 projectId: 'proj'
 tableId: 'events'>2destination:6apache_beam.io.gcp.bigquery_tools.AppendDestinationsFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
ƒ
/ref_AppliedPTransform_ReadTable-Read-Impulse_10ê

beam:transform:impulse:v1"%
Noneref_PCollection_PCollection_5*ReadTable/Read/ImpulseB2
python_type#apache_beam.transforms.core.Impulse
ï
]ref_AppliedPTransform_ReadDirect-_PassThroughThenCleanupTempDatasets-Create-MaybeReshuffle_41≥
"
 beam:transform:pickled_python:v1&
Noneref_PCollection_PCollection_24"&
Noneref_PCollection_PCollection_24*DReadDirect/_PassThroughThenCleanupTempDatasets/Create/MaybeReshuffle:%ref_Environment_default_environment_1BP
python_typeAapache_beam.transforms.core.Create.expand.<locals>.MaybeReshuffle
œ
Eref_AppliedPTransform_Stream-_StreamToBigQuery-ToHashableTableRef_128Ö

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_92"&
Noneref_PCollection_PCollection_93*+Stream/_StreamToBigQuery/ToHashableTableRef2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2Å
beam:display_data:labelled:v1`
Transform Functionto_hashable_table_ref2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
∏
"ref_AppliedPTransform_ReadQuery_44ë
%
#beam:transform:generic_composite:v17ref_AppliedPTransform_ReadQuery-FilesToRemoveImpulse_453ref_AppliedPTransform_ReadQuery-MapFilesToRemove_50'ref_AppliedPTransform_ReadQuery-Read_51:ref_AppliedPTransform_ReadQuery-_PassThroughThenCleanup_56"&
Noneref_PCollection_PCollection_34*	ReadQuery:%ref_Environment_default_environment_1B;
python_type,apache_beam.io.gcp.bigquery.ReadFromBigQuery
«
0ref_AppliedPTransform_ReadDirect-Read-Impulse_31í

beam:transform:impulse:v1"&
Noneref_PCollection_PCollection_18*ReadDirect/Read/ImpulseB2
python_type#apache_beam.transforms.core.Impulse
á
href_AppliedPTransform_Load-BigQueryBatchFileLoads-ReshuffleBeforeLoadWithTempTables-RemoveRandomKeys_100ö

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_68"&
Noneref_PCollection_PCollection_69*NLoad/BigQueryBatchFileLoads/ReshuffleBeforeLoadWithTempTables/RemoveRandomKeys2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2t
beam:display_data:labelled:v1S
Transform Function<lambda>2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
Ü
Eref_AppliedPTransform_Dynamic-_StreamToBigQuery-AppendDestination_140º

beam:transform:pardo:v1%
Noneref_PCollection_PCollection_8"'
Noneref_PCollection_PCollection_103*+Dynamic/_StreamToBigQuery/AppendDestination2î
beam:display_data:labelled:v1s
Transform Function6apache_beam.io.gcp.bigquery_tools.AppendDestinationsFn2fn:!apache_beam.transforms.core.ParDo2±
beam:display_data:labelled:v1è
destination;<function bigquery_job.<locals>.<lambda> at 0x7f09da306200>2destination:6apache_beam.io.gcp.bigquery_tools.AppendDestinationsFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
¡
;ref_AppliedPTransform_ReadDirect-ProjectToCleanupImpulse_24Å
"
 beam:transform:pickled_python:v1Cref_AppliedPTransform_ReadDirect-ProjectToCleanupImpulse-Impulse_25[ref_AppliedPTransform_ReadDirect-ProjectToCleanupImpulse-FlatMap-lambda-at-core-py-4094-_26Jref_AppliedPTransform_ReadDirect-ProjectToCleanupImpulse-MaybeReshuffle_27Gref_AppliedPTransform_ReadDirect-ProjectToCleanupImpulse-Map-decode-_28"&
Noneref_PCollection_PCollection_16*"ReadDirect/ProjectToCleanupImpulse:%ref_Environment_default_environment_1B1
python_type"apache_beam.transforms.core.Create
Á
Fref_AppliedPTransform_ReadQuery-FilesToRemoveImpulse-MaybeReshuffle_48ú
"
 beam:transform:pickled_python:v1&
Noneref_PCollection_PCollection_28"&
Noneref_PCollection_PCollection_28*-ReadQuery/FilesToRemoveImpulse/MaybeReshuffle:%ref_Environment_default_environment_1BP
python_typeAapache_beam.transforms.core.Create.expand.<locals>.MaybeReshuffle
π
2ref_AppliedPTransform_Stream-_StreamToBigQuery_125Ç
%
#beam:transform:generic_composite:v1Dref_AppliedPTransform_Stream-_StreamToBigQuery-AppendDestination_126?ref_AppliedPTransform_Stream-_StreamToBigQuery-AddInsertIds_127Eref_AppliedPTransform_Stream-_StreamToBigQuery-ToHashableTableRef_128Dref_AppliedPTransform_Stream-_StreamToBigQuery-WithFixedSharding_129Bref_AppliedPTransform_Stream-_StreamToBigQuery-CommitInsertIds_130<ref_AppliedPTransform_Stream-_StreamToBigQuery-DropShard_134Gref_AppliedPTransform_Stream-_StreamToBigQuery-FromHashableTableRef_135Cref_AppliedPTransform_Stream-_StreamToBigQuery-StreamInsertRows_136%
Noneref_PCollection_PCollection_8"7
FailedRowsWithErrorsref_PCollection_PCollection_101"-

FailedRowsref_PCollection_PCollection_100"'
Noneref_PCollection_PCollection_102*Stream/_StreamToBigQuery:%ref_Environment_default_environment_1B<
python_type-apache_beam.io.gcp.bigquery._StreamToBigQuery
Ú
_ref_AppliedPTransform_Load-BigQueryBatchFileLoads-ParDo-PartitionFiles-ParDo-PartitionFiles-_93é

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_61"5
MULTIPLE_PARTITIONSref_PCollection_PCollection_62"2
SINGLE_PARTITIONref_PCollection_PCollection_64"&
Noneref_PCollection_PCollection_63*GLoad/BigQueryBatchFileLoads/ParDo(PartitionFiles)/ParDo(PartitionFiles)2ì
beam:display_data:labelled:v1r
Transform Function5apache_beam.io.gcp.bigquery_file_loads.PartitionFiles2fn:!apache_beam.transforms.core.ParDo:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
˙
Lref_AppliedPTransform_Load-BigQueryBatchFileLoads-ParDo-TriggerCopyJobs-_114©
%
#beam:transform:generic_composite:v1bref_AppliedPTransform_Load-BigQueryBatchFileLoads-ParDo-TriggerCopyJobs-ParDo-TriggerCopyJobs-_115&
Noneref_PCollection_PCollection_81"9
TriggerDeleteTempTablesref_PCollection_PCollection_83"&
Noneref_PCollection_PCollection_82*2Load/BigQueryBatchFileLoads/ParDo(TriggerCopyJobs):%ref_Environment_default_environment_1B6
python_type'apache_beam.transforms.core._MultiParDo
‰
Wref_AppliedPTransform_Load-BigQueryBatchFileLoads-ImpulseSingleElementPC-Map-decode-_76à

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_44"&
Noneref_PCollection_PCollection_45*>Load/BigQueryBatchFileLoads/ImpulseSingleElementPC/Map(decode)2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2r
beam:display_data:labelled:v1Q
Transform Functiondecode2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
Ò
Mref_AppliedPTransform_ReadQuery-_PassThroughThenCleanup-ParDo-PassThrough-_57ü
%
#beam:transform:generic_composite:v1_ref_AppliedPTransform_ReadQuery-_PassThroughThenCleanup-ParDo-PassThrough-ParDo-PassThrough-_58&
Noneref_PCollection_PCollection_33"0
cleanup_signalref_PCollection_PCollection_35"&
Noneref_PCollection_PCollection_34*4ReadQuery/_PassThroughThenCleanup/ParDo(PassThrough):%ref_Environment_default_environment_1B6
python_type'apache_beam.transforms.core._MultiParDo
∆
Gref_AppliedPTransform_Load-BigQueryBatchFileLoads-IdentityWorkaround_90˙

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_59"&
Noneref_PCollection_PCollection_60*.Load/BigQueryBatchFileLoads/IdentityWorkaround2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2t
beam:display_data:labelled:v1S
Transform Function<lambda>2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
Î
Bref_AppliedPTransform_Stream-_StreamToBigQuery-CommitInsertIds_130§
%
#beam:transform:generic_composite:v1eref_AppliedPTransform_Stream-_StreamToBigQuery-CommitInsertIds-Map-reify_metadata_default_window-_131Mref_AppliedPTransform_Stream-_StreamToBigQuery-CommitInsertIds-GroupByKey_132href_AppliedPTransform_Stream-_StreamToBigQuery-CommitInsertIds-FlatMap-restore_metadata_default_wind_133&
Noneref_PCollection_PCollection_94"&
Noneref_PCollection_PCollection_97*(Stream/_StreamToBigQuery/CommitInsertIds:%ref_Environment_default_environment_1B:
python_type+apache_beam.transforms.util.ReshufflePerKey
–
#ref_AppliedPTransform_ReadDirect_23®
%
#beam:transform:generic_composite:v1;ref_AppliedPTransform_ReadDirect-ProjectToCleanupImpulse_247ref_AppliedPTransform_ReadDirect-MapProjectToCleanup_29(ref_AppliedPTransform_ReadDirect-Read_30Gref_AppliedPTransform_ReadDirect-_PassThroughThenCleanupTempDatasets_35"&
Noneref_PCollection_PCollection_21*
ReadDirect:%ref_Environment_default_environment_1B;
python_type,apache_beam.io.gcp.bigquery.ReadFromBigQuery
á
href_AppliedPTransform_Load-BigQueryBatchFileLoads-ReshuffleBeforeLoadWithoutTempTables-AddRandomKeys_102ö

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_64"&
Noneref_PCollection_PCollection_70*NLoad/BigQueryBatchFileLoads/ReshuffleBeforeLoadWithoutTempTables/AddRandomKeys2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2t
beam:display_data:labelled:v1S
Transform Function<lambda>2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
Ë
Wref_AppliedPTransform_ReadQuery-FilesToRemoveImpulse-FlatMap-lambda-at-core-py-4094-_47å

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_27"&
Noneref_PCollection_PCollection_28*@ReadQuery/FilesToRemoveImpulse/FlatMap(<lambda at core.py:4094>)2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2t
beam:display_data:labelled:v1S
Transform Function<lambda>2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
Ã
Eref_AppliedPTransform_Dynamic-_StreamToBigQuery-WithFixedSharding_143Ç

beam:transform:pardo:v1'
Noneref_PCollection_PCollection_105"'
Noneref_PCollection_PCollection_106*+Dynamic/_StreamToBigQuery/WithFixedSharding2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2}
beam:display_data:labelled:v1\
Transform Function_add_random_shard2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
˝
Kref_AppliedPTransform_Load-BigQueryBatchFileLoads-ImpulseEmptyPC-Impulse_68≠

beam:transform:impulse:v1"&
Noneref_PCollection_PCollection_40*2Load/BigQueryBatchFileLoads/ImpulseEmptyPC/ImpulseB2
python_type#apache_beam.transforms.core.Impulse
‹
Tref_AppliedPTransform_Load-BigQueryBatchFileLoads-ParDo-UpdateDestinationSchema-_110É

beam:transform:pardo:v14
python_side_input0ref_PCollection_PCollection_47&
Noneref_PCollection_PCollection_75"&
Noneref_PCollection_PCollection_78*:Load/BigQueryBatchFileLoads/ParDo(UpdateDestinationSchema)2ú
beam:display_data:labelled:v1{
Transform Function>apache_beam.io.gcp.bigquery_file_loads.UpdateDestinationSchema2fn:!apache_beam.transforms.core.ParDo2ó
beam:display_data:labelled:v1v
write_dispositionWRITE_TRUNCATE2write_disposition:>apache_beam.io.gcp.bigquery_file_loads.UpdateDestinationSchema2ë
beam:display_data:labelled:v1p
additional_bq_params{}2additional_bq_params:>apache_beam.io.gcp.bigquery_file_loads.UpdateDestinationSchema:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
Œ
Kref_AppliedPTransform_Load-BigQueryBatchFileLoads-SchemaModJobNamePrefix_78˛

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_45"&
Noneref_PCollection_PCollection_47*2Load/BigQueryBatchFileLoads/SchemaModJobNamePrefix2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2t
beam:display_data:labelled:v1S
Transform Function<lambda>2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
˘
Iref_AppliedPTransform_ReadQuery-_PassThroughThenCleanup-Create-Impulse_60´

beam:transform:impulse:v1"&
Noneref_PCollection_PCollection_36*0ReadQuery/_PassThroughThenCleanup/Create/ImpulseB2
python_type#apache_beam.transforms.core.Impulse
Ô
Jref_AppliedPTransform_ReadDirect-ProjectToCleanupImpulse-MaybeReshuffle_27†
"
 beam:transform:pickled_python:v1&
Noneref_PCollection_PCollection_15"&
Noneref_PCollection_PCollection_15*1ReadDirect/ProjectToCleanupImpulse/MaybeReshuffle:%ref_Environment_default_environment_1BP
python_typeAapache_beam.transforms.core.Create.expand.<locals>.MaybeReshuffle
»
Dref_AppliedPTransform_Stream-_StreamToBigQuery-WithFixedSharding_129ˇ

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_93"&
Noneref_PCollection_PCollection_94**Stream/_StreamToBigQuery/WithFixedSharding2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2}
beam:display_data:labelled:v1\
Transform Function_add_random_shard2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
ó
eref_AppliedPTransform_Stream-_StreamToBigQuery-CommitInsertIds-Map-reify_metadata_default_window-_131≠

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_94"&
Noneref_PCollection_PCollection_95*KStream/_StreamToBigQuery/CommitInsertIds/Map(reify_metadata_default_window)2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2â
beam:display_data:labelled:v1h
Transform Functionreify_metadata_default_window2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
È
Yref_AppliedPTransform_Load-BigQueryBatchFileLoads-RemoveTempTables-GetTableNames-Keys_119ã

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_85"&
Noneref_PCollection_PCollection_86*?Load/BigQueryBatchFileLoads/RemoveTempTables/GetTableNames/Keys2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2t
beam:display_data:labelled:v1S
Transform Function<lambda>2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
Œ
gref_AppliedPTransform_ReadDirect-_PassThroughThenCleanupTempDatasets-ParDo-PassThrough-ParDo-PassThr_37‚

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_20"0
cleanup_signalref_PCollection_PCollection_22"&
Noneref_PCollection_PCollection_21*TReadDirect/_PassThroughThenCleanupTempDatasets/ParDo(PassThrough)/ParDo(PassThrough)2ì
beam:display_data:labelled:v1r
Transform Function5apache_beam.io.gcp.bigquery_read_internal.PassThrough2fn:!apache_beam.transforms.core.ParDo:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
•
Nref_AppliedPTransform_Load-BigQueryBatchFileLoads-WriteGroupedRecordsToFile_88“

beam:transform:pardo:v14
python_side_input0ref_PCollection_PCollection_49&
Noneref_PCollection_PCollection_57"&
Noneref_PCollection_PCollection_58*5Load/BigQueryBatchFileLoads/WriteGroupedRecordsToFile2û
beam:display_data:labelled:v1}
Transform Function@apache_beam.io.gcp.bigquery_file_loads.WriteGroupedRecordsToFile2fn:!apache_beam.transforms.core.ParDo:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
¸
Jref_AppliedPTransform_Load-BigQueryBatchFileLoads-ParDo-PartitionFiles-_92≠
%
#beam:transform:generic_composite:v1_ref_AppliedPTransform_Load-BigQueryBatchFileLoads-ParDo-PartitionFiles-ParDo-PartitionFiles-_93&
Noneref_PCollection_PCollection_61"5
MULTIPLE_PARTITIONSref_PCollection_PCollection_62"2
SINGLE_PARTITIONref_PCollection_PCollection_64*1Load/BigQueryBatchFileLoads/ParDo(PartitionFiles):%ref_Environment_default_environment_1B6
python_type'apache_beam.transforms.core._MultiParDo
ƒ
Gref_AppliedPTransform_ReadDirect-ProjectToCleanupImpulse-Map-decode-_28¯

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_15"&
Noneref_PCollection_PCollection_16*.ReadDirect/ProjectToCleanupImpulse/Map(decode)2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2r
beam:display_data:labelled:v1Q
Transform Functiondecode2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
”
bref_AppliedPTransform_Load-BigQueryBatchFileLoads-ParDo-TriggerCopyJobs-ParDo-TriggerCopyJobs-_115Ï

beam:transform:pardo:v14
python_side_input0ref_PCollection_PCollection_484
python_side_input1ref_PCollection_PCollection_78&
Noneref_PCollection_PCollection_81"9
TriggerDeleteTempTablesref_PCollection_PCollection_83"&
Noneref_PCollection_PCollection_82*ILoad/BigQueryBatchFileLoads/ParDo(TriggerCopyJobs)/ParDo(TriggerCopyJobs)2î
beam:display_data:labelled:v1s
Transform Function6apache_beam.io.gcp.bigquery_file_loads.TriggerCopyJobs2fn:!apache_beam.transforms.core.ParDo2ú
beam:display_data:labelled:v1{
)This Dataflow job launches bigquery jobs.2launchesBigQueryJobs:6apache_beam.io.gcp.bigquery_file_loads.TriggerCopyJobs:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
¥
7ref_AppliedPTransform_ReadDirect-MapProjectToCleanup_29¯

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_16"&
Noneref_PCollection_PCollection_17*ReadDirect/MapProjectToCleanup2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2Å
beam:display_data:labelled:v1`
Transform Function_get_pipeline_details2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
ì
Zref_AppliedPTransform_ReadDirect-_PassThroughThenCleanupTempDatasets-ParDo-PassThrough-_36¥
%
#beam:transform:generic_composite:v1gref_AppliedPTransform_ReadDirect-_PassThroughThenCleanupTempDatasets-ParDo-PassThrough-ParDo-PassThr_37&
Noneref_PCollection_PCollection_20"0
cleanup_signalref_PCollection_PCollection_22"&
Noneref_PCollection_PCollection_21*AReadDirect/_PassThroughThenCleanupTempDatasets/ParDo(PassThrough):%ref_Environment_default_environment_1B6
python_type'apache_beam.transforms.core._MultiParDo
œ
Gref_AppliedPTransform_Stream-_StreamToBigQuery-FromHashableTableRef_135É

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_98"&
Noneref_PCollection_PCollection_99*-Stream/_StreamToBigQuery/FromHashableTableRef2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2~
beam:display_data:labelled:v1]
Transform Function_restore_table_ref2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
Ò
Cref_AppliedPTransform_Dynamic-_StreamToBigQuery-CommitInsertIds_144©
%
#beam:transform:generic_composite:v1fref_AppliedPTransform_Dynamic-_StreamToBigQuery-CommitInsertIds-Map-reify_metadata_default_window-_145Nref_AppliedPTransform_Dynamic-_StreamToBigQuery-CommitInsertIds-GroupByKey_146href_AppliedPTransform_Dynamic-_StreamToBigQuery-CommitInsertIds-FlatMap-restore_metadata_default_win_147'
Noneref_PCollection_PCollection_106"'
Noneref_PCollection_PCollection_109*)Dynamic/_StreamToBigQuery/CommitInsertIds:%ref_Environment_default_environment_1B:
python_type+apache_beam.transforms.util.ReshufflePerKey
Â
Aref_AppliedPTransform_ReadTable-_PassThroughThenCleanup-Create_17ü
"
 beam:transform:pickled_python:v1Iref_AppliedPTransform_ReadTable-_PassThroughThenCleanup-Create-Impulse_18aref_AppliedPTransform_ReadTable-_PassThroughThenCleanup-Create-FlatMap-lambda-at-core-py-4094-_19Pref_AppliedPTransform_ReadTable-_PassThroughThenCleanup-Create-MaybeReshuffle_20Mref_AppliedPTransform_ReadTable-_PassThroughThenCleanup-Create-Map-decode-_21"&
Noneref_PCollection_PCollection_12*(ReadTable/_PassThroughThenCleanup/Create:%ref_Environment_default_environment_1B1
python_type"apache_beam.transforms.core.Createó
ref_PCollection_PCollection_50u
546Load/BigQueryBatchFileLoads/RewindowIntoGlobal.Noneref_Coder_FastPrimitivesCoder_3"ref_Windowing_Windowing_1ü
ref_PCollection_PCollection_85}
E62Load/BigQueryBatchFileLoads/RemoveTempTables/DeduplicateTables.Noneref_Coder_TupleCoder_17"ref_Windowing_Windowing_1≠
ref_PCollection_PCollection_41ä
S76Load/BigQueryBatchFileLoads/ImpulseEmptyPC/FlatMap(<lambda at core.py:4094>).Noneref_Coder_BytesCoder_1"ref_Windowing_Windowing_1ç
ref_PCollection_PCollection_106j
243Dynamic/_StreamToBigQuery/WithFixedSharding.Noneref_Coder_TupleCoder_31"ref_Windowing_Windowing_1±
ref_PCollection_PCollection_63é
N71Load/BigQueryBatchFileLoads/ParDo(PartitionFiles)/ParDo(PartitionFiles).Noneref_Coder_FastPrimitivesCoder_3"ref_Windowing_Windowing_1ó
ref_PCollection_PCollection_16u
546ReadDirect/ProjectToCleanupImpulse/Map(decode).Noneref_Coder_FastPrimitivesCoder_3"ref_Windowing_Windowing_1»
ref_PCollection_PCollection_22•
e84ReadDirect/_PassThroughThenCleanupTempDatasets/ParDo(PassThrough)/ParDo(PassThrough).cleanup_signalref_Coder_FastPrimitivesCoder_3"ref_Windowing_Windowing_1è
ref_PCollection_PCollection_60m
546Load/BigQueryBatchFileLoads/IdentityWorkaround.Noneref_Coder_TupleCoder_15"ref_Windowing_Windowing_1u
ref_PCollection_PCollection_5T
22ReadTable/Read/Impulse.Noneref_Coder_BytesCoder_1"ref_Windowing_Windowing_1ª
ref_PCollection_PCollection_101ó
W64Stream/_StreamToBigQuery/StreamInsertRows/ParDo(BigQueryWriteFn).FailedRowsWithErrorsref_Coder_FastPrimitivesCoder_3"ref_Windowing_Windowing_1†
ref_PCollection_PCollection_28~
G64ReadQuery/FilesToRemoveImpulse/FlatMap(<lambda at core.py:4094>).Noneref_Coder_BytesCoder_1"ref_Windowing_Windowing_1≠
ref_PCollection_PCollection_95ä
R75Stream/_StreamToBigQuery/CommitInsertIds/Map(reify_metadata_default_window).Noneref_Coder_TupleCoder_33"ref_Windowing_Windowing_2Ü
ref_PCollection_PCollection_27d
-38ReadQuery/FilesToRemoveImpulse/Impulse.Noneref_Coder_BytesCoder_1"ref_Windowing_Windowing_1ç
ref_PCollection_PCollection_56k
344Load/BigQueryBatchFileLoads/GroupShardedRows.Noneref_Coder_TupleCoder_11"ref_Windowing_Windowing_1ö
ref_PCollection_PCollection_59x
849Load/BigQueryBatchFileLoads/DestinationFilesUnion.Noneref_Coder_FastPrimitivesCoder_3"ref_Windowing_Windowing_1ä
ref_PCollection_PCollection_91h
142Stream/_StreamToBigQuery/AppendDestination.Noneref_Coder_TupleCoder_7"ref_Windowing_Windowing_1õ
ref_PCollection_PCollection_47y
950Load/BigQueryBatchFileLoads/SchemaModJobNamePrefix.Noneref_Coder_FastPrimitivesCoder_3"ref_Windowing_Windowing_1É
ref_PCollection_PCollection_98a
)34Stream/_StreamToBigQuery/DropShard.Noneref_Coder_TupleCoder_30"ref_Windowing_Windowing_1µ
ref_PCollection_PCollection_44í
[84Load/BigQueryBatchFileLoads/ImpulseSingleElementPC/FlatMap(<lambda at core.py:4094>).Noneref_Coder_BytesCoder_1"ref_Windowing_Windowing_1≥
ref_PCollection_PCollection_97ê
X81Stream/_StreamToBigQuery/CommitInsertIds/FlatMap(restore_metadata_default_window).Noneref_Coder_TupleCoder_31"ref_Windowing_Windowing_1Ø
ref_PCollection_PCollection_107ã
S76Dynamic/_StreamToBigQuery/CommitInsertIds/Map(reify_metadata_default_window).Noneref_Coder_TupleCoder_33"ref_Windowing_Windowing_2ç
ref_PCollection_PCollection_51k
445Load/BigQueryBatchFileLoads/AppendDestination.Noneref_Coder_TupleCoder_7"ref_Windowing_Windowing_1µ
ref_PCollection_PCollection_109ë
Y82Dynamic/_StreamToBigQuery/CommitInsertIds/FlatMap(restore_metadata_default_window).Noneref_Coder_TupleCoder_31"ref_Windowing_Windowing_1®
ref_PCollection_PCollection_45Ö
E62Load/BigQueryBatchFileLoads/ImpulseSingleElementPC/Map(decode).Noneref_Coder_FastPrimitivesCoder_3"ref_Windowing_Windowing_1ù
ref_PCollection_PCollection_38{
;52ReadQuery/_PassThroughThenCleanup/Create/Map(decode).Noneref_Coder_FastPrimitivesCoder_3"ref_Windowing_Windowing_1á
ref_PCollection_PCollection_17e
%30ReadDirect/MapProjectToCleanup.Noneref_Coder_FastPrimitivesCoder_3"ref_Windowing_Windowing_1ê
ref_PCollection_PCollection_10n
748ReadTable/_PassThroughThenCleanup/Create/Impulse.Noneref_Coder_BytesCoder_1"ref_Windowing_Windowing_1›
ref_PCollection_PCollection_73∫
Å121Load/BigQueryBatchFileLoads/ReshuffleBeforeLoadWithoutTempTables/ReshufflePerKey/FlatMap(restore_metadata_default_window).Noneref_Coder_TupleCoder_18"ref_Windowing_Windowing_1ù
ref_PCollection_PCollection_84{
C60Load/BigQueryBatchFileLoads/RemoveTempTables/AddUselessValue.Noneref_Coder_TupleCoder_15"ref_Windowing_Windowing_1∞
ref_PCollection_PCollection_70ç
U78Load/BigQueryBatchFileLoads/ReshuffleBeforeLoadWithoutTempTables/AddRandomKeys.Noneref_Coder_TupleCoder_18"ref_Windowing_Windowing_1ó
ref_PCollection_PCollection_49u
546Load/BigQueryBatchFileLoads/GenerateFilePrefix.Noneref_Coder_FastPrimitivesCoder_3"ref_Windowing_Windowing_1∏
ref_PCollection_PCollection_24ï
^87ReadDirect/_PassThroughThenCleanupTempDatasets/Create/FlatMap(<lambda at core.py:4094>).Noneref_Coder_BytesCoder_1"ref_Windowing_Windowing_1π
ref_PCollection_PCollection_52ñ
V79Load/BigQueryBatchFileLoads/ParDo(WriteRecordsToFile)/ParDo(WriteRecordsToFile).Noneref_Coder_FastPrimitivesCoder_3"ref_Windowing_Windowing_1≠
ref_PCollection_PCollection_65ä
R75Load/BigQueryBatchFileLoads/ReshuffleBeforeLoadWithTempTables/AddRandomKeys.Noneref_Coder_TupleCoder_18"ref_Windowing_Windowing_1ù
ref_PCollection_PCollection_23{
D61ReadDirect/_PassThroughThenCleanupTempDatasets/Create/Impulse.Noneref_Coder_BytesCoder_1"ref_Windowing_Windowing_1å
ref_PCollection_PCollection_90j
*35Load/BigQueryBatchFileLoads/Flatten.Noneref_Coder_FastPrimitivesCoder_3"ref_Windowing_Windowing_1ß
ref_PCollection_PCollection_13Ñ
D61ReadTable/_PassThroughThenCleanup/ParDo(RemoveExtractedFiles).Noneref_Coder_FastPrimitivesCoder_3"ref_Windowing_Windowing_1ï
ref_PCollection_PCollection_55s
<53Load/BigQueryBatchFileLoads/ParDo(_ShardDestinations).Noneref_Coder_TupleCoder_8"ref_Windowing_Windowing_1ì
ref_PCollection_PCollection_29q
142ReadQuery/FilesToRemoveImpulse/Map(decode).Noneref_Coder_FastPrimitivesCoder_3"ref_Windowing_Windowing_1Ø
ref_PCollection_PCollection_79å
T77Load/BigQueryBatchFileLoads/MapTuple(<lambda at bigquery_file_loads.py:1240>).Noneref_Coder_TupleCoder_15"ref_Windowing_Windowing_1∫
ref_PCollection_PCollection_75ó
W80Load/BigQueryBatchFileLoads/TriggerLoadJobsWithTempTables/ParDo(TriggerLoadJobs).Noneref_Coder_FastPrimitivesCoder_3"ref_Windowing_Windowing_1w
ref_PCollection_PCollection_18U
23ReadDirect/Read/Impulse.Noneref_Coder_BytesCoder_1"ref_Windowing_Windowing_1Ñ
ref_PCollection_PCollection_32b
 25ReadQuery/Read/EmitSource.None!ref_Coder__MemoizingPickleCoder_4"ref_Windowing_Windowing_1î
ref_PCollection_PCollection_96r
:51Stream/_StreamToBigQuery/CommitInsertIds/GroupByKey.Noneref_Coder_TupleCoder_37"ref_Windowing_Windowing_2ñ
ref_PCollection_PCollection_46t
445Load/BigQueryBatchFileLoads/LoadJobNamePrefix.Noneref_Coder_FastPrimitivesCoder_3"ref_Windowing_Windowing_1í
ref_PCollection_PCollection_3q
142ReadTable/FilesToRemoveImpulse/Map(decode).Noneref_Coder_FastPrimitivesCoder_3"ref_Windowing_Windowing_1≥
ref_PCollection_PCollection_82ê
P73Load/BigQueryBatchFileLoads/ParDo(TriggerCopyJobs)/ParDo(TriggerCopyJobs).Noneref_Coder_FastPrimitivesCoder_3"ref_Windowing_Windowing_1ã
ref_PCollection_PCollection_94i
142Stream/_StreamToBigQuery/WithFixedSharding.Noneref_Coder_TupleCoder_31"ref_Windowing_Windowing_1ê
ref_PCollection_PCollection_111m
546Dynamic/_StreamToBigQuery/FromHashableTableRef.Noneref_Coder_TupleCoder_27"ref_Windowing_Windowing_1Ö
ref_PCollection_PCollection_1d
-38ReadTable/FilesToRemoveImpulse/Impulse.Noneref_Coder_BytesCoder_1"ref_Windowing_Windowing_1≤
ref_PCollection_PCollection_112é
N65Dynamic/_StreamToBigQuery/StreamInsertRows/ParDo(BigQueryWriteFn).FailedRowsref_Coder_FastPrimitivesCoder_3"ref_Windowing_Windowing_1´
ref_PCollection_PCollection_37à
Q74ReadQuery/_PassThroughThenCleanup/Create/FlatMap(<lambda at core.py:4094>).Noneref_Coder_BytesCoder_1"ref_Windowing_Windowing_1ö
ref_PCollection_PCollection_61x
@57Load/BigQueryBatchFileLoads/GroupFilesByTableDestinations.Noneref_Coder_TupleCoder_17"ref_Windowing_Windowing_1æ
ref_PCollection_PCollection_21õ
[84ReadDirect/_PassThroughThenCleanupTempDatasets/ParDo(PassThrough)/ParDo(PassThrough).Noneref_Coder_FastPrimitivesCoder_3"ref_Windowing_Windowing_1å
ref_PCollection_PCollection_93j
243Stream/_StreamToBigQuery/ToHashableTableRef.Noneref_Coder_TupleCoder_30"ref_Windowing_Windowing_1é
ref_PCollection_PCollection_105k
344Dynamic/_StreamToBigQuery/ToHashableTableRef.Noneref_Coder_TupleCoder_30"ref_Windowing_Windowing_1∏
ref_PCollection_PCollection_69ï
U78Load/BigQueryBatchFileLoads/ReshuffleBeforeLoadWithTempTables/RemoveRandomKeys.Noneref_Coder_FastPrimitivesCoder_3"ref_Windowing_Windowing_1ü
ref_PCollection_PCollection_2~
G64ReadTable/FilesToRemoveImpulse/FlatMap(<lambda at core.py:4094>).Noneref_Coder_BytesCoder_1"ref_Windowing_Windowing_1ñ
ref_PCollection_PCollection_58t
<53Load/BigQueryBatchFileLoads/WriteGroupedRecordsToFile.Noneref_Coder_TupleCoder_14"ref_Windowing_Windowing_1ä
ref_PCollection_PCollection_14h
142ReadDirect/ProjectToCleanupImpulse/Impulse.Noneref_Coder_BytesCoder_1"ref_Windowing_Windowing_1Ω
ref_PCollection_PCollection_88ö
Z83Load/BigQueryBatchFileLoads/TriggerLoadJobsWithoutTempTables/ParDo(TriggerLoadJobs).Noneref_Coder_FastPrimitivesCoder_3"ref_Windowing_Windowing_1¡
ref_PCollection_PCollection_54û
^79Load/BigQueryBatchFileLoads/ParDo(WriteRecordsToFile)/ParDo(WriteRecordsToFile).WrittenFilesref_Coder_FastPrimitivesCoder_3"ref_Windowing_Windowing_1Æ
ref_PCollection_PCollection_81ã
T77Load/BigQueryBatchFileLoads/MapTuple(<lambda at bigquery_file_loads.py:1244>).Noneref_Coder_ListCoder_26"ref_Windowing_Windowing_1´
ref_PCollection_PCollection_25à
H65ReadDirect/_PassThroughThenCleanupTempDatasets/Create/Map(decode).Noneref_Coder_FastPrimitivesCoder_3"ref_Windowing_Windowing_1í
ref_PCollection_PCollection_40p
950Load/BigQueryBatchFileLoads/ImpulseEmptyPC/Impulse.Noneref_Coder_BytesCoder_1"ref_Windowing_Windowing_1v
ref_PCollection_PCollection_31T
22ReadQuery/Read/Impulse.Noneref_Coder_BytesCoder_1"ref_Windowing_Windowing_1±
ref_PCollection_PCollection_34é
N71ReadQuery/_PassThroughThenCleanup/ParDo(PassThrough)/ParDo(PassThrough).Noneref_Coder_FastPrimitivesCoder_3"ref_Windowing_Windowing_1´
ref_PCollection_PCollection_102á
G64Stream/_StreamToBigQuery/StreamInsertRows/ParDo(BigQueryWriteFn).Noneref_Coder_FastPrimitivesCoder_3"ref_Windowing_Windowing_1•
ref_PCollection_PCollection_15Ç
K68ReadDirect/ProjectToCleanupImpulse/FlatMap(<lambda at core.py:4094>).Noneref_Coder_BytesCoder_1"ref_Windowing_Windowing_1Ø
ref_PCollection_PCollection_26å
L69ReadDirect/_PassThroughThenCleanupTempDatasets/ParDo(CleanUpProjects).Noneref_Coder_FastPrimitivesCoder_3"ref_Windowing_Windowing_1É
ref_PCollection_PCollection_6b
 25ReadTable/Read/EmitSource.None!ref_Coder__MemoizingPickleCoder_4"ref_Windowing_Windowing_1à
ref_PCollection_PCollection_104e
-38Dynamic/_StreamToBigQuery/AddInsertIds.Noneref_Coder_TupleCoder_30"ref_Windowing_Windowing_1ƒ
ref_PCollection_PCollection_89°
a83Load/BigQueryBatchFileLoads/TriggerLoadJobsWithoutTempTables/ParDo(TriggerLoadJobs).OngoingJobsref_Coder_FastPrimitivesCoder_3"ref_Windowing_Windowing_1ñ
ref_PCollection_PCollection_48t
445Load/BigQueryBatchFileLoads/CopyJobNamePrefix.Noneref_Coder_FastPrimitivesCoder_3"ref_Windowing_Windowing_1§
ref_PCollection_PCollection_78Å
A58Load/BigQueryBatchFileLoads/ParDo(UpdateDestinationSchema).Noneref_Coder_FastPrimitivesCoder_3"ref_Windowing_Windowing_1ñ
ref_PCollection_PCollection_108s
;52Dynamic/_StreamToBigQuery/CommitInsertIds/GroupByKey.Noneref_Coder_TupleCoder_37"ref_Windowing_Windowing_2Ÿ
ref_PCollection_PCollection_68∂
~118Load/BigQueryBatchFileLoads/ReshuffleBeforeLoadWithTempTables/ReshufflePerKey/FlatMap(restore_metadata_default_window).Noneref_Coder_TupleCoder_18"ref_Windowing_Windowing_1∫
ref_PCollection_PCollection_67ó
_88Load/BigQueryBatchFileLoads/ReshuffleBeforeLoadWithTempTables/ReshufflePerKey/GroupByKey.Noneref_Coder_TupleCoder_24"ref_Windowing_Windowing_2¨
ref_PCollection_PCollection_114à
H65Dynamic/_StreamToBigQuery/StreamInsertRows/ParDo(BigQueryWriteFn).Noneref_Coder_FastPrimitivesCoder_3"ref_Windowing_Windowing_1ö
ref_PCollection_PCollection_43x
A58Load/BigQueryBatchFileLoads/ImpulseSingleElementPC/Impulse.Noneref_Coder_BytesCoder_1"ref_Windowing_Windowing_1∫
ref_PCollection_PCollection_9ò
X71ReadTable/_PassThroughThenCleanup/ParDo(PassThrough)/ParDo(PassThrough).cleanup_signalref_Coder_FastPrimitivesCoder_3"ref_Windowing_Windowing_1å
ref_PCollection_PCollection_57j
243Load/BigQueryBatchFileLoads/DropShardNumber.Noneref_Coder_TupleCoder_13"ref_Windowing_Windowing_1™
ref_PCollection_PCollection_7à
H65ReadTable/Read/SDFBoundedSourceReader/ParDo(SDFBoundedSourceDoFn).Noneref_Coder_FastPrimitivesCoder_3"ref_Windowing_Windowing_1´
ref_PCollection_PCollection_33à
H65ReadQuery/Read/SDFBoundedSourceReader/ParDo(SDFBoundedSourceDoFn).Noneref_Coder_FastPrimitivesCoder_3"ref_Windowing_Windowing_1é
ref_PCollection_PCollection_99l
445Stream/_StreamToBigQuery/FromHashableTableRef.Noneref_Coder_TupleCoder_27"ref_Windowing_Windowing_1´
ref_PCollection_PCollection_11à
Q74ReadTable/_PassThroughThenCleanup/Create/FlatMap(<lambda at core.py:4094>).Noneref_Coder_BytesCoder_1"ref_Windowing_Windowing_1¨
ref_PCollection_PCollection_20â
I66ReadDirect/Read/SDFBoundedSourceReader/ParDo(SDFBoundedSourceDoFn).Noneref_Coder_FastPrimitivesCoder_3"ref_Windowing_Windowing_1¿
ref_PCollection_PCollection_62ù
]71Load/BigQueryBatchFileLoads/ParDo(PartitionFiles)/ParDo(PartitionFiles).MULTIPLE_PARTITIONSref_Coder_FastPrimitivesCoder_3"ref_Windowing_Windowing_1ß
ref_PCollection_PCollection_39Ñ
D61ReadQuery/_PassThroughThenCleanup/ParDo(RemoveExtractedFiles).Noneref_Coder_FastPrimitivesCoder_3"ref_Windowing_Windowing_1ù
ref_PCollection_PCollection_12{
;52ReadTable/_PassThroughThenCleanup/Create/Map(decode).Noneref_Coder_FastPrimitivesCoder_3"ref_Windowing_Windowing_1∞
ref_PCollection_PCollection_8é
N71ReadTable/_PassThroughThenCleanup/ParDo(PassThrough)/ParDo(PassThrough).Noneref_Coder_FastPrimitivesCoder_3"ref_Windowing_Windowing_1≈
ref_PCollection_PCollection_77¢
b80Load/BigQueryBatchFileLoads/TriggerLoadJobsWithTempTables/ParDo(TriggerLoadJobs).TemporaryTablesref_Coder_FastPrimitivesCoder_3"ref_Windowing_Windowing_1∆
ref_PCollection_PCollection_83£
c73Load/BigQueryBatchFileLoads/ParDo(TriggerCopyJobs)/ParDo(TriggerCopyJobs).TriggerDeleteTempTablesref_Coder_FastPrimitivesCoder_3"ref_Windowing_Windowing_1Ω
ref_PCollection_PCollection_72ö
b91Load/BigQueryBatchFileLoads/ReshuffleBeforeLoadWithoutTempTables/ReshufflePerKey/GroupByKey.Noneref_Coder_TupleCoder_24"ref_Windowing_Windowing_2©
ref_PCollection_PCollection_86Ü
F63Load/BigQueryBatchFileLoads/RemoveTempTables/GetTableNames/Keys.Noneref_Coder_FastPrimitivesCoder_3"ref_Windowing_Windowing_1≈
ref_PCollection_PCollection_53¢
b79Load/BigQueryBatchFileLoads/ParDo(WriteRecordsToFile)/ParDo(WriteRecordsToFile).UnwrittenRecordsref_Coder_FastPrimitivesCoder_3"ref_Windowing_Windowing_1ª
ref_PCollection_PCollection_74ò
X81Load/BigQueryBatchFileLoads/ReshuffleBeforeLoadWithoutTempTables/RemoveRandomKeys.Noneref_Coder_FastPrimitivesCoder_3"ref_Windowing_Windowing_1ü
ref_PCollection_PCollection_42}
=54Load/BigQueryBatchFileLoads/ImpulseEmptyPC/Map(decode).Noneref_Coder_FastPrimitivesCoder_3"ref_Windowing_Windowing_1ú
ref_PCollection_PCollection_87z
:51Load/BigQueryBatchFileLoads/RemoveTempTables/Delete.Noneref_Coder_FastPrimitivesCoder_3"ref_Windowing_Windowing_1±
ref_PCollection_PCollection_100ç
M64Stream/_StreamToBigQuery/StreamInsertRows/ParDo(BigQueryWriteFn).FailedRowsref_Coder_FastPrimitivesCoder_3"ref_Windowing_Windowing_1Ü
ref_PCollection_PCollection_92d
,37Stream/_StreamToBigQuery/AddInsertIds.Noneref_Coder_TupleCoder_27"ref_Windowing_Windowing_1º
ref_PCollection_PCollection_113ò
X65Dynamic/_StreamToBigQuery/StreamInsertRows/ParDo(BigQueryWriteFn).FailedRowsWithErrorsref_Coder_FastPrimitivesCoder_3"ref_Windowing_Windowing_1÷
ref_PCollection_PCollection_71≥
{115Load/BigQueryBatchFileLoads/ReshuffleBeforeLoadWithoutTempTables/ReshufflePerKey/Map(reify_metadata_default_window).Noneref_Coder_TupleCoder_20"ref_Windowing_Windowing_2¡
ref_PCollection_PCollection_76û
^80Load/BigQueryBatchFileLoads/TriggerLoadJobsWithTempTables/ParDo(TriggerLoadJobs).OngoingJobsref_Coder_FastPrimitivesCoder_3"ref_Windowing_Windowing_1ª
ref_PCollection_PCollection_35ò
X71ReadQuery/_PassThroughThenCleanup/ParDo(PassThrough)/ParDo(PassThrough).cleanup_signalref_Coder_FastPrimitivesCoder_3"ref_Windowing_Windowing_1”
ref_PCollection_PCollection_66∞
x112Load/BigQueryBatchFileLoads/ReshuffleBeforeLoadWithTempTables/ReshufflePerKey/Map(reify_metadata_default_window).Noneref_Coder_TupleCoder_20"ref_Windowing_Windowing_2ç
ref_PCollection_PCollection_103j
243Dynamic/_StreamToBigQuery/AppendDestination.Noneref_Coder_TupleCoder_32"ref_Windowing_Windowing_1Ö
ref_PCollection_PCollection_19c
!26ReadDirect/Read/EmitSource.None!ref_Coder__MemoizingPickleCoder_4"ref_Windowing_Windowing_1Ç
ref_PCollection_PCollection_4a
!26ReadTable/MapFilesToRemove.Noneref_Coder_FastPrimitivesCoder_3"ref_Windowing_Windowing_1á
ref_PCollection_PCollection_80e
-38Load/BigQueryBatchFileLoads/GroupByKey.Noneref_Coder_TupleCoder_17"ref_Windowing_Windowing_1ê
ref_PCollection_PCollection_36n
748ReadQuery/_PassThroughThenCleanup/Create/Impulse.Noneref_Coder_BytesCoder_1"ref_Windowing_Windowing_1Ω
ref_PCollection_PCollection_64ö
Z71Load/BigQueryBatchFileLoads/ParDo(PartitionFiles)/ParDo(PartitionFiles).SINGLE_PARTITIONref_Coder_FastPrimitivesCoder_3"ref_Windowing_Windowing_1É
ref_PCollection_PCollection_30a
!26ReadQuery/MapFilesToRemove.Noneref_Coder_FastPrimitivesCoder_3"ref_Windowing_Windowing_1Ö
ref_PCollection_PCollection_110b
*35Dynamic/_StreamToBigQuery/DropShard.Noneref_Coder_TupleCoder_30"ref_Windowing_Windowing_1ï
ref_Windowing_Windowing_2x
"
 beam:window_fn:global_windows:v1ref_Coder_GlobalWindowCoder_2"b (08HZ%ref_Environment_default_environment_1ï
ref_Windowing_Windowing_1x
"
 beam:window_fn:global_windows:v1ref_Coder_GlobalWindowCoder_2": (08HZ%ref_Environment_default_environment_1"4
ref_Coder_VarIntCoder_19

beam:coder:varint:v1"Ç
ref_Coder_TupleCoder_17g

beam:coder:kv:v15ref_Coder_DeterministicFastPrimitivesCoderV2_v2_69_16ref_Coder_IterableCoder_12"ã
ref_Coder_TupleCoder_21p

beam:coder:tuple:v1ref_Coder_FastPrimitivesCoder_3ref_Coder_NullableCoder_22ref_Coder_PaneInfoCoder_23"l
ref_Coder_TupleCoder_13Q

beam:coder:kv:v1ref_Coder_FastPrimitivesCoder_3ref_Coder_IterableCoder_12"a
ref_Coder_TupleCoder_33F

beam:coder:kv:v1ref_Coder_TupleCoder_34ref_Coder_TupleCoder_36"h
ref_Coder_TupleCoder_14M

beam:coder:kv:v1ref_Coder_FastPrimitivesCoder_3ref_Coder_TupleCoder_7"@
ref_Coder_GlobalWindowCoder_2

beam:coder:global_window:v1"Å
4ref_Coder__SDFBoundedSourceWrapperRestrictionCoder_6»
≈
beam:coder:pickled_python:v1§
°_SDFBoundedSourceWrapperRestrictionCoder$eNprYJ3qwwABPaKJBYnJGanxSamJuXqZ+UCUlFicOqVHIz7Yxc0pvzQvJTUlOL+0KDk1vCixoCC1KCi1uKQoM7kkMz/POT8ltWjK5CmajVNqpyTpAQDfhR+5"ù
ref_Coder_TupleCoder_9Ç

beam:coder:kv:v15ref_Coder_DeterministicFastPrimitivesCoderV2_v2_69_105ref_Coder_DeterministicFastPrimitivesCoderV2_v2_69_10"‡
ref_Coder_ListCoder_26≈
¬
beam:coder:pickled_python:v1°
ûListCoder$eNprYJ3azQABPZKJBYnJGanxSamJuXrJ+SmpRcVQakoPp09mcYkziDNl8hTNxim1U3q441NzUnPjwSqmZDD0CLslFpcEFGXmZpZklqUWoyrmj09LzMlJSkzORmjgDshMzs5JRVGYVAyEegAjIjm8"1
ref_Coder_BytesCoder_1

beam:coder:bytes:v1"¥
5ref_Coder_DeterministicFastPrimitivesCoderV2_v2_69_10˙
˜
beam:coder:pickled_python:v1÷
”DeterministicFastPrimitivesCoderV2$eNptjz1LA0EQhmORKoJICtHSSkESSCHYRomNRVSwHWb3JrdD9m6P/ThJIWhnsZ3j//UuRETINAMvzzPM+z78PjsYbCefYoPaECjCaqJdQT7sluTzO4rkK645RNYLDHHpueLILYXbnnmZyZdcfsibXORjSHUX2Q3XJWwviBnk8R7r18lHsEJrFer1nzBasl5b+geqoPIIQqQGLCqykq8eHBbTOZePifxmjlGbBVvq0zC99y41zwZ9QcWTe+2KnEAKBJ4s9l/AqkMbjCbIZz6EtuvKroaIpeRhO4PrG0lq8gPSOnH4"p
ref_Coder_TupleCoder_7V

beam:coder:kv:v1ref_Coder_FastPrimitivesCoder_3ref_Coder_FastPrimitivesCoder_3"”
!ref_Coder__MemoizingPickleCoder_4≠
™
beam:coder:pickled_python:v1â
Ü_MemoizingPickleCoder$eNprYJ3qywABPZKJBYnJGanxSamJuXrJ+SmpRcVQakqPaLxvam5+ZlVmXnpAZnJ2TqozSGLK5CmajVNqp/RwJYN1FmdWpU7xFihO0gMAF9kf5w=="É
ref_Coder_TupleCoder_36h

beam:coder:tuple:v1ref_Coder_TupleCoder_28ref_Coder_NullableCoder_22ref_Coder_PaneInfoCoder_23"ﬂ
ref_Coder_FastPrimitivesCoder_3ª
∏
beam:coder:pickled_python:v1ó
îFastPrimitivesCoder$eNprYJ2axgABPZKJBYnJGanxSamJuXrJ+SmpRcVQakqPsFticUlAUWZuZklmWWqxM0h4yuQpmo1Taqf08MenJebkJCUmZ8eD1U/JYOjhDshMzs5JRVGYVJykBwDOwCqZ"Y
ref_Coder_IterableCoder_12;

beam:coder:iterable:v1ref_Coder_FastPrimitivesCoder_3":
ref_Coder_StrUtf8Coder_29

beam:coder:string_utf8:v1"c
ref_Coder_TupleCoder_30H

beam:coder:kv:v1ref_Coder_StrUtf8Coder_29ref_Coder_TupleCoder_28"a
ref_Coder_TupleCoder_31F

beam:coder:kv:v1ref_Coder_TupleCoder_32ref_Coder_TupleCoder_28"c
ref_Coder_TupleCoder_11H

beam:coder:kv:v1ref_Coder_TupleCoder_9ref_Coder_IterableCoder_12"Q
ref_Coder_IterableCoder_253

beam:coder:iterable:v1ref_Coder_TupleCoder_21"Y
ref_Coder_NullableCoder_22;

beam:coder:nullable:v1ref_Coder_FastPrimitivesCoder_3"Q
ref_Coder_IterableCoder_383

beam:coder:iterable:v1ref_Coder_TupleCoder_36"i
ref_Coder_TupleCoder_27N

beam:coder:kv:v1ref_Coder_FastPrimitivesCoder_3ref_Coder_TupleCoder_28"d
ref_Coder_TupleCoder_37I

beam:coder:kv:v1ref_Coder_TupleCoder_34ref_Coder_IterableCoder_38"b
ref_Coder_TupleCoder_20G

beam:coder:kv:v1ref_Coder_VarIntCoder_19ref_Coder_TupleCoder_21"¶
ref_Coder_PaneInfoCoder_23á
Ñ
beam:coder:pickled_python:v1d
bPaneInfoCoder$eNprYJ1qygABPZKJBYnJGanxSamJuXrJ+SmpRcVQakoPb0BiXqpnXlq+M0hgyuQpmo1Taqck6QEAesMWaw=="j
ref_Coder_TupleCoder_18O

beam:coder:kv:v1ref_Coder_VarIntCoder_19ref_Coder_FastPrimitivesCoder_3"º
5ref_Coder_DeterministicFastPrimitivesCoderV2_v2_69_35Ç
ˇ
beam:coder:pickled_python:v1ﬁ
€DeterministicFastPrimitivesCoderV2$eNptj71LA0EQxWORKoKIhVhqpU0OIgi2iSjBJqKkHWY3k7she7vHfhxcIWhnsZ3j/+slKiI4zYPh/Wbeexl+nO4NdpNPsEFdESjCeqzdinz4FslnNxTJ12w5RNa3GOLCc82RWwqzrWc5kXe5eJVnOc+HkGy/Mh3bEnYXpBrko3+oHyYfwBqNUag3v8BowXpj6I9RBZVHECI1YFCRkXz5GH0fuIAvfXJTLh8S+a6Yubr/NbeBfJyvQnHnXWqm3T11ko8hBQJPBrdhYM2GGoxVkLe8D21fmZ2FiKXkYTuBq2tJavwJEPN01g=="Ö
ref_Coder_TupleCoder_5k

beam:coder:kv:v14ref_Coder__SDFBoundedSourceWrapperRestrictionCoder_6ref_Coder_FastPrimitivesCoder_3"g
ref_Coder_TupleCoder_8M

beam:coder:kv:v1ref_Coder_TupleCoder_9ref_Coder_FastPrimitivesCoder_3"á
ref_Coder_TupleCoder_15l

beam:coder:kv:v15ref_Coder_DeterministicFastPrimitivesCoderV2_v2_69_16ref_Coder_FastPrimitivesCoder_3"º
5ref_Coder_DeterministicFastPrimitivesCoderV2_v2_69_16Ç
ˇ
beam:coder:pickled_python:v1ﬁ
€DeterministicFastPrimitivesCoderV2$eNptj71LA0EQxWORKoKIRbC1UZsEUghpLyE2FhHEdpjdTO6G7O0d+3FwhaCdxXaO/697QRHBaYZ5/N7w3uv48/pkdJx0iS3qikAR1jPd7Mj57yXpak2BXM2WfWC9QR+2jmsO3JFfDczzQj7k9k1e5CadQ7RZMj3bEo4fpBqli39cP550Bns0RqE+/BomW9YHQ39A5VWagA/UgkFFRtLyocHdvODyMZLrCwy62rChQfXze9fEdjh90T+hMrSmnN9i4MbmVlOInsCRwSES7DPYYqi8vKdT6HLxjEHAUtK4W8DdUqKafQHObHc3"Å
ref_Coder_TupleCoder_34f

beam:coder:kv:v1ref_Coder_StrUtf8Coder_295ref_Coder_DeterministicFastPrimitivesCoderV2_v2_69_35"k
ref_Coder_TupleCoder_28P

beam:coder:kv:v1ref_Coder_FastPrimitivesCoder_3ref_Coder_StrUtf8Coder_29"e
ref_Coder_TupleCoder_24J

beam:coder:kv:v1ref_Coder_VarIntCoder_19ref_Coder_IterableCoder_25"k
ref_Coder_TupleCoder_32P

beam:coder:kv:v1ref_Coder_StrUtf8Coder_29ref_Coder_FastPrimitivesCoder_3*>
%ref_Environment_default_environment_1beam:env:default:v1)ref_AppliedPTransform_AppliedPTransform_1")beam:requirement:pardo:splittable_dofn:v1