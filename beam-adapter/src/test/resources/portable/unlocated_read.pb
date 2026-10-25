
©Ç
‹
/ref_AppliedPTransform_ReadCsv-_ReadFromPandas_3®
%
#beam:transform:generic_composite:v16ref_AppliedPTransform_ReadCsv-_ReadFromPandas-Create_48ref_AppliedPTransform_ReadCsv-_ReadFromPandas-MatchAll_97ref_AppliedPTransform_ReadCsv-_ReadFromPandas-DoOnce_11Iref_AppliedPTransform_ReadCsv-_ReadFromPandas-Map-lambda-at-io-py-306-_16Iref_AppliedPTransform_ReadCsv-_ReadFromPandas-Map-lambda-at-io-py-301-_17:ref_AppliedPTransform_ReadCsv-_ReadFromPandas-Reshuffle_18<ref_AppliedPTransform_ReadCsv-_ReadFromPandas-ReadMatches_25Kref_AppliedPTransform_ReadCsv-_ReadFromPandas-ParDo-_ReadFromPandasDoFn-_27*ReadCsv/_ReadFromPandas:%ref_Environment_default_environment_1B7
python_type(apache_beam.dataframe.io._ReadFromPandas
¯
8ref_AppliedPTransform_ReadCsv-_ReadFromPandas-MatchAll_9ª
%
#beam:transform:generic_composite:v1Lref_AppliedPTransform_ReadCsv-_ReadFromPandas-MatchAll-ParDo-_MatchAllFn-_10%
Noneref_PCollection_PCollection_3"%
Noneref_PCollection_PCollection_4* ReadCsv/_ReadFromPandas/MatchAll:%ref_Environment_default_environment_1B-
python_typeapache_beam.io.fileio.MatchAll
Ú
2ref_AppliedPTransform_ReadCsv-ToPCollection-df-_28ª
"
 beam:transform:pickled_python:v1^
<PlaceholderExpression[placeholder_DataFrame_139680289665360]ref_PCollection_PCollection_16"#
0ref_PCollection_PCollection_16*ReadCsv/ToPCollection(df):%ref_Environment_default_environment_1BN
python_type?apache_beam.dataframe.transforms._DataframeExpressionsTransform
‰
Eref_AppliedPTransform_ReadCsv-_ReadFromPandas-Create-MaybeReshuffle_7ö
"
 beam:transform:pickled_python:v1%
Noneref_PCollection_PCollection_2"%
Noneref_PCollection_PCollection_2*-ReadCsv/_ReadFromPandas/Create/MaybeReshuffle:%ref_Environment_default_environment_1BP
python_typeAapache_beam.transforms.core.Create.expand.<locals>.MaybeReshuffle
Â
Vref_AppliedPTransform_ReadCsv-_ReadFromPandas-Create-FlatMap-lambda-at-core-py-4094-_6ä

beam:transform:pardo:v1%
Noneref_PCollection_PCollection_1"%
Noneref_PCollection_PCollection_2*@ReadCsv/_ReadFromPandas/Create/FlatMap(<lambda at core.py:4094>)2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2t
beam:display_data:labelled:v1S
Transform Function<lambda>2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
Ì
Cref_AppliedPTransform_WriteReport-Write-WriteImpl-DoOnce-Impulse_36•

beam:transform:impulse:v1"&
Noneref_PCollection_PCollection_20**WriteReport/Write/WriteImpl/DoOnce/ImpulseB2
python_type#apache_beam.transforms.core.Impulse
¡
Uref_AppliedPTransform_ReadCsv-_ReadFromPandas-Reshuffle-ReshufflePerKey-GroupByKey_22Á
 
beam:transform:group_by_key:v1&
Noneref_PCollection_PCollection_11"&
Noneref_PCollection_PCollection_12*<ReadCsv/_ReadFromPandas/Reshuffle/ReshufflePerKey/GroupByKeyB5
python_type&apache_beam.transforms.core.GroupByKey
ê
:ref_AppliedPTransform_ReadCsv-_ReadFromPandas-Reshuffle_18—

beam:transform:reshuffle:v1Href_AppliedPTransform_ReadCsv-_ReadFromPandas-Reshuffle-AddRandomKeys_19Jref_AppliedPTransform_ReadCsv-_ReadFromPandas-Reshuffle-ReshufflePerKey_20Kref_AppliedPTransform_ReadCsv-_ReadFromPandas-Reshuffle-RemoveRandomKeys_24%
Noneref_PCollection_PCollection_4"&
Noneref_PCollection_PCollection_14*!ReadCsv/_ReadFromPandas/Reshuffle:%ref_Environment_default_environment_1B4
python_type%apache_beam.transforms.util.Reshuffle
‰
?ref_AppliedPTransform_ReadCsv-_ReadFromPandas-DoOnce-Impulse_12†

beam:transform:impulse:v1"%
Noneref_PCollection_PCollection_5*&ReadCsv/_ReadFromPandas/DoOnce/ImpulseB2
python_type#apache_beam.transforms.core.Impulse
Â
Fref_AppliedPTransform_ReadCsv-_ReadFromPandas-DoOnce-MaybeReshuffle_14ö
"
 beam:transform:pickled_python:v1%
Noneref_PCollection_PCollection_6"%
Noneref_PCollection_PCollection_6*-ReadCsv/_ReadFromPandas/DoOnce/MaybeReshuffle:%ref_Environment_default_environment_1BP
python_typeAapache_beam.transforms.core.Create.expand.<locals>.MaybeReshuffle
Â
Href_AppliedPTransform_WriteReport-ParDo-_RowDictionariesToArrowTable-_32ò

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_18"&
Noneref_PCollection_PCollection_19*/WriteReport/ParDo(_RowDictionariesToArrowTable)2ì
beam:display_data:labelled:v1r
Transform Function5apache_beam.io.parquetio._RowDictionariesToArrowTable2fn:!apache_beam.transforms.core.ParDo2ê
beam:display_data:labelled:v1o
row_group_buffer_size671088642row_group_buffer_size:5apache_beam.io.parquetio._RowDictionariesToArrowTable2x
beam:display_data:labelled:v1W
buffer_size10002buffer_size:5apache_beam.io.parquetio._RowDictionariesToArrowTable:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo

[ref_AppliedPTransform_WriteReport-Write-WriteImpl-DoOnce-FlatMap-lambda-at-core-py-4094-_37ê

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_20"&
Noneref_PCollection_PCollection_21*DWriteReport/Write/WriteImpl/DoOnce/FlatMap(<lambda at core.py:4094>)2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2t
beam:display_data:labelled:v1S
Transform Function<lambda>2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
π
Bref_AppliedPTransform_ReadCsv-_ReadFromPandas-Create-Map-decode-_8Ú

beam:transform:pardo:v1%
Noneref_PCollection_PCollection_2"%
Noneref_PCollection_PCollection_3**ReadCsv/_ReadFromPandas/Create/Map(decode)2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2r
beam:display_data:labelled:v1Q
Transform Functiondecode2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
∞
<ref_AppliedPTransform_WriteReport-Write-WriteImpl-Extract_45Ô

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_27"&
Noneref_PCollection_PCollection_28*#WriteReport/Write/WriteImpl/Extract2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2t
beam:display_data:labelled:v1S
Transform Function<lambda>2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
Â
Bref_AppliedPTransform_WriteReport-Write-WriteImpl-FinalizeWrite_47û

beam:transform:pardo:v14
python_side_input0ref_PCollection_PCollection_234
python_side_input1ref_PCollection_PCollection_284
python_side_input2ref_PCollection_PCollection_29&
Noneref_PCollection_PCollection_22"&
Noneref_PCollection_PCollection_30*)WriteReport/Write/WriteImpl/FinalizeWrite2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2{
beam:display_data:labelled:v1Z
Transform Function_finalize_write2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
Ô
$ref_AppliedPTransform_WriteReport_31∆
%
#beam:transform:generic_composite:v1Href_AppliedPTransform_WriteReport-ParDo-_RowDictionariesToArrowTable-_32*ref_AppliedPTransform_WriteReport-Write_33&
Noneref_PCollection_PCollection_18"&
Noneref_PCollection_PCollection_30*WriteReport2p
beam:display_data:labelled:v1O
compressionuncompressed2compression:%apache_beam.io.parquetio._ParquetSink2ô
beam:display_data:labelled:v1x
File Pattern3data/out/report-%(shard_num)05d-of-%(num_shards)05d2file_pattern:%apache_beam.io.parquetio._ParquetSink2\
beam:display_data:labelled:v1;
codecnone2codec:%apache_beam.io.parquetio._ParquetSink2z
beam:display_data:labelled:v1Y
schema id: int64 not null
amount: int642schema:%apache_beam.io.parquetio._ParquetSink2Ç
beam:display_data:labelled:v1a
row_group_buffer_size671088642row_group_buffer_size:'apache_beam.io.parquetio.WriteToParquet:%ref_Environment_default_environment_1B6
python_type'apache_beam.io.parquetio.WriteToParquet
¿
Dref_AppliedPTransform_WriteReport-Write-WriteImpl-InitializeWrite_40˜

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_22"&
Noneref_PCollection_PCollection_23*+WriteReport/Write/WriteImpl/InitializeWrite2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2t
beam:display_data:labelled:v1S
Transform Function<lambda>2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
Ù
ref_AppliedPTransform_Typed_30—

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_17"&
Noneref_PCollection_PCollection_18*Typed2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2t
beam:display_data:labelled:v1S
Transform Function<lambda>2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
∫
Cref_AppliedPTransform_ReadCsv-_ReadFromPandas-DoOnce-Map-decode-_15Ú

beam:transform:pardo:v1%
Noneref_PCollection_PCollection_6"%
Noneref_PCollection_PCollection_7**ReadCsv/_ReadFromPandas/DoOnce/Map(decode)2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2r
beam:display_data:labelled:v1Q
Transform Functiondecode2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
ï
?ref_AppliedPTransform_WriteReport-Write-WriteImpl-GroupByKey_44—
 
beam:transform:group_by_key:v1&
Noneref_PCollection_PCollection_26"&
Noneref_PCollection_PCollection_27*&WriteReport/Write/WriteImpl/GroupByKeyB5
python_type&apache_beam.transforms.core.GroupByKey
È
)ref_AppliedPTransform_AppliedPTransform_1ª
 ref_AppliedPTransform_ReadCsv_2ref_AppliedPTransform_Typed_30$ref_AppliedPTransform_WriteReport_31*)ref_AppliedPTransform_AppliedPTransform_1:%ref_Environment_default_environment_1
¡
;ref_AppliedPTransform_WriteReport-Write-WriteImpl-DoOnce_35Å
"
 beam:transform:pickled_python:v1Cref_AppliedPTransform_WriteReport-Write-WriteImpl-DoOnce-Impulse_36[ref_AppliedPTransform_WriteReport-Write-WriteImpl-DoOnce-FlatMap-lambda-at-core-py-4094-_37Jref_AppliedPTransform_WriteReport-Write-WriteImpl-DoOnce-MaybeReshuffle_38Gref_AppliedPTransform_WriteReport-Write-WriteImpl-DoOnce-Map-decode-_39"&
Noneref_PCollection_PCollection_22*"WriteReport/Write/WriteImpl/DoOnce:%ref_Environment_default_environment_1B1
python_type"apache_beam.transforms.core.Create
ù
*ref_AppliedPTransform_WriteReport-Write_33Ó
%
#beam:transform:generic_composite:v14ref_AppliedPTransform_WriteReport-Write-WriteImpl_34&
Noneref_PCollection_PCollection_19"&
Noneref_PCollection_PCollection_30*WriteReport/Write2q
beam:display_data:labelled:v1P
sink%apache_beam.io.parquetio._ParquetSink2sink:apache_beam.io.iobase.Write2p
beam:display_data:labelled:v1O
compressionuncompressed2compression:%apache_beam.io.parquetio._ParquetSink2ô
beam:display_data:labelled:v1x
File Pattern3data/out/report-%(shard_num)05d-of-%(num_shards)05d2file_pattern:%apache_beam.io.parquetio._ParquetSink2\
beam:display_data:labelled:v1;
codecnone2codec:%apache_beam.io.parquetio._ParquetSink2z
beam:display_data:labelled:v1Y
schema id: int64 not null
amount: int642schema:%apache_beam.io.parquetio._ParquetSink:%ref_Environment_default_environment_1B*
python_typeapache_beam.io.iobase.Write
ƒ
Gref_AppliedPTransform_WriteReport-Write-WriteImpl-DoOnce-Map-decode-_39¯

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_21"&
Noneref_PCollection_PCollection_22*.WriteReport/Write/WriteImpl/DoOnce/Map(decode)2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2r
beam:display_data:labelled:v1Q
Transform Functiondecode2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
„
>ref_AppliedPTransform_ReadCsv-_ReadFromPandas-Create-Impulse_5†

beam:transform:impulse:v1"%
Noneref_PCollection_PCollection_1*&ReadCsv/_ReadFromPandas/Create/ImpulseB2
python_type#apache_beam.transforms.core.Impulse
ä
Kref_AppliedPTransform_ReadCsv-_ReadFromPandas-ParDo-_ReadFromPandasDoFn-_27∫

beam:transform:pardo:v13
python_side_input0ref_PCollection_PCollection_9&
Noneref_PCollection_PCollection_15"&
Noneref_PCollection_PCollection_16*2ReadCsv/_ReadFromPandas/ParDo(_ReadFromPandasDoFn)2ä
beam:display_data:labelled:v1i
Transform Function,apache_beam.dataframe.io._ReadFromPandasDoFn2fn:!apache_beam.transforms.core.ParDo:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
ˇ
Iref_AppliedPTransform_ReadCsv-_ReadFromPandas-Map-lambda-at-io-py-301-_17±

beam:transform:pardo:v13
python_side_input0ref_PCollection_PCollection_8%
Noneref_PCollection_PCollection_7"%
Noneref_PCollection_PCollection_9*2ReadCsv/_ReadFromPandas/Map(<lambda at io.py:301>)2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2t
beam:display_data:labelled:v1S
Transform Function<lambda>2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
Ê
Wref_AppliedPTransform_ReadCsv-_ReadFromPandas-DoOnce-FlatMap-lambda-at-core-py-4094-_13ä

beam:transform:pardo:v1%
Noneref_PCollection_PCollection_5"%
Noneref_PCollection_PCollection_6*@ReadCsv/_ReadFromPandas/DoOnce/FlatMap(<lambda at core.py:4094>)2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2t
beam:display_data:labelled:v1S
Transform Function<lambda>2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
ê
ref_AppliedPTransform_ReadCsv_2Ï
%
#beam:transform:generic_composite:v1/ref_AppliedPTransform_ReadCsv-_ReadFromPandas_32ref_AppliedPTransform_ReadCsv-ToPCollection-df-_28Oref_AppliedPTransform_ReadCsv-Unbatch-placeholder_DataFrame_139680289665360-_29"&
Noneref_PCollection_PCollection_17*ReadCsv:%ref_Environment_default_environment_1B5
python_type&apache_beam.dataframe.io.ReadViaPandas
Œ
Kref_AppliedPTransform_ReadCsv-_ReadFromPandas-Reshuffle-RemoveRandomKeys_24˛

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_13"&
Noneref_PCollection_PCollection_14*2ReadCsv/_ReadFromPandas/Reshuffle/RemoveRandomKeys2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2t
beam:display_data:labelled:v1S
Transform Function<lambda>2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
 
Iref_AppliedPTransform_ReadCsv-_ReadFromPandas-Map-lambda-at-io-py-306-_16¸

beam:transform:pardo:v1%
Noneref_PCollection_PCollection_4"%
Noneref_PCollection_PCollection_8*2ReadCsv/_ReadFromPandas/Map(<lambda at io.py:306>)2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2t
beam:display_data:labelled:v1S
Transform Function<lambda>2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
Ô
Jref_AppliedPTransform_WriteReport-Write-WriteImpl-DoOnce-MaybeReshuffle_38†
"
 beam:transform:pickled_python:v1&
Noneref_PCollection_PCollection_21"&
Noneref_PCollection_PCollection_21*1WriteReport/Write/WriteImpl/DoOnce/MaybeReshuffle:%ref_Environment_default_environment_1BP
python_typeAapache_beam.transforms.core.Create.expand.<locals>.MaybeReshuffle
Ö
Jref_AppliedPTransform_ReadCsv-_ReadFromPandas-Reshuffle-ReshufflePerKey_20∂
%
#beam:transform:generic_composite:v1gref_AppliedPTransform_ReadCsv-_ReadFromPandas-Reshuffle-ReshufflePerKey-Map-reify_metadata_default_w_21Uref_AppliedPTransform_ReadCsv-_ReadFromPandas-Reshuffle-ReshufflePerKey-GroupByKey_22gref_AppliedPTransform_ReadCsv-_ReadFromPandas-Reshuffle-ReshufflePerKey-FlatMap-restore_metadata_def_23&
Noneref_PCollection_PCollection_10"&
Noneref_PCollection_PCollection_13*1ReadCsv/_ReadFromPandas/Reshuffle/ReshufflePerKey:%ref_Environment_default_environment_1B:
python_type+apache_beam.transforms.util.ReshufflePerKey
«
Href_AppliedPTransform_ReadCsv-_ReadFromPandas-Reshuffle-AddRandomKeys_19˙

beam:transform:pardo:v1%
Noneref_PCollection_PCollection_4"&
Noneref_PCollection_PCollection_10*/ReadCsv/_ReadFromPandas/Reshuffle/AddRandomKeys2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2t
beam:display_data:labelled:v1S
Transform Function<lambda>2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
™
gref_AppliedPTransform_ReadCsv-_ReadFromPandas-Reshuffle-ReshufflePerKey-FlatMap-restore_metadata_def_23æ

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_12"&
Noneref_PCollection_PCollection_13*ZReadCsv/_ReadFromPandas/Reshuffle/ReshufflePerKey/FlatMap(restore_metadata_default_window)2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2ã
beam:display_data:labelled:v1j
Transform Functionrestore_metadata_default_window2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
€
Rref_AppliedPTransform_ReadCsv-_ReadFromPandas-ReadMatches-ParDo-_ReadMatchesFn-_26Ñ

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_14"&
Noneref_PCollection_PCollection_15*9ReadCsv/_ReadFromPandas/ReadMatches/ParDo(_ReadMatchesFn)2Ç
beam:display_data:labelled:v1a
Transform Function$apache_beam.io.fileio._ReadMatchesFn2fn:!apache_beam.transforms.core.ParDo:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
™
9ref_AppliedPTransform_WriteReport-Write-WriteImpl-Pair_43Ï

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_25"&
Noneref_PCollection_PCollection_26* WriteReport/Write/WriteImpl/Pair2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2t
beam:display_data:labelled:v1S
Transform Function<lambda>2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
˘
4ref_AppliedPTransform_WriteReport-Write-WriteImpl_34¿
%
#beam:transform:generic_composite:v1;ref_AppliedPTransform_WriteReport-Write-WriteImpl-DoOnce_35Dref_AppliedPTransform_WriteReport-Write-WriteImpl-InitializeWrite_40Mref_AppliedPTransform_WriteReport-Write-WriteImpl-WindowInto-WindowIntoFn-_41Aref_AppliedPTransform_WriteReport-Write-WriteImpl-WriteBundles_429ref_AppliedPTransform_WriteReport-Write-WriteImpl-Pair_43?ref_AppliedPTransform_WriteReport-Write-WriteImpl-GroupByKey_44<ref_AppliedPTransform_WriteReport-Write-WriteImpl-Extract_45@ref_AppliedPTransform_WriteReport-Write-WriteImpl-PreFinalize_46Bref_AppliedPTransform_WriteReport-Write-WriteImpl-FinalizeWrite_47&
Noneref_PCollection_PCollection_19"&
Noneref_PCollection_PCollection_30*WriteReport/Write/WriteImpl:%ref_Environment_default_environment_1B.
python_typeapache_beam.io.iobase.WriteImpl
£
6ref_AppliedPTransform_ReadCsv-_ReadFromPandas-Create_4Ë
"
 beam:transform:pickled_python:v1>ref_AppliedPTransform_ReadCsv-_ReadFromPandas-Create-Impulse_5Vref_AppliedPTransform_ReadCsv-_ReadFromPandas-Create-FlatMap-lambda-at-core-py-4094-_6Eref_AppliedPTransform_ReadCsv-_ReadFromPandas-Create-MaybeReshuffle_7Bref_AppliedPTransform_ReadCsv-_ReadFromPandas-Create-Map-decode-_8"%
Noneref_PCollection_PCollection_3*ReadCsv/_ReadFromPandas/Create:%ref_Environment_default_environment_1B1
python_type"apache_beam.transforms.core.Create
·
Oref_AppliedPTransform_ReadCsv-Unbatch-placeholder_DataFrame_139680289665360-_29ç

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_16"&
Noneref_PCollection_PCollection_17*7ReadCsv/Unbatch 'placeholder_DataFrame_139680289665360'2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.dataframe.convert.DataFrameToRowsFn2fn:!apache_beam.transforms.core.ParDo:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
Ÿ
Aref_AppliedPTransform_WriteReport-Write-WriteImpl-WriteBundles_42ì

beam:transform:pardo:v14
python_side_input0ref_PCollection_PCollection_23&
Noneref_PCollection_PCollection_24"&
Noneref_PCollection_PCollection_25*(WriteReport/Write/WriteImpl/WriteBundles2Ñ
beam:display_data:labelled:v1c
Transform Function&apache_beam.io.iobase._WriteBundleDoFn2fn:!apache_beam.transforms.core.ParDo2p
beam:display_data:labelled:v1O
compressionuncompressed2compression:%apache_beam.io.parquetio._ParquetSink2ô
beam:display_data:labelled:v1x
File Pattern3data/out/report-%(shard_num)05d-of-%(num_shards)05d2file_pattern:%apache_beam.io.parquetio._ParquetSink2\
beam:display_data:labelled:v1;
codecnone2codec:%apache_beam.io.parquetio._ParquetSink2z
beam:display_data:labelled:v1Y
schema id: int64 not null
amount: int642schema:%apache_beam.io.parquetio._ParquetSink:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
©
@ref_AppliedPTransform_WriteReport-Write-WriteImpl-PreFinalize_46‰

beam:transform:pardo:v14
python_side_input0ref_PCollection_PCollection_234
python_side_input1ref_PCollection_PCollection_28&
Noneref_PCollection_PCollection_22"&
Noneref_PCollection_PCollection_29*'WriteReport/Write/WriteImpl/PreFinalize2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2y
beam:display_data:labelled:v1X
Transform Function_pre_finalize2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
…
Lref_AppliedPTransform_ReadCsv-_ReadFromPandas-MatchAll-ParDo-_MatchAllFn-_10¯

beam:transform:pardo:v1%
Noneref_PCollection_PCollection_3"%
Noneref_PCollection_PCollection_4*3ReadCsv/_ReadFromPandas/MatchAll/ParDo(_MatchAllFn)2
beam:display_data:labelled:v1^
Transform Function!apache_beam.io.fileio._MatchAllFn2fn:!apache_beam.transforms.core.ParDo:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
ä
<ref_AppliedPTransform_ReadCsv-_ReadFromPandas-ReadMatches_25…
%
#beam:transform:generic_composite:v1Rref_AppliedPTransform_ReadCsv-_ReadFromPandas-ReadMatches-ParDo-_ReadMatchesFn-_26&
Noneref_PCollection_PCollection_14"&
Noneref_PCollection_PCollection_15*#ReadCsv/_ReadFromPandas/ReadMatches:%ref_Environment_default_environment_1B0
python_type!apache_beam.io.fileio.ReadMatches
®
7ref_AppliedPTransform_ReadCsv-_ReadFromPandas-DoOnce_11Ï
"
 beam:transform:pickled_python:v1?ref_AppliedPTransform_ReadCsv-_ReadFromPandas-DoOnce-Impulse_12Wref_AppliedPTransform_ReadCsv-_ReadFromPandas-DoOnce-FlatMap-lambda-at-core-py-4094-_13Fref_AppliedPTransform_ReadCsv-_ReadFromPandas-DoOnce-MaybeReshuffle_14Cref_AppliedPTransform_ReadCsv-_ReadFromPandas-DoOnce-Map-decode-_15"%
Noneref_PCollection_PCollection_7*ReadCsv/_ReadFromPandas/DoOnce:%ref_Environment_default_environment_1B1
python_type"apache_beam.transforms.core.Create
¢
gref_AppliedPTransform_ReadCsv-_ReadFromPandas-Reshuffle-ReshufflePerKey-Map-reify_metadata_default_w_21∂

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_10"&
Noneref_PCollection_PCollection_11*TReadCsv/_ReadFromPandas/Reshuffle/ReshufflePerKey/Map(reify_metadata_default_window)2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2â
beam:display_data:labelled:v1h
Transform Functionreify_metadata_default_window2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
Â
Mref_AppliedPTransform_WriteReport-Write-WriteImpl-WindowInto-WindowIntoFn-_41ì

beam:transform:window_into:v1&
Noneref_PCollection_PCollection_19"&
Noneref_PCollection_PCollection_24*4WriteReport/Write/WriteImpl/WindowInto(WindowIntoFn)2ã
beam:display_data:labelled:v1j
Transform Function(apache_beam.transforms.core.WindowIntoFn2fn:&apache_beam.transforms.core.WindowInto:%ref_Environment_default_environment_1B5
python_type&apache_beam.transforms.core.WindowIntoñ
ref_PCollection_PCollection_17t
>55ReadCsv/Unbatch 'placeholder_DataFrame_139680289665360'.Noneref_Coder_RowCoder_14"ref_Windowing_Windowing_1ö
ref_PCollection_PCollection_9y
950ReadCsv/_ReadFromPandas/Map(<lambda at io.py:301>).Noneref_Coder_FastPrimitivesCoder_4"ref_Windowing_Windowing_1ü
ref_PCollection_PCollection_2~
G64ReadCsv/_ReadFromPandas/Create/FlatMap(<lambda at core.py:4094>).Noneref_Coder_BytesCoder_1"ref_Windowing_Windowing_1è
ref_PCollection_PCollection_10m
647ReadCsv/_ReadFromPandas/Reshuffle/AddRandomKeys.Noneref_Coder_TupleCoder_5"ref_Windowing_Windowing_1õ
ref_PCollection_PCollection_16y
950ReadCsv/_ReadFromPandas/ParDo(_ReadFromPandasDoFn).Noneref_Coder_FastPrimitivesCoder_4"ref_Windowing_Windowing_1ó
ref_PCollection_PCollection_22u
546WriteReport/Write/WriteImpl/DoOnce/Map(decode).Noneref_Coder_FastPrimitivesCoder_4"ref_Windowing_Windowing_1í
ref_PCollection_PCollection_7q
142ReadCsv/_ReadFromPandas/DoOnce/Map(decode).Noneref_Coder_FastPrimitivesCoder_4"ref_Windowing_Windowing_1õ
ref_PCollection_PCollection_14y
950ReadCsv/_ReadFromPandas/Reshuffle/RemoveRandomKeys.Noneref_Coder_FastPrimitivesCoder_4"ref_Windowing_Windowing_1ë
ref_PCollection_PCollection_25o
/40WriteReport/Write/WriteImpl/WriteBundles.Noneref_Coder_FastPrimitivesCoder_4"ref_Windowing_Windowing_1ù
ref_PCollection_PCollection_24{
;52WriteReport/Write/WriteImpl/WindowInto(WindowIntoFn).Noneref_Coder_FastPrimitivesCoder_4"ref_Windowing_Windowing_1µ
ref_PCollection_PCollection_11í
[84ReadCsv/_ReadFromPandas/Reshuffle/ReshufflePerKey/Map(reify_metadata_default_window).Noneref_Coder_TupleCoder_7"ref_Windowing_Windowing_2ö
ref_PCollection_PCollection_8y
950ReadCsv/_ReadFromPandas/Map(<lambda at io.py:306>).Noneref_Coder_FastPrimitivesCoder_4"ref_Windowing_Windowing_1ù
ref_PCollection_PCollection_12{
C60ReadCsv/_ReadFromPandas/Reshuffle/ReshufflePerKey/GroupByKey.Noneref_Coder_TupleCoder_11"ref_Windowing_Windowing_2Ö
ref_PCollection_PCollection_5d
-38ReadCsv/_ReadFromPandas/DoOnce/Impulse.Noneref_Coder_BytesCoder_1"ref_Windowing_Windowing_1î
ref_PCollection_PCollection_23r
243WriteReport/Write/WriteImpl/InitializeWrite.Noneref_Coder_FastPrimitivesCoder_4"ref_Windowing_Windowing_1ª
ref_PCollection_PCollection_13ò
a90ReadCsv/_ReadFromPandas/Reshuffle/ReshufflePerKey/FlatMap(restore_metadata_default_window).Noneref_Coder_TupleCoder_5"ref_Windowing_Windowing_1å
ref_PCollection_PCollection_28j
*35WriteReport/Write/WriteImpl/Extract.Noneref_Coder_FastPrimitivesCoder_4"ref_Windowing_Windowing_1ä
ref_PCollection_PCollection_20h
142WriteReport/Write/WriteImpl/DoOnce/Impulse.Noneref_Coder_BytesCoder_1"ref_Windowing_Windowing_1£
ref_PCollection_PCollection_15Ä
@57ReadCsv/_ReadFromPandas/ReadMatches/ParDo(_ReadMatchesFn).Noneref_Coder_FastPrimitivesCoder_4"ref_Windowing_Windowing_1ê
ref_PCollection_PCollection_29n
.39WriteReport/Write/WriteImpl/PreFinalize.Noneref_Coder_FastPrimitivesCoder_4"ref_Windowing_Windowing_1ü
ref_PCollection_PCollection_6~
G64ReadCsv/_ReadFromPandas/DoOnce/FlatMap(<lambda at core.py:4094>).Noneref_Coder_BytesCoder_1"ref_Windowing_Windowing_1Å
ref_PCollection_PCollection_26_
'32WriteReport/Write/WriteImpl/Pair.Noneref_Coder_TupleCoder_16"ref_Windowing_Windowing_1á
ref_PCollection_PCollection_27e
-38WriteReport/Write/WriteImpl/GroupByKey.Noneref_Coder_TupleCoder_18"ref_Windowing_Windowing_1c
ref_PCollection_PCollection_18A
5Typed.Noneref_Coder_RowCoder_15"ref_Windowing_Windowing_1ã
ref_PCollection_PCollection_3j
142ReadCsv/_ReadFromPandas/Create/Map(decode).Noneref_Coder_StrUtf8Coder_3"ref_Windowing_Windowing_1Ö
ref_PCollection_PCollection_1d
-38ReadCsv/_ReadFromPandas/Create/Impulse.Noneref_Coder_BytesCoder_1"ref_Windowing_Windowing_1ò
ref_PCollection_PCollection_19v
647WriteReport/ParDo(_RowDictionariesToArrowTable).Noneref_Coder_FastPrimitivesCoder_4"ref_Windowing_Windowing_1õ
ref_PCollection_PCollection_4z
:51ReadCsv/_ReadFromPandas/MatchAll/ParDo(_MatchAllFn).Noneref_Coder_FastPrimitivesCoder_4"ref_Windowing_Windowing_1ã
ref_PCollection_PCollection_30i
041WriteReport/Write/WriteImpl/FinalizeWrite.Noneref_Coder_StrUtf8Coder_3"ref_Windowing_Windowing_1•
ref_PCollection_PCollection_21Ç
K68WriteReport/Write/WriteImpl/DoOnce/FlatMap(<lambda at core.py:4094>).Noneref_Coder_BytesCoder_1"ref_Windowing_Windowing_1ï
ref_Windowing_Windowing_2x
"
 beam:window_fn:global_windows:v1ref_Coder_GlobalWindowCoder_2"b (08HZ%ref_Environment_default_environment_1ï
ref_Windowing_Windowing_1x
"
 beam:window_fn:global_windows:v1ref_Coder_GlobalWindowCoder_2": (08HZ%ref_Environment_default_environment_1"p
ref_Coder_RowCoder_15W
U
beam:coder:row:v1@

id

amount$4a7170af-76a6-43ae-a3db-cb10267d45ab"Y
ref_Coder_IterableCoder_19;

beam:coder:iterable:v1ref_Coder_FastPrimitivesCoder_4"d
ref_Coder_TupleCoder_11I

beam:coder:kv:v1ref_Coder_VarIntCoder_6ref_Coder_IterableCoder_12"n
ref_Coder_RowCoder_14U
S
beam:coder:row:v1>

id

amount$73dae6cd-7f27-448e-960e-6b2a6757c801"@
ref_Coder_GlobalWindowCoder_2

beam:coder:global_window:v1"q
ref_Coder_TupleCoder_13V

beam:coder:kv:v1ref_Coder_FastPrimitivesCoder_4ref_Coder_FastPrimitivesCoder_4"1
ref_Coder_BytesCoder_1

beam:coder:bytes:v1"Ç
ref_Coder_TupleCoder_18g

beam:coder:kv:v15ref_Coder_DeterministicFastPrimitivesCoderV2_v2_69_17ref_Coder_IterableCoder_19"3
ref_Coder_VarIntCoder_6

beam:coder:varint:v1"X
ref_Coder_NullableCoder_9;

beam:coder:nullable:v1ref_Coder_FastPrimitivesCoder_4"9
ref_Coder_StrUtf8Coder_3

beam:coder:string_utf8:v1"á
ref_Coder_TupleCoder_16l

beam:coder:kv:v15ref_Coder_DeterministicFastPrimitivesCoderV2_v2_69_17ref_Coder_FastPrimitivesCoder_4"_
ref_Coder_TupleCoder_7E

beam:coder:kv:v1ref_Coder_VarIntCoder_6ref_Coder_TupleCoder_8"â
ref_Coder_TupleCoder_8o

beam:coder:tuple:v1ref_Coder_FastPrimitivesCoder_4ref_Coder_NullableCoder_9ref_Coder_PaneInfoCoder_10"¨
5ref_Coder_DeterministicFastPrimitivesCoderV2_v2_69_17Ú
Ô
beam:coder:pickled_python:v1Œ
ÀDeterministicFastPrimitivesCoderV2$eNptj7FLA1EMxuvQqYJIBXF1EF1a6CC4qijiUhx0DHnPtBf67u7xXu7gBkE3h2zG/9f2VERohi8hfL8v5HX4ebAz6EuPMKIvCBxhOfH1M6X800yPr0kolVxxFvY3mGWeuGThlvLVxvM4sw87e7MXO9V9aKr1KnRcLaFPsGKg4y3UL6N7sMAQHPrVHzCas18F+md02ekIslCEgI6C6clTYqEHinWSaT9/610Zw/Q21U287O6pMz2EJhMkCri5DwsOFFGKbO+6C+36S64rEFyaDtsZnF9Y4yZfP/Nvyw=="h
ref_Coder_TupleCoder_5N

beam:coder:kv:v1ref_Coder_VarIntCoder_6ref_Coder_FastPrimitivesCoder_4"¶
ref_Coder_PaneInfoCoder_10á
Ñ
beam:coder:pickled_python:v1d
bPaneInfoCoder$eNprYJ1qygABPZKJBYnJGanxSamJuXrJ+SmpRcVQakoPb0BiXqpnXlq+M0hgyuQpmo1Taqck6QEAesMWaw=="ﬂ
ref_Coder_FastPrimitivesCoder_4ª
∏
beam:coder:pickled_python:v1ó
îFastPrimitivesCoder$eNprYJ2axgABPZKJBYnJGanxSamJuXrJ+SmpRcVQakqPsFticUlAUWZuZklmWWqxM0h4yuQpmo1Taqf08MenJebkJCUmZ8eD1U/JYOjhDshMzs5JRVGYVJykBwDOwCqZ"P
ref_Coder_IterableCoder_122

beam:coder:iterable:v1ref_Coder_TupleCoder_8*>
%ref_Environment_default_environment_1beam:env:default:v1)ref_AppliedPTransform_AppliedPTransform_1")beam:requirement:pardo:splittable_dofn:v1