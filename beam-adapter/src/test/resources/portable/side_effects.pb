
ñî
±
%ref_AppliedPTransform_Make-Impulse_29á

beam:transform:impulse:v1"&
Noneref_PCollection_PCollection_19*Make/ImpulseB2
python_type#apache_beam.transforms.core.Impulse
·
Kref_AppliedPTransform_WriteMade-Write-WriteImpl-WindowInto-WindowIntoFn-_42ë

beam:transform:window_into:v1&
Noneref_PCollection_PCollection_21"&
Noneref_PCollection_PCollection_26*2WriteMade/Write/WriteImpl/WindowInto(WindowIntoFn)2ã
beam:display_data:labelled:v1j
Transform Function(apache_beam.transforms.core.WindowIntoFn2fn:&apache_beam.transforms.core.WindowInto:%ref_Environment_default_environment_1B5
python_type&apache_beam.transforms.core.WindowInto
•
>ref_AppliedPTransform_ReadOrders-Read-SDFBoundedSourceReader_6‚
%
#beam:transform:generic_composite:v1Zref_AppliedPTransform_ReadOrders-Read-SDFBoundedSourceReader-ParDo-SDFBoundedSourceDoFn-_7%
Noneref_PCollection_PCollection_2"%
Noneref_PCollection_PCollection_3*&ReadOrders/Read/SDFBoundedSourceReader2É
beam:display_data:labelled:v1b
File Patterndata/in/orders/*.parquet2file_pattern:*apache_beam.io.iobase.SDFBoundedSourceDoFn2r
beam:display_data:labelled:v1Q
Compression Typeauto2compression:*apache_beam.io.iobase.SDFBoundedSourceDoFn2à
beam:display_data:labelled:v1g
source'apache_beam.io.parquetio._ParquetSource2source:,apache_beam.io.iobase.SDFBoundedSourceReader:%ref_Environment_default_environment_1B;
python_type,apache_beam.io.iobase.SDFBoundedSourceReader
¬
Gref_AppliedPTransform_WriteReport-Write-WriteImpl-DoOnce-Map-decode-_17ˆ

beam:transform:pardo:v1%
Noneref_PCollection_PCollection_7"%
Noneref_PCollection_PCollection_8*.WriteReport/Write/WriteImpl/DoOnce/Map(decode)2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2r
beam:display_data:labelled:v1Q
Transform Functiondecode2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
„
Bref_AppliedPTransform_WriteReport-Write-WriteImpl-FinalizeWrite_25ú

beam:transform:pardo:v13
python_side_input0ref_PCollection_PCollection_94
python_side_input1ref_PCollection_PCollection_144
python_side_input2ref_PCollection_PCollection_15%
Noneref_PCollection_PCollection_8"&
Noneref_PCollection_PCollection_16*)WriteReport/Write/WriteImpl/FinalizeWrite2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2{
beam:display_data:labelled:v1Z
Transform Function_finalize_write2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
Ï
Cref_AppliedPTransform_WriteReport-Write-WriteImpl-DoOnce-Impulse_14§

beam:transform:impulse:v1"%
Noneref_PCollection_PCollection_6**WriteReport/Write/WriteImpl/DoOnce/ImpulseB2
python_type#apache_beam.transforms.core.Impulse
Ä
&ref_AppliedPTransform_PushToService_27’

beam:transform:pardo:v1%
Noneref_PCollection_PCollection_4"&
Noneref_PCollection_PCollection_18*PushToService2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2q
beam:display_data:labelled:v1P
Transform Functionprint2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
Ó
[ref_AppliedPTransform_WriteReport-Write-WriteImpl-DoOnce-FlatMap-lambda-at-core-py-4094-_15é

beam:transform:pardo:v1%
Noneref_PCollection_PCollection_6"%
Noneref_PCollection_PCollection_7*DWriteReport/Write/WriteImpl/DoOnce/FlatMap(<lambda at core.py:4094>)2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2t
beam:display_data:labelled:v1S
Transform Function<lambda>2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
•
>ref_AppliedPTransform_WriteMade-Write-WriteImpl-PreFinalize_47‚

beam:transform:pardo:v14
python_side_input0ref_PCollection_PCollection_254
python_side_input1ref_PCollection_PCollection_30&
Noneref_PCollection_PCollection_24"&
Noneref_PCollection_PCollection_31*%WriteMade/Write/WriteImpl/PreFinalize2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2y
beam:display_data:labelled:v1X
Transform Function_pre_finalize2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
È
Aref_AppliedPTransform_WriteMade-Write-WriteImpl-DoOnce-Impulse_37£

beam:transform:impulse:v1"&
Noneref_PCollection_PCollection_22*(WriteMade/Write/WriteImpl/DoOnce/ImpulseB2
python_type#apache_beam.transforms.core.Impulse
æ
?ref_AppliedPTransform_ReadOrders-ParDo-_ArrowTableToBeamRows-_8˙

beam:transform:pardo:v1%
Noneref_PCollection_PCollection_3"%
Noneref_PCollection_PCollection_4*'ReadOrders/ParDo(_ArrowTableToBeamRows)2å
beam:display_data:labelled:v1k
Transform Function.apache_beam.io.parquetio._ArrowTableToBeamRows2fn:!apache_beam.transforms.core.ParDo:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
≥
,ref_AppliedPTransform_Make-MaybeReshuffle_31Ç
"
 beam:transform:pickled_python:v1&
Noneref_PCollection_PCollection_20"&
Noneref_PCollection_PCollection_20*Make/MaybeReshuffle:%ref_Environment_default_environment_1BP
python_typeAapache_beam.transforms.core.Create.expand.<locals>.MaybeReshuffle
‡

Aref_AppliedPTransform_WriteReport-Write-WriteImpl-WriteBundles_20ö


beam:transform:pardo:v13
python_side_input0ref_PCollection_PCollection_9&
Noneref_PCollection_PCollection_10"&
Noneref_PCollection_PCollection_11*(WriteReport/Write/WriteImpl/WriteBundles2Ñ
beam:display_data:labelled:v1c
Transform Function&apache_beam.io.iobase._WriteBundleDoFn2fn:!apache_beam.transforms.core.ParDo2p
beam:display_data:labelled:v1O
compressionuncompressed2compression:%apache_beam.io.parquetio._ParquetSink2ô
beam:display_data:labelled:v1x
File Pattern3data/out/report-%(shard_num)05d-of-%(num_shards)05d2file_pattern:%apache_beam.io.parquetio._ParquetSink2\
beam:display_data:labelled:v1;
codecnone2codec:%apache_beam.io.parquetio._ParquetSink2Å
beam:display_data:labelled:v1ﬂ
schema•id: int64 not null
amount: int64
tags: list<item: string>
  child 0, item: string
placed: timestamp[us, tz=UTC]
price: decimal128(10, 2)
customer: struct<name: string, since: date32[day] not null>
  child 0, name: string
  child 1, since: date32[day] not null
attrs: map<string, int32>
  child 0, entries: struct<key: string not null, value: int32> not null
      child 0, key: string not null
      child 1, value: int322schema:%apache_beam.io.parquetio._ParquetSink:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
Ï
Yref_AppliedPTransform_WriteMade-Write-WriteImpl-DoOnce-FlatMap-lambda-at-core-py-4094-_38é

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_22"&
Noneref_PCollection_PCollection_23*BWriteMade/Write/WriteImpl/DoOnce/FlatMap(<lambda at core.py:4094>)2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2t
beam:display_data:labelled:v1S
Transform Function<lambda>2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
º
Bref_AppliedPTransform_WriteMade-Write-WriteImpl-InitializeWrite_41ı

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_24"&
Noneref_PCollection_PCollection_25*)WriteMade/Write/WriteImpl/InitializeWrite2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2t
beam:display_data:labelled:v1S
Transform Function<lambda>2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
™
9ref_AppliedPTransform_WriteReport-Write-WriteImpl-Pair_21Ï

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_11"&
Noneref_PCollection_PCollection_12* WriteReport/Write/WriteImpl/Pair2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2t
beam:display_data:labelled:v1S
Transform Function<lambda>2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
à
)ref_AppliedPTransform_Make-Map-decode-_32⁄

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_20"&
Noneref_PCollection_PCollection_21*Make/Map(decode)2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2r
beam:display_data:labelled:v1Q
Transform Functiondecode2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
æ
Dref_AppliedPTransform_WriteReport-Write-WriteImpl-InitializeWrite_18ı

beam:transform:pardo:v1%
Noneref_PCollection_PCollection_8"%
Noneref_PCollection_PCollection_9*+WriteReport/Write/WriteImpl/InitializeWrite2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2t
beam:display_data:labelled:v1S
Transform Function<lambda>2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
÷
?ref_AppliedPTransform_WriteMade-Write-WriteImpl-WriteBundles_43í

beam:transform:pardo:v14
python_side_input0ref_PCollection_PCollection_25&
Noneref_PCollection_PCollection_26"&
Noneref_PCollection_PCollection_27*&WriteMade/Write/WriteImpl/WriteBundles2Ñ
beam:display_data:labelled:v1c
Transform Function&apache_beam.io.iobase._WriteBundleDoFn2fn:!apache_beam.transforms.core.ParDo2b
beam:display_data:labelled:v1A
compressionauto2compression:apache_beam.io.textio._TextSink2ë
beam:display_data:labelled:v1p
File Pattern1data/out/made-%(shard_num)05d-of-%(num_shards)05d2file_pattern:apache_beam.io.textio._TextSink2o
beam:display_data:labelled:v1N
Append Trailing New Lines2append_newline:apache_beam.io.textio._TextSink:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
Ê
'ref_AppliedPTransform_ReadOrders-Read_3∫

beam:transform:read:v1/ref_AppliedPTransform_ReadOrders-Read-Impulse_42ref_AppliedPTransform_ReadOrders-Read-EmitSource_5>ref_AppliedPTransform_ReadOrders-Read-SDFBoundedSourceReader_6"%
Noneref_PCollection_PCollection_3*ReadOrders/Read2{
beam:display_data:labelled:v1Z
Read Source'apache_beam.io.parquetio._ParquetSource2source:apache_beam.io.iobase.Read2Ä
beam:display_data:labelled:v1_
File Patterndata/in/orders/*.parquet2file_pattern:'apache_beam.io.parquetio._ParquetSource2o
beam:display_data:labelled:v1N
Compression Typeauto2compression:'apache_beam.io.parquetio._ParquetSource:%ref_Environment_default_environment_1B)
python_typeapache_beam.io.iobase.Read
◊
)ref_AppliedPTransform_AppliedPTransform_1©
 "ref_AppliedPTransform_ReadOrders_2#ref_AppliedPTransform_WriteReport_9ref_AppliedPTransform_Notify_26&ref_AppliedPTransform_PushToService_27ref_AppliedPTransform_Make_28"ref_AppliedPTransform_WriteMade_33*)ref_AppliedPTransform_AppliedPTransform_1:%ref_Environment_default_environment_1
í
(ref_AppliedPTransform_WriteMade-Write_34Â
%
#beam:transform:generic_composite:v12ref_AppliedPTransform_WriteMade-Write-WriteImpl_35&
Noneref_PCollection_PCollection_21"&
Noneref_PCollection_PCollection_32*WriteMade/Write2k
beam:display_data:labelled:v1J
sinkapache_beam.io.textio._TextSink2sink:apache_beam.io.iobase.Write2b
beam:display_data:labelled:v1A
compressionauto2compression:apache_beam.io.textio._TextSink2ë
beam:display_data:labelled:v1p
File Pattern1data/out/made-%(shard_num)05d-of-%(num_shards)05d2file_pattern:apache_beam.io.textio._TextSink2o
beam:display_data:labelled:v1N
Append Trailing New Lines2append_newline:apache_beam.io.textio._TextSink:%ref_Environment_default_environment_1B*
python_typeapache_beam.io.iobase.Write
„
2ref_AppliedPTransform_WriteMade-Write-WriteImpl_35¨
%
#beam:transform:generic_composite:v19ref_AppliedPTransform_WriteMade-Write-WriteImpl-DoOnce_36Bref_AppliedPTransform_WriteMade-Write-WriteImpl-InitializeWrite_41Kref_AppliedPTransform_WriteMade-Write-WriteImpl-WindowInto-WindowIntoFn-_42?ref_AppliedPTransform_WriteMade-Write-WriteImpl-WriteBundles_437ref_AppliedPTransform_WriteMade-Write-WriteImpl-Pair_44=ref_AppliedPTransform_WriteMade-Write-WriteImpl-GroupByKey_45:ref_AppliedPTransform_WriteMade-Write-WriteImpl-Extract_46>ref_AppliedPTransform_WriteMade-Write-WriteImpl-PreFinalize_47@ref_AppliedPTransform_WriteMade-Write-WriteImpl-FinalizeWrite_48&
Noneref_PCollection_PCollection_21"&
Noneref_PCollection_PCollection_32*WriteMade/Write/WriteImpl:%ref_Environment_default_environment_1B.
python_typeapache_beam.io.iobase.WriteImpl
ç
ref_AppliedPTransform_Make_28Î
"
 beam:transform:pickled_python:v1%ref_AppliedPTransform_Make-Impulse_29=ref_AppliedPTransform_Make-FlatMap-lambda-at-core-py-4094-_30,ref_AppliedPTransform_Make-MaybeReshuffle_31)ref_AppliedPTransform_Make-Map-decode-_32"&
Noneref_PCollection_PCollection_21*Make:%ref_Environment_default_environment_1B1
python_type"apache_beam.transforms.core.Create
§

*ref_AppliedPTransform_WriteReport-Write_11ı	
%
#beam:transform:generic_composite:v14ref_AppliedPTransform_WriteReport-Write-WriteImpl_12%
Noneref_PCollection_PCollection_5"&
Noneref_PCollection_PCollection_16*WriteReport/Write2q
beam:display_data:labelled:v1P
sink%apache_beam.io.parquetio._ParquetSink2sink:apache_beam.io.iobase.Write2p
beam:display_data:labelled:v1O
compressionuncompressed2compression:%apache_beam.io.parquetio._ParquetSink2ô
beam:display_data:labelled:v1x
File Pattern3data/out/report-%(shard_num)05d-of-%(num_shards)05d2file_pattern:%apache_beam.io.parquetio._ParquetSink2\
beam:display_data:labelled:v1;
codecnone2codec:%apache_beam.io.parquetio._ParquetSink2Å
beam:display_data:labelled:v1ﬂ
schema•id: int64 not null
amount: int64
tags: list<item: string>
  child 0, item: string
placed: timestamp[us, tz=UTC]
price: decimal128(10, 2)
customer: struct<name: string, since: date32[day] not null>
  child 0, name: string
  child 1, since: date32[day] not null
attrs: map<string, int32>
  child 0, entries: struct<key: string not null, value: int32> not null
      child 0, key: string not null
      child 1, value: int322schema:%apache_beam.io.parquetio._ParquetSink:%ref_Environment_default_environment_1B*
python_typeapache_beam.io.iobase.Write
∞
<ref_AppliedPTransform_WriteReport-Write-WriteImpl-Extract_23Ô

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_13"&
Noneref_PCollection_PCollection_14*#WriteReport/Write/WriteImpl/Extract2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2t
beam:display_data:labelled:v1S
Transform Function<lambda>2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
¨
"ref_AppliedPTransform_WriteMade_33Ö
%
#beam:transform:generic_composite:v1(ref_AppliedPTransform_WriteMade-Write_34&
Noneref_PCollection_PCollection_21"&
Noneref_PCollection_PCollection_32*	WriteMade:%ref_Environment_default_environment_1B0
python_type!apache_beam.io.textio.WriteToText
¯
4ref_AppliedPTransform_WriteReport-Write-WriteImpl_12ø
%
#beam:transform:generic_composite:v1;ref_AppliedPTransform_WriteReport-Write-WriteImpl-DoOnce_13Dref_AppliedPTransform_WriteReport-Write-WriteImpl-InitializeWrite_18Mref_AppliedPTransform_WriteReport-Write-WriteImpl-WindowInto-WindowIntoFn-_19Aref_AppliedPTransform_WriteReport-Write-WriteImpl-WriteBundles_209ref_AppliedPTransform_WriteReport-Write-WriteImpl-Pair_21?ref_AppliedPTransform_WriteReport-Write-WriteImpl-GroupByKey_22<ref_AppliedPTransform_WriteReport-Write-WriteImpl-Extract_23@ref_AppliedPTransform_WriteReport-Write-WriteImpl-PreFinalize_24Bref_AppliedPTransform_WriteReport-Write-WriteImpl-FinalizeWrite_25%
Noneref_PCollection_PCollection_5"&
Noneref_PCollection_PCollection_16*WriteReport/Write/WriteImpl:%ref_Environment_default_environment_1B.
python_typeapache_beam.io.iobase.WriteImpl
ï
?ref_AppliedPTransform_WriteReport-Write-WriteImpl-GroupByKey_22—
 
beam:transform:group_by_key:v1&
Noneref_PCollection_PCollection_12"&
Noneref_PCollection_PCollection_13*&WriteReport/Write/WriteImpl/GroupByKeyB5
python_type&apache_beam.transforms.core.GroupByKey
õ
2ref_AppliedPTransform_ReadOrders-Read-EmitSource_5‰

beam:transform:pardo:v1%
Noneref_PCollection_PCollection_1"%
Noneref_PCollection_PCollection_2*ReadOrders/Read/EmitSource2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2t
beam:display_data:labelled:v1S
Transform Function<lambda>2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
ë
=ref_AppliedPTransform_WriteMade-Write-WriteImpl-GroupByKey_45œ
 
beam:transform:group_by_key:v1&
Noneref_PCollection_PCollection_28"&
Noneref_PCollection_PCollection_29*$WriteMade/Write/WriteImpl/GroupByKeyB5
python_type&apache_beam.transforms.core.GroupByKey
¶
7ref_AppliedPTransform_WriteMade-Write-WriteImpl-Pair_44Í

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_27"&
Noneref_PCollection_PCollection_28*WriteMade/Write/WriteImpl/Pair2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2t
beam:display_data:labelled:v1S
Transform Function<lambda>2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
¿
Eref_AppliedPTransform_WriteMade-Write-WriteImpl-DoOnce-Map-decode-_40ˆ

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_23"&
Noneref_PCollection_PCollection_24*,WriteMade/Write/WriteImpl/DoOnce/Map(decode)2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2r
beam:display_data:labelled:v1Q
Transform Functiondecode2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
µ
9ref_AppliedPTransform_WriteMade-Write-WriteImpl-DoOnce_36˜
"
 beam:transform:pickled_python:v1Aref_AppliedPTransform_WriteMade-Write-WriteImpl-DoOnce-Impulse_37Yref_AppliedPTransform_WriteMade-Write-WriteImpl-DoOnce-FlatMap-lambda-at-core-py-4094-_38Href_AppliedPTransform_WriteMade-Write-WriteImpl-DoOnce-MaybeReshuffle_39Eref_AppliedPTransform_WriteMade-Write-WriteImpl-DoOnce-Map-decode-_40"&
Noneref_PCollection_PCollection_24* WriteMade/Write/WriteImpl/DoOnce:%ref_Environment_default_environment_1B1
python_type"apache_beam.transforms.core.Create
„
Href_AppliedPTransform_WriteReport-ParDo-_RowDictionariesToArrowTable-_10ñ

beam:transform:pardo:v1%
Noneref_PCollection_PCollection_4"%
Noneref_PCollection_PCollection_5*/WriteReport/ParDo(_RowDictionariesToArrowTable)2ì
beam:display_data:labelled:v1r
Transform Function5apache_beam.io.parquetio._RowDictionariesToArrowTable2fn:!apache_beam.transforms.core.ParDo2ê
beam:display_data:labelled:v1o
row_group_buffer_size671088642row_group_buffer_size:5apache_beam.io.parquetio._RowDictionariesToArrowTable2x
beam:display_data:labelled:v1W
buffer_size10002buffer_size:5apache_beam.io.parquetio._RowDictionariesToArrowTable:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
≈
/ref_AppliedPTransform_ReadOrders-Read-Impulse_4ë

beam:transform:impulse:v1"%
Noneref_PCollection_PCollection_1*ReadOrders/Read/ImpulseB2
python_type#apache_beam.transforms.core.Impulse
Ì
Jref_AppliedPTransform_WriteReport-Write-WriteImpl-DoOnce-MaybeReshuffle_16û
"
 beam:transform:pickled_python:v1%
Noneref_PCollection_PCollection_7"%
Noneref_PCollection_PCollection_7*1WriteReport/Write/WriteImpl/DoOnce/MaybeReshuffle:%ref_Environment_default_environment_1BP
python_typeAapache_beam.transforms.core.Create.expand.<locals>.MaybeReshuffle
¥
=ref_AppliedPTransform_Make-FlatMap-lambda-at-core-py-4094-_30Ú

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_19"&
Noneref_PCollection_PCollection_20*&Make/FlatMap(<lambda at core.py:4094>)2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2t
beam:display_data:labelled:v1S
Transform Function<lambda>2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
Û
Zref_AppliedPTransform_ReadOrders-Read-SDFBoundedSourceReader-ParDo-SDFBoundedSourceDoFn-_7î

beam:transform:pardo:v1%
Noneref_PCollection_PCollection_2"%
Noneref_PCollection_PCollection_3*BReadOrders/Read/SDFBoundedSourceReader/ParDo(SDFBoundedSourceDoFn)2à
beam:display_data:labelled:v1g
Transform Function*apache_beam.io.iobase.SDFBoundedSourceDoFn2fn:!apache_beam.transforms.core.ParDo2É
beam:display_data:labelled:v1b
File Patterndata/in/orders/*.parquet2file_pattern:*apache_beam.io.iobase.SDFBoundedSourceDoFn2r
beam:display_data:labelled:v1Q
Compression Typeauto2compression:*apache_beam.io.iobase.SDFBoundedSourceDoFn2Ü
beam:display_data:labelled:v1e
source'apache_beam.io.parquetio._ParquetSource2source:*apache_beam.io.iobase.SDFBoundedSourceDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
¨
:ref_AppliedPTransform_WriteMade-Write-WriteImpl-Extract_46Ì

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_29"&
Noneref_PCollection_PCollection_30*!WriteMade/Write/WriteImpl/Extract2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2t
beam:display_data:labelled:v1S
Transform Function<lambda>2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
ß
@ref_AppliedPTransform_WriteReport-Write-WriteImpl-PreFinalize_24‚

beam:transform:pardo:v13
python_side_input0ref_PCollection_PCollection_94
python_side_input1ref_PCollection_PCollection_14%
Noneref_PCollection_PCollection_8"&
Noneref_PCollection_PCollection_15*'WriteReport/Write/WriteImpl/PreFinalize2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2y
beam:display_data:labelled:v1X
Transform Function_pre_finalize2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
ı

#ref_AppliedPTransform_WriteReport_9Õ

%
#beam:transform:generic_composite:v1Href_AppliedPTransform_WriteReport-ParDo-_RowDictionariesToArrowTable-_10*ref_AppliedPTransform_WriteReport-Write_11%
Noneref_PCollection_PCollection_4"&
Noneref_PCollection_PCollection_16*WriteReport2p
beam:display_data:labelled:v1O
compressionuncompressed2compression:%apache_beam.io.parquetio._ParquetSink2ô
beam:display_data:labelled:v1x
File Pattern3data/out/report-%(shard_num)05d-of-%(num_shards)05d2file_pattern:%apache_beam.io.parquetio._ParquetSink2\
beam:display_data:labelled:v1;
codecnone2codec:%apache_beam.io.parquetio._ParquetSink2Å
beam:display_data:labelled:v1ﬂ
schema•id: int64 not null
amount: int64
tags: list<item: string>
  child 0, item: string
placed: timestamp[us, tz=UTC]
price: decimal128(10, 2)
customer: struct<name: string, since: date32[day] not null>
  child 0, name: string
  child 1, since: date32[day] not null
attrs: map<string, int32>
  child 0, entries: struct<key: string not null, value: int32> not null
      child 0, key: string not null
      child 1, value: int322schema:%apache_beam.io.parquetio._ParquetSink2Ç
beam:display_data:labelled:v1a
row_group_buffer_size671088642row_group_buffer_size:'apache_beam.io.parquetio.WriteToParquet:%ref_Environment_default_environment_1B6
python_type'apache_beam.io.parquetio.WriteToParquet
Û
ref_AppliedPTransform_Notify_26œ

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_16"&
Noneref_PCollection_PCollection_17*Notify2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2q
beam:display_data:labelled:v1P
Transform Functionprint2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
Î
Href_AppliedPTransform_WriteMade-Write-WriteImpl-DoOnce-MaybeReshuffle_39û
"
 beam:transform:pickled_python:v1&
Noneref_PCollection_PCollection_23"&
Noneref_PCollection_PCollection_23*/WriteMade/Write/WriteImpl/DoOnce/MaybeReshuffle:%ref_Environment_default_environment_1BP
python_typeAapache_beam.transforms.core.Create.expand.<locals>.MaybeReshuffle
·
@ref_AppliedPTransform_WriteMade-Write-WriteImpl-FinalizeWrite_48ú

beam:transform:pardo:v14
python_side_input0ref_PCollection_PCollection_254
python_side_input1ref_PCollection_PCollection_304
python_side_input2ref_PCollection_PCollection_31&
Noneref_PCollection_PCollection_24"&
Noneref_PCollection_PCollection_32*'WriteMade/Write/WriteImpl/FinalizeWrite2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2{
beam:display_data:labelled:v1Z
Transform Function_finalize_write2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
ø
"ref_AppliedPTransform_ReadOrders_2ò
%
#beam:transform:generic_composite:v1'ref_AppliedPTransform_ReadOrders-Read_3?ref_AppliedPTransform_ReadOrders-ParDo-_ArrowTableToBeamRows-_8"%
Noneref_PCollection_PCollection_4*
ReadOrders2Ä
beam:display_data:labelled:v1_
File Patterndata/in/orders/*.parquet2file_pattern:'apache_beam.io.parquetio._ParquetSource2o
beam:display_data:labelled:v1N
Compression Typeauto2compression:'apache_beam.io.parquetio._ParquetSource:%ref_Environment_default_environment_1B7
python_type(apache_beam.io.parquetio.ReadFromParquet
¿
;ref_AppliedPTransform_WriteReport-Write-WriteImpl-DoOnce_13Ä
"
 beam:transform:pickled_python:v1Cref_AppliedPTransform_WriteReport-Write-WriteImpl-DoOnce-Impulse_14[ref_AppliedPTransform_WriteReport-Write-WriteImpl-DoOnce-FlatMap-lambda-at-core-py-4094-_15Jref_AppliedPTransform_WriteReport-Write-WriteImpl-DoOnce-MaybeReshuffle_16Gref_AppliedPTransform_WriteReport-Write-WriteImpl-DoOnce-Map-decode-_17"%
Noneref_PCollection_PCollection_8*"WriteReport/Write/WriteImpl/DoOnce:%ref_Environment_default_environment_1B1
python_type"apache_beam.transforms.core.Create
‰
Mref_AppliedPTransform_WriteReport-Write-WriteImpl-WindowInto-WindowIntoFn-_19í

beam:transform:window_into:v1%
Noneref_PCollection_PCollection_5"&
Noneref_PCollection_PCollection_10*4WriteReport/Write/WriteImpl/WindowInto(WindowIntoFn)2ã
beam:display_data:labelled:v1j
Transform Function(apache_beam.transforms.core.WindowIntoFn2fn:&apache_beam.transforms.core.WindowInto:%ref_Environment_default_environment_1B5
python_type&apache_beam.transforms.core.WindowInton
ref_PCollection_PCollection_17L
6Notify.Noneref_Coder_FastPrimitivesCoder_6"ref_Windowing_Windowing_1ì
ref_PCollection_PCollection_9r
243WriteReport/Write/WriteImpl/InitializeWrite.Noneref_Coder_FastPrimitivesCoder_6"ref_Windowing_Windowing_1Ñ
ref_PCollection_PCollection_2c
!26ReadOrders/Read/EmitSource.None!ref_Coder__MemoizingPickleCoder_3"ref_Windowing_Windowing_1ù
ref_PCollection_PCollection_10{
;52WriteReport/Write/WriteImpl/WindowInto(WindowIntoFn).Noneref_Coder_FastPrimitivesCoder_6"ref_Windowing_Windowing_1å
ref_PCollection_PCollection_16j
041WriteReport/Write/WriteImpl/FinalizeWrite.Noneref_Coder_StrUtf8Coder_12"ref_Windowing_Windowing_1à
ref_PCollection_PCollection_22f
/40WriteMade/Write/WriteImpl/DoOnce/Impulse.Noneref_Coder_BytesCoder_1"ref_Windowing_Windowing_1§
ref_PCollection_PCollection_7Ç
K68WriteReport/Write/WriteImpl/DoOnce/FlatMap(<lambda at core.py:4094>).Noneref_Coder_BytesCoder_1"ref_Windowing_Windowing_1å
ref_PCollection_PCollection_14j
*35WriteReport/Write/WriteImpl/Extract.Noneref_Coder_FastPrimitivesCoder_6"ref_Windowing_Windowing_1í
ref_PCollection_PCollection_25p
041WriteMade/Write/WriteImpl/InitializeWrite.Noneref_Coder_FastPrimitivesCoder_6"ref_Windowing_Windowing_1ï
ref_PCollection_PCollection_24s
344WriteMade/Write/WriteImpl/DoOnce/Map(decode).Noneref_Coder_FastPrimitivesCoder_6"ref_Windowing_Windowing_1ë
ref_PCollection_PCollection_11o
/40WriteReport/Write/WriteImpl/WriteBundles.Noneref_Coder_FastPrimitivesCoder_6"ref_Windowing_Windowing_1ñ
ref_PCollection_PCollection_8u
546WriteReport/Write/WriteImpl/DoOnce/Map(decode).Noneref_Coder_FastPrimitivesCoder_6"ref_Windowing_Windowing_1Ä
ref_PCollection_PCollection_12^
'32WriteReport/Write/WriteImpl/Pair.Noneref_Coder_TupleCoder_8"ref_Windowing_Windowing_1ó
ref_PCollection_PCollection_5v
647WriteReport/ParDo(_RowDictionariesToArrowTable).Noneref_Coder_FastPrimitivesCoder_6"ref_Windowing_Windowing_1£
ref_PCollection_PCollection_23Ä
I66WriteMade/Write/WriteImpl/DoOnce/FlatMap(<lambda at core.py:4094>).Noneref_Coder_BytesCoder_1"ref_Windowing_Windowing_1á
ref_PCollection_PCollection_13e
-38WriteReport/Write/WriteImpl/GroupByKey.Noneref_Coder_TupleCoder_10"ref_Windowing_Windowing_1~
ref_PCollection_PCollection_28\
%30WriteMade/Write/WriteImpl/Pair.Noneref_Coder_TupleCoder_8"ref_Windowing_Windowing_1é
ref_PCollection_PCollection_31l
,37WriteMade/Write/WriteImpl/PreFinalize.Noneref_Coder_FastPrimitivesCoder_6"ref_Windowing_Windowing_1Ü
ref_PCollection_PCollection_20d
-38Make/FlatMap(<lambda at core.py:4094>).Noneref_Coder_BytesCoder_1"ref_Windowing_Windowing_1ê
ref_PCollection_PCollection_15n
.39WriteReport/Write/WriteImpl/PreFinalize.Noneref_Coder_FastPrimitivesCoder_6"ref_Windowing_Windowing_1Ö
ref_PCollection_PCollection_29c
+36WriteMade/Write/WriteImpl/GroupByKey.Noneref_Coder_TupleCoder_10"ref_Windowing_Windowing_1â
ref_PCollection_PCollection_6h
142WriteReport/Write/WriteImpl/DoOnce/Impulse.Noneref_Coder_BytesCoder_1"ref_Windowing_Windowing_1ë
ref_PCollection_PCollection_26o
950WriteMade/Write/WriteImpl/WindowInto(WindowIntoFn).Noneref_Coder_RowCoder_13"ref_Windowing_Windowing_1è
ref_PCollection_PCollection_27m
-38WriteMade/Write/WriteImpl/WriteBundles.Noneref_Coder_FastPrimitivesCoder_6"ref_Windowing_Windowing_1v
ref_PCollection_PCollection_18T
13PushToService.Noneref_Coder_FastPrimitivesCoder_6"ref_Windowing_Windowing_1ä
ref_PCollection_PCollection_32h
.39WriteMade/Write/WriteImpl/FinalizeWrite.Noneref_Coder_StrUtf8Coder_12"ref_Windowing_Windowing_1´
ref_PCollection_PCollection_3â
I66ReadOrders/Read/SDFBoundedSourceReader/ParDo(SDFBoundedSourceDoFn).Noneref_Coder_FastPrimitivesCoder_6"ref_Windowing_Windowing_1v
ref_PCollection_PCollection_1U
23ReadOrders/Read/Impulse.Noneref_Coder_BytesCoder_1"ref_Windowing_Windowing_1l
ref_PCollection_PCollection_19J
12Make/Impulse.Noneref_Coder_BytesCoder_1"ref_Windowing_Windowing_1Ñ
ref_PCollection_PCollection_4c
.39ReadOrders/ParDo(_ArrowTableToBeamRows).Noneref_Coder_RowCoder_7"ref_Windowing_Windowing_1ä
ref_PCollection_PCollection_30h
(33WriteMade/Write/WriteImpl/Extract.Noneref_Coder_FastPrimitivesCoder_6"ref_Windowing_Windowing_1o
ref_PCollection_PCollection_21M
16Make/Map(decode).Noneref_Coder_RowCoder_13"ref_Windowing_Windowing_1ï
ref_Windowing_Windowing_1x
"
 beam:window_fn:global_windows:v1ref_Coder_GlobalWindowCoder_2": (08HZ%ref_Environment_default_environment_1"1
ref_Coder_BytesCoder_1

beam:coder:bytes:v1":
ref_Coder_StrUtf8Coder_12

beam:coder:string_utf8:v1"Ö
ref_Coder_TupleCoder_4k

beam:coder:kv:v14ref_Coder__SDFBoundedSourceWrapperRestrictionCoder_5ref_Coder_FastPrimitivesCoder_6"Y
ref_Coder_IterableCoder_11;

beam:coder:iterable:v1ref_Coder_FastPrimitivesCoder_6"Å
4ref_Coder__SDFBoundedSourceWrapperRestrictionCoder_5»
≈
beam:coder:pickled_python:v1§
°_SDFBoundedSourceWrapperRestrictionCoder$eNprYJ3qwwABPaKJBYnJGanxSamJuXqZ+UCUlFicOqVHIz7Yxc0pvzQvJTUlOL+0KDk1vCixoCC1KCi1uKQoM7kkMz/POT8ltWjK5CmajVNqpyTpAQDfhR+5"”
!ref_Coder__MemoizingPickleCoder_3≠
™
beam:coder:pickled_python:v1â
Ü_MemoizingPickleCoder$eNprYJ3qywABPZKJBYnJGanxSamJuXrJ+SmpRcVQakqPaLxvam5+ZlVmXnpAZnJ2TqozSGLK5CmajVNqp/RwJYN1FmdWpU7xFihO0gMAF9kf5w=="@
ref_Coder_GlobalWindowCoder_2

beam:coder:global_window:v1"Ö
ref_Coder_TupleCoder_8k

beam:coder:kv:v14ref_Coder_DeterministicFastPrimitivesCoderV2_v2_69_9ref_Coder_FastPrimitivesCoder_6"´
4ref_Coder_DeterministicFastPrimitivesCoderV2_v2_69_9Ú
Ô
beam:coder:pickled_python:v1Œ
ÀDeterministicFastPrimitivesCoderV2$eNptj7FLA1EMxuvQqYJIBXF1EF1a6CC4qijiUhx0DHnPtBf67u7xXu7gBkE3h2zG/9f2VERohi8hfL8v5HX4ebAz6EuPMKIvCBxhOfH1M6X800yPr0kolVxxFvY3mGWeuGThlvLVxvM4sw87e7MXO9V9aKr1KnRcLaFPsGKg4y3UL6N7sMAQHPrVHzCas18F+md02ekIslCEgI6C6clTYqEHinWSaT9/610Zw/Q21U287O6pMz2EJhMkCri5DwsOFFGKbO+6C+36S64rEFyaDtsZnF9Y4yZfP/Nvyw=="K
ref_Coder_RowCoder_73
1
beam:coder:row:v1


id

amount"p
ref_Coder_RowCoder_13W
U
beam:coder:row:v1@

id

amount$4a7170af-76a6-43ae-a3db-cb10267d45ab"ﬂ
ref_Coder_FastPrimitivesCoder_6ª
∏
beam:coder:pickled_python:v1ó
îFastPrimitivesCoder$eNprYJ2axgABPZKJBYnJGanxSamJuXrJ+SmpRcVQakqPsFticUlAUWZuZklmWWqxM0h4yuQpmo1Taqf08MenJebkJCUmZ8eD1U/JYOjhDshMzs5JRVGYVJykBwDOwCqZ"Å
ref_Coder_TupleCoder_10f

beam:coder:kv:v14ref_Coder_DeterministicFastPrimitivesCoderV2_v2_69_9ref_Coder_IterableCoder_11*>
%ref_Environment_default_environment_1beam:env:default:v1)ref_AppliedPTransform_AppliedPTransform_1")beam:requirement:pardo:splittable_dofn:v1