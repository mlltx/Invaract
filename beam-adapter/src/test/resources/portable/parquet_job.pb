
ио
ъ

Aref_AppliedPTransform_WriteReport-Write-WriteImpl-WriteBundles_22д


beam:transform:pardo:v14
python_side_input0ref_PCollection_PCollection_11&
Noneref_PCollection_PCollection_12"&
Noneref_PCollection_PCollection_13*(WriteReport/Write/WriteImpl/WriteBundles2Д
beam:display_data:labelled:v1c
Transform Function&apache_beam.io.iobase._WriteBundleDoFn2fn:!apache_beam.transforms.core.ParDo2p
beam:display_data:labelled:v1O
compressionuncompressed2compression:%apache_beam.io.parquetio._ParquetSink2в
beam:display_data:labelled:v1А
File Pattern;data/out/report-%(shard_num)05d-of-%(num_shards)05d.parquet2file_pattern:%apache_beam.io.parquetio._ParquetSink2\
beam:display_data:labelled:v1;
codecnone2codec:%apache_beam.io.parquetio._ParquetSink2Б
beam:display_data:labelled:v1▀
schemaеid: int64 not null
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
ю
[ref_AppliedPTransform_WriteReport-Write-WriteImpl-DoOnce-FlatMap-lambda-at-core-py-4094-_17О

beam:transform:pardo:v1%
Noneref_PCollection_PCollection_8"%
Noneref_PCollection_PCollection_9*DWriteReport/Write/WriteImpl/DoOnce/FlatMap(<lambda at core.py:4094>)2Н
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2t
beam:display_data:labelled:v1S
Transform Function<lambda>2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
е
>ref_AppliedPTransform_ReadOrders-Read-SDFBoundedSourceReader_6т
%
#beam:transform:generic_composite:v1Zref_AppliedPTransform_ReadOrders-Read-SDFBoundedSourceReader-ParDo-SDFBoundedSourceDoFn-_7%
Noneref_PCollection_PCollection_2"%
Noneref_PCollection_PCollection_3*&ReadOrders/Read/SDFBoundedSourceReader2Г
beam:display_data:labelled:v1b
File Patterndata/in/orders/*.parquet2file_pattern:*apache_beam.io.iobase.SDFBoundedSourceDoFn2r
beam:display_data:labelled:v1Q
Compression Typeauto2compression:*apache_beam.io.iobase.SDFBoundedSourceDoFn2И
beam:display_data:labelled:v1g
source'apache_beam.io.parquetio._ParquetSource2source:,apache_beam.io.iobase.SDFBoundedSourceReader:%ref_Environment_default_environment_1B;
python_type,apache_beam.io.iobase.SDFBoundedSourceReader
Х
?ref_AppliedPTransform_WriteReport-Write-WriteImpl-GroupByKey_24╤
 
beam:transform:group_by_key:v1&
Noneref_PCollection_PCollection_14"&
Noneref_PCollection_PCollection_15*&WriteReport/Write/WriteImpl/GroupByKeyB5
python_type&apache_beam.transforms.core.GroupByKey
╛
?ref_AppliedPTransform_ReadOrders-ParDo-_ArrowTableToBeamRows-_8·

beam:transform:pardo:v1%
Noneref_PCollection_PCollection_3"%
Noneref_PCollection_PCollection_4*'ReadOrders/ParDo(_ArrowTableToBeamRows)2М
beam:display_data:labelled:v1k
Transform Function.apache_beam.io.parquetio._ArrowTableToBeamRows2fn:!apache_beam.transforms.core.ParDo:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
┴
;ref_AppliedPTransform_WriteReport-Write-WriteImpl-DoOnce_15Б
"
 beam:transform:pickled_python:v1Cref_AppliedPTransform_WriteReport-Write-WriteImpl-DoOnce-Impulse_16[ref_AppliedPTransform_WriteReport-Write-WriteImpl-DoOnce-FlatMap-lambda-at-core-py-4094-_17Jref_AppliedPTransform_WriteReport-Write-WriteImpl-DoOnce-MaybeReshuffle_18Gref_AppliedPTransform_WriteReport-Write-WriteImpl-DoOnce-Map-decode-_19"&
Noneref_PCollection_PCollection_10*"WriteReport/Write/WriteImpl/DoOnce:%ref_Environment_default_environment_1B1
python_type"apache_beam.transforms.core.Create
э
Jref_AppliedPTransform_WriteReport-Write-WriteImpl-DoOnce-MaybeReshuffle_18Ю
"
 beam:transform:pickled_python:v1%
Noneref_PCollection_PCollection_9"%
Noneref_PCollection_PCollection_9*1WriteReport/Write/WriteImpl/DoOnce/MaybeReshuffle:%ref_Environment_default_environment_1BP
python_typeAapache_beam.transforms.core.Create.expand.<locals>.MaybeReshuffle
у
Href_AppliedPTransform_WriteReport-ParDo-_RowDictionariesToArrowTable-_12Ц

beam:transform:pardo:v1%
Noneref_PCollection_PCollection_6"%
Noneref_PCollection_PCollection_7*/WriteReport/ParDo(_RowDictionariesToArrowTable)2У
beam:display_data:labelled:v1r
Transform Function5apache_beam.io.parquetio._RowDictionariesToArrowTable2fn:!apache_beam.transforms.core.ParDo2Р
beam:display_data:labelled:v1o
row_group_buffer_size671088642row_group_buffer_size:5apache_beam.io.parquetio._RowDictionariesToArrowTable2x
beam:display_data:labelled:v1W
buffer_size10002buffer_size:5apache_beam.io.parquetio._RowDictionariesToArrowTable:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
й
@ref_AppliedPTransform_WriteReport-Write-WriteImpl-PreFinalize_26ф

beam:transform:pardo:v14
python_side_input0ref_PCollection_PCollection_114
python_side_input1ref_PCollection_PCollection_16&
Noneref_PCollection_PCollection_10"&
Noneref_PCollection_PCollection_17*'WriteReport/Write/WriteImpl/PreFinalize2Н
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2y
beam:display_data:labelled:v1X
Transform Function_pre_finalize2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
└
Dref_AppliedPTransform_WriteReport-Write-WriteImpl-InitializeWrite_20ў

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_10"&
Noneref_PCollection_PCollection_11*+WriteReport/Write/WriteImpl/InitializeWrite2Н
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2t
beam:display_data:labelled:v1S
Transform Function<lambda>2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
ф
Mref_AppliedPTransform_WriteReport-Write-WriteImpl-WindowInto-WindowIntoFn-_21Т

beam:transform:window_into:v1%
Noneref_PCollection_PCollection_7"&
Noneref_PCollection_PCollection_12*4WriteReport/Write/WriteImpl/WindowInto(WindowIntoFn)2Л
beam:display_data:labelled:v1j
Transform Function(apache_beam.transforms.core.WindowIntoFn2fn:&apache_beam.transforms.core.WindowInto:%ref_Environment_default_environment_1B5
python_type&apache_beam.transforms.core.WindowInto
°
4ref_AppliedPTransform_WriteReport-Write-WriteImpl_14┐
%
#beam:transform:generic_composite:v1;ref_AppliedPTransform_WriteReport-Write-WriteImpl-DoOnce_15Dref_AppliedPTransform_WriteReport-Write-WriteImpl-InitializeWrite_20Mref_AppliedPTransform_WriteReport-Write-WriteImpl-WindowInto-WindowIntoFn-_21Aref_AppliedPTransform_WriteReport-Write-WriteImpl-WriteBundles_229ref_AppliedPTransform_WriteReport-Write-WriteImpl-Pair_23?ref_AppliedPTransform_WriteReport-Write-WriteImpl-GroupByKey_24<ref_AppliedPTransform_WriteReport-Write-WriteImpl-Extract_25@ref_AppliedPTransform_WriteReport-Write-WriteImpl-PreFinalize_26Bref_AppliedPTransform_WriteReport-Write-WriteImpl-FinalizeWrite_27%
Noneref_PCollection_PCollection_7"&
Noneref_PCollection_PCollection_18*WriteReport/Write/WriteImpl:%ref_Environment_default_environment_1B.
python_typeapache_beam.io.iobase.WriteImpl
 

$ref_AppliedPTransform_WriteReport_11╓

%
#beam:transform:generic_composite:v1Href_AppliedPTransform_WriteReport-ParDo-_RowDictionariesToArrowTable-_12*ref_AppliedPTransform_WriteReport-Write_13%
Noneref_PCollection_PCollection_6"&
Noneref_PCollection_PCollection_18*WriteReport2p
beam:display_data:labelled:v1O
compressionuncompressed2compression:%apache_beam.io.parquetio._ParquetSink2в
beam:display_data:labelled:v1А
File Pattern;data/out/report-%(shard_num)05d-of-%(num_shards)05d.parquet2file_pattern:%apache_beam.io.parquetio._ParquetSink2\
beam:display_data:labelled:v1;
codecnone2codec:%apache_beam.io.parquetio._ParquetSink2Б
beam:display_data:labelled:v1▀
schemaеid: int64 not null
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
      child 1, value: int322schema:%apache_beam.io.parquetio._ParquetSink2В
beam:display_data:labelled:v1a
row_group_buffer_size671088642row_group_buffer_size:'apache_beam.io.parquetio.WriteToParquet:%ref_Environment_default_environment_1B6
python_type'apache_beam.io.parquetio.WriteToParquet
ь
Cref_AppliedPTransform_WriteReport-Write-WriteImpl-DoOnce-Impulse_16д

beam:transform:impulse:v1"%
Noneref_PCollection_PCollection_8**WriteReport/Write/WriteImpl/DoOnce/ImpulseB2
python_type#apache_beam.transforms.core.Impulse
┼
/ref_AppliedPTransform_ReadOrders-Read-Impulse_4С

beam:transform:impulse:v1"%
Noneref_PCollection_PCollection_1*ReadOrders/Read/ImpulseB2
python_type#apache_beam.transforms.core.Impulse
ц
'ref_AppliedPTransform_ReadOrders-Read_3║

beam:transform:read:v1/ref_AppliedPTransform_ReadOrders-Read-Impulse_42ref_AppliedPTransform_ReadOrders-Read-EmitSource_5>ref_AppliedPTransform_ReadOrders-Read-SDFBoundedSourceReader_6"%
Noneref_PCollection_PCollection_3*ReadOrders/Read2{
beam:display_data:labelled:v1Z
Read Source'apache_beam.io.parquetio._ParquetSource2source:apache_beam.io.iobase.Read2А
beam:display_data:labelled:v1_
File Patterndata/in/orders/*.parquet2file_pattern:'apache_beam.io.parquetio._ParquetSource2o
beam:display_data:labelled:v1N
Compression Typeauto2compression:'apache_beam.io.parquetio._ParquetSource:%ref_Environment_default_environment_1B)
python_typeapache_beam.io.iobase.Read
х
Bref_AppliedPTransform_WriteReport-Write-WriteImpl-FinalizeWrite_27Ю

beam:transform:pardo:v14
python_side_input0ref_PCollection_PCollection_114
python_side_input1ref_PCollection_PCollection_164
python_side_input2ref_PCollection_PCollection_17&
Noneref_PCollection_PCollection_10"&
Noneref_PCollection_PCollection_18*)WriteReport/Write/WriteImpl/FinalizeWrite2Н
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2{
beam:display_data:labelled:v1Z
Transform Function_finalize_write2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
├
Gref_AppliedPTransform_WriteReport-Write-WriteImpl-DoOnce-Map-decode-_19ў

beam:transform:pardo:v1%
Noneref_PCollection_PCollection_9"&
Noneref_PCollection_PCollection_10*.WriteReport/Write/WriteImpl/DoOnce/Map(decode)2Н
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2r
beam:display_data:labelled:v1Q
Transform Functiondecode2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
є
Zref_AppliedPTransform_ReadOrders-Read-SDFBoundedSourceReader-ParDo-SDFBoundedSourceDoFn-_7Ф

beam:transform:pardo:v1%
Noneref_PCollection_PCollection_2"%
Noneref_PCollection_PCollection_3*BReadOrders/Read/SDFBoundedSourceReader/ParDo(SDFBoundedSourceDoFn)2И
beam:display_data:labelled:v1g
Transform Function*apache_beam.io.iobase.SDFBoundedSourceDoFn2fn:!apache_beam.transforms.core.ParDo2Г
beam:display_data:labelled:v1b
File Patterndata/in/orders/*.parquet2file_pattern:*apache_beam.io.iobase.SDFBoundedSourceDoFn2r
beam:display_data:labelled:v1Q
Compression Typeauto2compression:*apache_beam.io.iobase.SDFBoundedSourceDoFn2Ж
beam:display_data:labelled:v1e
source'apache_beam.io.parquetio._ParquetSource2source:*apache_beam.io.iobase.SDFBoundedSourceDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
Р
)ref_AppliedPTransform_AppliedPTransform_1т
 "ref_AppliedPTransform_ReadOrders_2 ref_AppliedPTransform_Positive_9 ref_AppliedPTransform_Project_10$ref_AppliedPTransform_WriteReport_11*)ref_AppliedPTransform_AppliedPTransform_1:%ref_Environment_default_environment_1
ў
 ref_AppliedPTransform_Positive_9╥

beam:transform:pardo:v1%
Noneref_PCollection_PCollection_4"%
Noneref_PCollection_PCollection_5*Positive2Н
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2t
beam:display_data:labelled:v1S
Transform Function<lambda>2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
Ы
2ref_AppliedPTransform_ReadOrders-Read-EmitSource_5ф

beam:transform:pardo:v1%
Noneref_PCollection_PCollection_1"%
Noneref_PCollection_PCollection_2*ReadOrders/Read/EmitSource2Н
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2t
beam:display_data:labelled:v1S
Transform Function<lambda>2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
Ў
 ref_AppliedPTransform_Project_10╤

beam:transform:pardo:v1%
Noneref_PCollection_PCollection_5"%
Noneref_PCollection_PCollection_6*Project2Н
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2t
beam:display_data:labelled:v1S
Transform Function<lambda>2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
н

*ref_AppliedPTransform_WriteReport-Write_13■	
%
#beam:transform:generic_composite:v14ref_AppliedPTransform_WriteReport-Write-WriteImpl_14%
Noneref_PCollection_PCollection_7"&
Noneref_PCollection_PCollection_18*WriteReport/Write2q
beam:display_data:labelled:v1P
sink%apache_beam.io.parquetio._ParquetSink2sink:apache_beam.io.iobase.Write2p
beam:display_data:labelled:v1O
compressionuncompressed2compression:%apache_beam.io.parquetio._ParquetSink2в
beam:display_data:labelled:v1А
File Pattern;data/out/report-%(shard_num)05d-of-%(num_shards)05d.parquet2file_pattern:%apache_beam.io.parquetio._ParquetSink2\
beam:display_data:labelled:v1;
codecnone2codec:%apache_beam.io.parquetio._ParquetSink2Б
beam:display_data:labelled:v1▀
schemaеid: int64 not null
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
┐
"ref_AppliedPTransform_ReadOrders_2Ш
%
#beam:transform:generic_composite:v1'ref_AppliedPTransform_ReadOrders-Read_3?ref_AppliedPTransform_ReadOrders-ParDo-_ArrowTableToBeamRows-_8"%
Noneref_PCollection_PCollection_4*
ReadOrders2А
beam:display_data:labelled:v1_
File Patterndata/in/orders/*.parquet2file_pattern:'apache_beam.io.parquetio._ParquetSource2o
beam:display_data:labelled:v1N
Compression Typeauto2compression:'apache_beam.io.parquetio._ParquetSource:%ref_Environment_default_environment_1B7
python_type(apache_beam.io.parquetio.ReadFromParquet
к
9ref_AppliedPTransform_WriteReport-Write-WriteImpl-Pair_23ь

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_13"&
Noneref_PCollection_PCollection_14* WriteReport/Write/WriteImpl/Pair2Н
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2t
beam:display_data:labelled:v1S
Transform Function<lambda>2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
░
<ref_AppliedPTransform_WriteReport-Write-WriteImpl-Extract_25я

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_15"&
Noneref_PCollection_PCollection_16*#WriteReport/Write/WriteImpl/Extract2Н
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2t
beam:display_data:labelled:v1S
Transform Function<lambda>2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDoЗ
ref_PCollection_PCollection_15e
-38WriteReport/Write/WriteImpl/GroupByKey.Noneref_Coder_TupleCoder_11"ref_Windowing_Windowing_1Р
ref_PCollection_PCollection_17n
.39WriteReport/Write/WriteImpl/PreFinalize.Noneref_Coder_FastPrimitivesCoder_6"ref_Windowing_Windowing_1c
ref_PCollection_PCollection_6B
7Project.Noneref_Coder_RowCoder_8"ref_Windowing_Windowing_1д
ref_PCollection_PCollection_9В
K68WriteReport/Write/WriteImpl/DoOnce/FlatMap(<lambda at core.py:4094>).Noneref_Coder_BytesCoder_1"ref_Windowing_Windowing_1Ч
ref_PCollection_PCollection_10u
546WriteReport/Write/WriteImpl/DoOnce/Map(decode).Noneref_Coder_FastPrimitivesCoder_6"ref_Windowing_Windowing_1Д
ref_PCollection_PCollection_2c
!26ReadOrders/Read/EmitSource.None!ref_Coder__MemoizingPickleCoder_3"ref_Windowing_Windowing_1М
ref_PCollection_PCollection_18j
041WriteReport/Write/WriteImpl/FinalizeWrite.Noneref_Coder_StrUtf8Coder_13"ref_Windowing_Windowing_1М
ref_PCollection_PCollection_16j
*35WriteReport/Write/WriteImpl/Extract.Noneref_Coder_FastPrimitivesCoder_6"ref_Windowing_Windowing_1л
ref_PCollection_PCollection_3Й
I66ReadOrders/Read/SDFBoundedSourceReader/ParDo(SDFBoundedSourceDoFn).Noneref_Coder_FastPrimitivesCoder_6"ref_Windowing_Windowing_1Ч
ref_PCollection_PCollection_7v
647WriteReport/ParDo(_RowDictionariesToArrowTable).Noneref_Coder_FastPrimitivesCoder_6"ref_Windowing_Windowing_1А
ref_PCollection_PCollection_14^
'32WriteReport/Write/WriteImpl/Pair.Noneref_Coder_TupleCoder_9"ref_Windowing_Windowing_1v
ref_PCollection_PCollection_1U
23ReadOrders/Read/Impulse.Noneref_Coder_BytesCoder_1"ref_Windowing_Windowing_1Ф
ref_PCollection_PCollection_11r
243WriteReport/Write/WriteImpl/InitializeWrite.Noneref_Coder_FastPrimitivesCoder_6"ref_Windowing_Windowing_1Д
ref_PCollection_PCollection_4c
.39ReadOrders/ParDo(_ArrowTableToBeamRows).Noneref_Coder_RowCoder_7"ref_Windowing_Windowing_1Э
ref_PCollection_PCollection_12{
;52WriteReport/Write/WriteImpl/WindowInto(WindowIntoFn).Noneref_Coder_FastPrimitivesCoder_6"ref_Windowing_Windowing_1d
ref_PCollection_PCollection_5C
8Positive.Noneref_Coder_RowCoder_7"ref_Windowing_Windowing_1Й
ref_PCollection_PCollection_8h
142WriteReport/Write/WriteImpl/DoOnce/Impulse.Noneref_Coder_BytesCoder_1"ref_Windowing_Windowing_1С
ref_PCollection_PCollection_13o
/40WriteReport/Write/WriteImpl/WriteBundles.Noneref_Coder_FastPrimitivesCoder_6"ref_Windowing_Windowing_1Х
ref_Windowing_Windowing_1x
"
 beam:window_fn:global_windows:v1ref_Coder_GlobalWindowCoder_2": (08HZ%ref_Environment_default_environment_1"1
ref_Coder_BytesCoder_1

beam:coder:bytes:v1"Е
ref_Coder_TupleCoder_4k

beam:coder:kv:v14ref_Coder__SDFBoundedSourceWrapperRestrictionCoder_5ref_Coder_FastPrimitivesCoder_6"В
ref_Coder_TupleCoder_11g

beam:coder:kv:v15ref_Coder_DeterministicFastPrimitivesCoderV2_v2_69_10ref_Coder_IterableCoder_12"м
5ref_Coder_DeterministicFastPrimitivesCoderV2_v2_69_10Є
я
beam:coder:pickled_python:v1╬
╦DeterministicFastPrimitivesCoderV2$eNptj7FLA1EMxuvQqYJIBXF1EF1a6CC4qijiUhx0DHnPtBf67u7xXu7gBkE3h2zG/9f2VERohi8hfL8v5HX4ebAz6EuPMKIvCBxhOfH1M6X800yPr0kolVxxFvY3mGWeuGThlvLVxvM4sw87e7MXO9V9aKr1KnRcLaFPsGKg4y3UL6N7sMAQHPrVHzCas18F+md02ekIslCEgI6C6clTYqEHinWSaT9/610Zw/Q21U287O6pMz2EJhMkCri5DwsOFFGKbO+6C+36S64rEFyaDtsZnF9Y4yZfP/Nvyw=="K
ref_Coder_RowCoder_73
1
beam:coder:row:v1


id

amount"╙
!ref_Coder__MemoizingPickleCoder_3н
к
beam:coder:pickled_python:v1Й
Ж_MemoizingPickleCoder$eNprYJ3qywABPZKJBYnJGanxSamJuXrJ+SmpRcVQakqPaLxvam5+ZlVmXnpAZnJ2TqozSGLK5CmajVNqp/RwJYN1FmdWpU7xFihO0gMAF9kf5w=="Б
4ref_Coder__SDFBoundedSourceWrapperRestrictionCoder_5╚
┼
beam:coder:pickled_python:v1д
б_SDFBoundedSourceWrapperRestrictionCoder$eNprYJ3qwwABPaKJBYnJGanxSamJuXqZ+UCUlFicOqVHIz7Yxc0pvzQvJTUlOL+0KDk1vCixoCC1KCi1uKQoM7kkMz/POT8ltWjK5CmajVNqpyTpAQDfhR+5"@
ref_Coder_GlobalWindowCoder_2

beam:coder:global_window:v1"
ref_Coder_RowCoder_8g
e
beam:coder:row:v1P

id

amount

tags
$47275994-de3e-4728-911d-05e2fff5cdba"Ж
ref_Coder_TupleCoder_9l

beam:coder:kv:v15ref_Coder_DeterministicFastPrimitivesCoderV2_v2_69_10ref_Coder_FastPrimitivesCoder_6"Y
ref_Coder_IterableCoder_12;

beam:coder:iterable:v1ref_Coder_FastPrimitivesCoder_6"▀
ref_Coder_FastPrimitivesCoder_6╗
╕
beam:coder:pickled_python:v1Ч
ФFastPrimitivesCoder$eNprYJ2axgABPZKJBYnJGanxSamJuXrJ+SmpRcVQakqPsFticUlAUWZuZklmWWqxM0h4yuQpmo1Taqf08MenJebkJCUmZ8eD1U/JYOjhDshMzs5JRVGYVJykBwDOwCqZ":
ref_Coder_StrUtf8Coder_13

beam:coder:string_utf8:v1*>
%ref_Environment_default_environment_1beam:env:default:v1)ref_AppliedPTransform_AppliedPTransform_1")beam:requirement:pardo:splittable_dofn:v1