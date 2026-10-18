
û
”
Fref_AppliedPTransform_ReadOrders-ParDo-_ArrowTableToRowDictionaries-_8à

beam:transform:pardo:v1%
Noneref_PCollection_PCollection_3"%
Noneref_PCollection_PCollection_4*.ReadOrders/ParDo(_ArrowTableToRowDictionaries)2ì
beam:display_data:labelled:v1r
Transform Function5apache_beam.io.parquetio._ArrowTableToRowDictionaries2fn:!apache_beam.transforms.core.ParDo:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
™
9ref_AppliedPTransform_WriteReport-Write-WriteImpl-Pair_22Ï

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_12"&
Noneref_PCollection_PCollection_13* WriteReport/Write/WriteImpl/Pair2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2t
beam:display_data:labelled:v1S
Transform Function<lambda>2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
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
¯
4ref_AppliedPTransform_WriteReport-Write-WriteImpl_13ø
%
#beam:transform:generic_composite:v1;ref_AppliedPTransform_WriteReport-Write-WriteImpl-DoOnce_14Dref_AppliedPTransform_WriteReport-Write-WriteImpl-InitializeWrite_19Mref_AppliedPTransform_WriteReport-Write-WriteImpl-WindowInto-WindowIntoFn-_20Aref_AppliedPTransform_WriteReport-Write-WriteImpl-WriteBundles_219ref_AppliedPTransform_WriteReport-Write-WriteImpl-Pair_22?ref_AppliedPTransform_WriteReport-Write-WriteImpl-GroupByKey_23<ref_AppliedPTransform_WriteReport-Write-WriteImpl-Extract_24@ref_AppliedPTransform_WriteReport-Write-WriteImpl-PreFinalize_25Bref_AppliedPTransform_WriteReport-Write-WriteImpl-FinalizeWrite_26%
Noneref_PCollection_PCollection_6"&
Noneref_PCollection_PCollection_17*WriteReport/Write/WriteImpl:%ref_Environment_default_environment_1B.
python_typeapache_beam.io.iobase.WriteImpl
¬
Gref_AppliedPTransform_WriteReport-Write-WriteImpl-DoOnce-Map-decode-_18ˆ

beam:transform:pardo:v1%
Noneref_PCollection_PCollection_8"%
Noneref_PCollection_PCollection_9*.WriteReport/Write/WriteImpl/DoOnce/Map(decode)2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2r
beam:display_data:labelled:v1Q
Transform Functiondecode2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
Ì
Jref_AppliedPTransform_WriteReport-Write-WriteImpl-DoOnce-MaybeReshuffle_17û
"
 beam:transform:pickled_python:v1%
Noneref_PCollection_PCollection_8"%
Noneref_PCollection_PCollection_8*1WriteReport/Write/WriteImpl/DoOnce/MaybeReshuffle:%ref_Environment_default_environment_1BP
python_typeAapache_beam.transforms.core.Create.expand.<locals>.MaybeReshuffle
Ó
[ref_AppliedPTransform_WriteReport-Write-WriteImpl-DoOnce-FlatMap-lambda-at-core-py-4094-_16é

beam:transform:pardo:v1%
Noneref_PCollection_PCollection_7"%
Noneref_PCollection_PCollection_8*DWriteReport/Write/WriteImpl/DoOnce/FlatMap(<lambda at core.py:4094>)2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2t
beam:display_data:labelled:v1S
Transform Function<lambda>2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
ø
Dref_AppliedPTransform_WriteReport-Write-WriteImpl-InitializeWrite_19ˆ

beam:transform:pardo:v1%
Noneref_PCollection_PCollection_9"&
Noneref_PCollection_PCollection_10*+WriteReport/Write/WriteImpl/InitializeWrite2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2t
beam:display_data:labelled:v1S
Transform Function<lambda>2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
˜
$ref_AppliedPTransform_WriteReport_10Œ
%
#beam:transform:generic_composite:v1Href_AppliedPTransform_WriteReport-ParDo-_RowDictionariesToArrowTable-_11*ref_AppliedPTransform_WriteReport-Write_12%
Noneref_PCollection_PCollection_5"&
Noneref_PCollection_PCollection_17*WriteReport2p
beam:display_data:labelled:v1O
compressionuncompressed2compression:%apache_beam.io.parquetio._ParquetSink2¢
beam:display_data:labelled:v1Ä
File Pattern;data/out/report-%(shard_num)05d-of-%(num_shards)05d.parquet2file_pattern:%apache_beam.io.parquetio._ParquetSink2\
beam:display_data:labelled:v1;
codecnone2codec:%apache_beam.io.parquetio._ParquetSink2z
beam:display_data:labelled:v1Y
schema id: int64 not null
amount: int642schema:%apache_beam.io.parquetio._ParquetSink2Ç
beam:display_data:labelled:v1a
row_group_buffer_size671088642row_group_buffer_size:'apache_beam.io.parquetio.WriteToParquet:%ref_Environment_default_environment_1B6
python_type'apache_beam.io.parquetio.WriteToParquet
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
®
@ref_AppliedPTransform_WriteReport-Write-WriteImpl-PreFinalize_25„

beam:transform:pardo:v14
python_side_input0ref_PCollection_PCollection_104
python_side_input1ref_PCollection_PCollection_15%
Noneref_PCollection_PCollection_9"&
Noneref_PCollection_PCollection_16*'WriteReport/Write/WriteImpl/PreFinalize2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2y
beam:display_data:labelled:v1X
Transform Function_pre_finalize2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
≈
/ref_AppliedPTransform_ReadOrders-Read-Impulse_4ë

beam:transform:impulse:v1"%
Noneref_PCollection_PCollection_1*ReadOrders/Read/ImpulseB2
python_type#apache_beam.transforms.core.Impulse
¿
;ref_AppliedPTransform_WriteReport-Write-WriteImpl-DoOnce_14Ä
"
 beam:transform:pickled_python:v1Cref_AppliedPTransform_WriteReport-Write-WriteImpl-DoOnce-Impulse_15[ref_AppliedPTransform_WriteReport-Write-WriteImpl-DoOnce-FlatMap-lambda-at-core-py-4094-_16Jref_AppliedPTransform_WriteReport-Write-WriteImpl-DoOnce-MaybeReshuffle_17Gref_AppliedPTransform_WriteReport-Write-WriteImpl-DoOnce-Map-decode-_18"%
Noneref_PCollection_PCollection_9*"WriteReport/Write/WriteImpl/DoOnce:%ref_Environment_default_environment_1B1
python_type"apache_beam.transforms.core.Create
‚
Aref_AppliedPTransform_WriteReport-Write-WriteImpl-WriteBundles_21ú

beam:transform:pardo:v14
python_side_input0ref_PCollection_PCollection_10&
Noneref_PCollection_PCollection_11"&
Noneref_PCollection_PCollection_12*(WriteReport/Write/WriteImpl/WriteBundles2Ñ
beam:display_data:labelled:v1c
Transform Function&apache_beam.io.iobase._WriteBundleDoFn2fn:!apache_beam.transforms.core.ParDo2p
beam:display_data:labelled:v1O
compressionuncompressed2compression:%apache_beam.io.parquetio._ParquetSink2¢
beam:display_data:labelled:v1Ä
File Pattern;data/out/report-%(shard_num)05d-of-%(num_shards)05d.parquet2file_pattern:%apache_beam.io.parquetio._ParquetSink2\
beam:display_data:labelled:v1;
codecnone2codec:%apache_beam.io.parquetio._ParquetSink2z
beam:display_data:labelled:v1Y
schema id: int64 not null
amount: int642schema:%apache_beam.io.parquetio._ParquetSink:%ref_Environment_default_environment_1B0
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
∞
<ref_AppliedPTransform_WriteReport-Write-WriteImpl-Extract_24Ô

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_14"&
Noneref_PCollection_PCollection_15*#WriteReport/Write/WriteImpl/Extract2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2t
beam:display_data:labelled:v1S
Transform Function<lambda>2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
Ï
Cref_AppliedPTransform_WriteReport-Write-WriteImpl-DoOnce-Impulse_15§

beam:transform:impulse:v1"%
Noneref_PCollection_PCollection_7**WriteReport/Write/WriteImpl/DoOnce/ImpulseB2
python_type#apache_beam.transforms.core.Impulse
Ó
)ref_AppliedPTransform_AppliedPTransform_1¿
 "ref_AppliedPTransform_ReadOrders_2 ref_AppliedPTransform_Positive_9$ref_AppliedPTransform_WriteReport_10*)ref_AppliedPTransform_AppliedPTransform_1:%ref_Environment_default_environment_1
„
Href_AppliedPTransform_WriteReport-ParDo-_RowDictionariesToArrowTable-_11ñ

beam:transform:pardo:v1%
Noneref_PCollection_PCollection_5"%
Noneref_PCollection_PCollection_6*/WriteReport/ParDo(_RowDictionariesToArrowTable)2ì
beam:display_data:labelled:v1r
Transform Function5apache_beam.io.parquetio._RowDictionariesToArrowTable2fn:!apache_beam.transforms.core.ParDo2ê
beam:display_data:labelled:v1o
row_group_buffer_size671088642row_group_buffer_size:5apache_beam.io.parquetio._RowDictionariesToArrowTable2x
beam:display_data:labelled:v1W
buffer_size10002buffer_size:5apache_beam.io.parquetio._RowDictionariesToArrowTable:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
‰
Mref_AppliedPTransform_WriteReport-Write-WriteImpl-WindowInto-WindowIntoFn-_20í

beam:transform:window_into:v1%
Noneref_PCollection_PCollection_6"&
Noneref_PCollection_PCollection_11*4WriteReport/Write/WriteImpl/WindowInto(WindowIntoFn)2ã
beam:display_data:labelled:v1j
Transform Function(apache_beam.transforms.core.WindowIntoFn2fn:&apache_beam.transforms.core.WindowInto:%ref_Environment_default_environment_1B5
python_type&apache_beam.transforms.core.WindowInto
˜
 ref_AppliedPTransform_Positive_9“

beam:transform:pardo:v1%
Noneref_PCollection_PCollection_4"%
Noneref_PCollection_PCollection_5*Positive2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2t
beam:display_data:labelled:v1S
Transform Function<lambda>2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
‰
Bref_AppliedPTransform_WriteReport-Write-WriteImpl-FinalizeWrite_26ù

beam:transform:pardo:v14
python_side_input0ref_PCollection_PCollection_104
python_side_input1ref_PCollection_PCollection_154
python_side_input2ref_PCollection_PCollection_16%
Noneref_PCollection_PCollection_9"&
Noneref_PCollection_PCollection_17*)WriteReport/Write/WriteImpl/FinalizeWrite2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2{
beam:display_data:labelled:v1Z
Transform Function_finalize_write2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
•
*ref_AppliedPTransform_WriteReport-Write_12ˆ
%
#beam:transform:generic_composite:v14ref_AppliedPTransform_WriteReport-Write-WriteImpl_13%
Noneref_PCollection_PCollection_6"&
Noneref_PCollection_PCollection_17*WriteReport/Write2q
beam:display_data:labelled:v1P
sink%apache_beam.io.parquetio._ParquetSink2sink:apache_beam.io.iobase.Write2p
beam:display_data:labelled:v1O
compressionuncompressed2compression:%apache_beam.io.parquetio._ParquetSink2¢
beam:display_data:labelled:v1Ä
File Pattern;data/out/report-%(shard_num)05d-of-%(num_shards)05d.parquet2file_pattern:%apache_beam.io.parquetio._ParquetSink2\
beam:display_data:labelled:v1;
codecnone2codec:%apache_beam.io.parquetio._ParquetSink2z
beam:display_data:labelled:v1Y
schema id: int64 not null
amount: int642schema:%apache_beam.io.parquetio._ParquetSink:%ref_Environment_default_environment_1B*
python_typeapache_beam.io.iobase.Write
∆
"ref_AppliedPTransform_ReadOrders_2ü
%
#beam:transform:generic_composite:v1'ref_AppliedPTransform_ReadOrders-Read_3Fref_AppliedPTransform_ReadOrders-ParDo-_ArrowTableToRowDictionaries-_8"%
Noneref_PCollection_PCollection_4*
ReadOrders2Ä
beam:display_data:labelled:v1_
File Patterndata/in/orders/*.parquet2file_pattern:'apache_beam.io.parquetio._ParquetSource2o
beam:display_data:labelled:v1N
Compression Typeauto2compression:'apache_beam.io.parquetio._ParquetSource:%ref_Environment_default_environment_1B7
python_type(apache_beam.io.parquetio.ReadFromParquet
ï
?ref_AppliedPTransform_WriteReport-Write-WriteImpl-GroupByKey_23—
 
beam:transform:group_by_key:v1&
Noneref_PCollection_PCollection_13"&
Noneref_PCollection_PCollection_14*&WriteReport/Write/WriteImpl/GroupByKeyB5
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
python_type!apache_beam.transforms.core.ParDoå
ref_PCollection_PCollection_15j
*35WriteReport/Write/WriteImpl/Extract.Noneref_Coder_FastPrimitivesCoder_6"ref_Windowing_Windowing_1å
ref_PCollection_PCollection_17j
041WriteReport/Write/WriteImpl/FinalizeWrite.Noneref_Coder_StrUtf8Coder_11"ref_Windowing_Windowing_1ó
ref_PCollection_PCollection_6v
647WriteReport/ParDo(_RowDictionariesToArrowTable).Noneref_Coder_FastPrimitivesCoder_6"ref_Windowing_Windowing_1ñ
ref_PCollection_PCollection_9u
546WriteReport/Write/WriteImpl/DoOnce/Map(decode).Noneref_Coder_FastPrimitivesCoder_6"ref_Windowing_Windowing_1Ñ
ref_PCollection_PCollection_2c
!26ReadOrders/Read/EmitSource.None!ref_Coder__MemoizingPickleCoder_3"ref_Windowing_Windowing_1î
ref_PCollection_PCollection_10r
243WriteReport/Write/WriteImpl/InitializeWrite.Noneref_Coder_FastPrimitivesCoder_6"ref_Windowing_Windowing_1ê
ref_PCollection_PCollection_16n
.39WriteReport/Write/WriteImpl/PreFinalize.Noneref_Coder_FastPrimitivesCoder_6"ref_Windowing_Windowing_1´
ref_PCollection_PCollection_3â
I66ReadOrders/Read/SDFBoundedSourceReader/ParDo(SDFBoundedSourceDoFn).Noneref_Coder_FastPrimitivesCoder_6"ref_Windowing_Windowing_1â
ref_PCollection_PCollection_7h
142WriteReport/Write/WriteImpl/DoOnce/Impulse.Noneref_Coder_BytesCoder_1"ref_Windowing_Windowing_1Ü
ref_PCollection_PCollection_14d
-38WriteReport/Write/WriteImpl/GroupByKey.Noneref_Coder_TupleCoder_9"ref_Windowing_Windowing_1v
ref_PCollection_PCollection_1U
23ReadOrders/Read/Impulse.Noneref_Coder_BytesCoder_1"ref_Windowing_Windowing_1ù
ref_PCollection_PCollection_11{
;52WriteReport/Write/WriteImpl/WindowInto(WindowIntoFn).Noneref_Coder_FastPrimitivesCoder_6"ref_Windowing_Windowing_1ñ
ref_PCollection_PCollection_4u
546ReadOrders/ParDo(_ArrowTableToRowDictionaries).Noneref_Coder_FastPrimitivesCoder_6"ref_Windowing_Windowing_1ë
ref_PCollection_PCollection_12o
/40WriteReport/Write/WriteImpl/WriteBundles.Noneref_Coder_FastPrimitivesCoder_6"ref_Windowing_Windowing_1o
ref_PCollection_PCollection_5N
8Positive.Noneref_Coder_FastPrimitivesCoder_6"ref_Windowing_Windowing_1§
ref_PCollection_PCollection_8Ç
K68WriteReport/Write/WriteImpl/DoOnce/FlatMap(<lambda at core.py:4094>).Noneref_Coder_BytesCoder_1"ref_Windowing_Windowing_1Ä
ref_PCollection_PCollection_13^
'32WriteReport/Write/WriteImpl/Pair.Noneref_Coder_TupleCoder_7"ref_Windowing_Windowing_1ï
ref_Windowing_Windowing_1x
"
 beam:window_fn:global_windows:v1ref_Coder_GlobalWindowCoder_2": (08HZ%ref_Environment_default_environment_1"1
ref_Coder_BytesCoder_1

beam:coder:bytes:v1"Ö
ref_Coder_TupleCoder_4k

beam:coder:kv:v14ref_Coder__SDFBoundedSourceWrapperRestrictionCoder_5ref_Coder_FastPrimitivesCoder_6"´
4ref_Coder_DeterministicFastPrimitivesCoderV2_v2_69_8Ú
Ô
beam:coder:pickled_python:v1Œ
ÀDeterministicFastPrimitivesCoderV2$eNptj7FLA1EMxuvQqYJIBXF1EF1a6CC4qijiUhx0DHnPtBf67u7xXu7gBkE3h2zG/9f2VERohi8hfL8v5HX4ebAz6EuPMKIvCBxhOfH1M6X800yPr0kolVxxFvY3mGWeuGThlvLVxvM4sw87e7MXO9V9aKr1KnRcLaFPsGKg4y3UL6N7sMAQHPrVHzCas18F+md02ekIslCEgI6C6clTYqEHinWSaT9/610Zw/Q21U287O6pMz2EJhMkCri5DwsOFFGKbO+6C+36S64rEFyaDtsZnF9Y4yZfP/Nvyw=="”
!ref_Coder__MemoizingPickleCoder_3≠
™
beam:coder:pickled_python:v1â
Ü_MemoizingPickleCoder$eNprYJ3qywABPZKJBYnJGanxSamJuXrJ+SmpRcVQakqPaLxvam5+ZlVmXnpAZnJ2TqozSGLK5CmajVNqp/RwJYN1FmdWpU7xFihO0gMAF9kf5w=="Ö
ref_Coder_TupleCoder_7k

beam:coder:kv:v14ref_Coder_DeterministicFastPrimitivesCoderV2_v2_69_8ref_Coder_FastPrimitivesCoder_6"@
ref_Coder_GlobalWindowCoder_2

beam:coder:global_window:v1"Å
4ref_Coder__SDFBoundedSourceWrapperRestrictionCoder_5»
≈
beam:coder:pickled_python:v1§
°_SDFBoundedSourceWrapperRestrictionCoder$eNprYJ3qwwABPaKJBYnJGanxSamJuXqZ+UCUlFicOqVHIz7Yxc0pvzQvJTUlOL+0KDk1vCixoCC1KCi1uKQoM7kkMz/POT8ltWjK5CmajVNqpyTpAQDfhR+5"Ä
ref_Coder_TupleCoder_9f

beam:coder:kv:v14ref_Coder_DeterministicFastPrimitivesCoderV2_v2_69_8ref_Coder_IterableCoder_10"Y
ref_Coder_IterableCoder_10;

beam:coder:iterable:v1ref_Coder_FastPrimitivesCoder_6"ﬂ
ref_Coder_FastPrimitivesCoder_6ª
∏
beam:coder:pickled_python:v1ó
îFastPrimitivesCoder$eNprYJ2axgABPZKJBYnJGanxSamJuXrJ+SmpRcVQakqPsFticUlAUWZuZklmWWqxM0h4yuQpmo1Taqf08MenJebkJCUmZ8eD1U/JYOjhDshMzs5JRVGYVJykBwDOwCqZ":
ref_Coder_StrUtf8Coder_11

beam:coder:string_utf8:v1*>
%ref_Environment_default_environment_1beam:env:default:v1)ref_AppliedPTransform_AppliedPTransform_1")beam:requirement:pardo:splittable_dofn:v1