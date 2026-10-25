
¬è
”
Fref_AppliedPTransform_ReadOrders-ParDo-_ArrowTableToRowDictionaries-_8à

beam:transform:pardo:v1%
Noneref_PCollection_PCollection_3"%
Noneref_PCollection_PCollection_4*.ReadOrders/ParDo(_ArrowTableToRowDictionaries)2ì
beam:display_data:labelled:v1r
Transform Function5apache_beam.io.parquetio._ArrowTableToRowDictionaries2fn:!apache_beam.transforms.core.ParDo:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
Î
Iref_AppliedPTransform_WriteLines-Write-WriteImpl-DoOnce-MaybeReshuffle_16ù
"
 beam:transform:pickled_python:v1%
Noneref_PCollection_PCollection_7"%
Noneref_PCollection_PCollection_7*0WriteLines/Write/WriteImpl/DoOnce/MaybeReshuffle:%ref_Environment_default_environment_1BP
python_typeAapache_beam.transforms.core.Create.expand.<locals>.MaybeReshuffle
Ó
ref_AppliedPTransform_Format_9À

beam:transform:pardo:v1%
Noneref_PCollection_PCollection_4"%
Noneref_PCollection_PCollection_5*Format2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2o
beam:display_data:labelled:v1N
Transform Functionstr2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
ô
)ref_AppliedPTransform_WriteLines-Write_11Î
%
#beam:transform:generic_composite:v13ref_AppliedPTransform_WriteLines-Write-WriteImpl_12%
Noneref_PCollection_PCollection_5"&
Noneref_PCollection_PCollection_16*WriteLines/Write2k
beam:display_data:labelled:v1J
sinkapache_beam.io.textio._TextSink2sink:apache_beam.io.iobase.Write2b
beam:display_data:labelled:v1A
compressionauto2compression:apache_beam.io.textio._TextSink2ñ
beam:display_data:labelled:v1u
File Pattern6data/out/lines-%(shard_num)05d-of-%(num_shards)05d.txt2file_pattern:apache_beam.io.textio._TextSink2o
beam:display_data:labelled:v1N
Append Trailing New Lines2append_newline:apache_beam.io.textio._TextSink:%ref_Environment_default_environment_1B*
python_typeapache_beam.io.iobase.Write
¿
Fref_AppliedPTransform_WriteLines-Write-WriteImpl-DoOnce-Map-decode-_17ı

beam:transform:pardo:v1%
Noneref_PCollection_PCollection_7"%
Noneref_PCollection_PCollection_8*-WriteLines/Write/WriteImpl/DoOnce/Map(decode)2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2r
beam:display_data:labelled:v1Q
Transform Functiondecode2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
®
8ref_AppliedPTransform_WriteLines-Write-WriteImpl-Pair_21Î

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_11"&
Noneref_PCollection_PCollection_12*WriteLines/Write/WriteImpl/Pair2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2t
beam:display_data:labelled:v1S
Transform Function<lambda>2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
•
?ref_AppliedPTransform_WriteLines-Write-WriteImpl-PreFinalize_24·

beam:transform:pardo:v13
python_side_input0ref_PCollection_PCollection_94
python_side_input1ref_PCollection_PCollection_14%
Noneref_PCollection_PCollection_8"&
Noneref_PCollection_PCollection_15*&WriteLines/Write/WriteImpl/PreFinalize2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2y
beam:display_data:labelled:v1X
Transform Function_pre_finalize2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
‹
@ref_AppliedPTransform_WriteLines-Write-WriteImpl-WriteBundles_20ó

beam:transform:pardo:v13
python_side_input0ref_PCollection_PCollection_9&
Noneref_PCollection_PCollection_10"&
Noneref_PCollection_PCollection_11*'WriteLines/Write/WriteImpl/WriteBundles2Ñ
beam:display_data:labelled:v1c
Transform Function&apache_beam.io.iobase._WriteBundleDoFn2fn:!apache_beam.transforms.core.ParDo2b
beam:display_data:labelled:v1A
compressionauto2compression:apache_beam.io.textio._TextSink2ñ
beam:display_data:labelled:v1u
File Pattern6data/out/lines-%(shard_num)05d-of-%(num_shards)05d.txt2file_pattern:apache_beam.io.textio._TextSink2o
beam:display_data:labelled:v1N
Append Trailing New Lines2append_newline:apache_beam.io.textio._TextSink:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
ì
>ref_AppliedPTransform_WriteLines-Write-WriteImpl-GroupByKey_22–
 
beam:transform:group_by_key:v1&
Noneref_PCollection_PCollection_12"&
Noneref_PCollection_PCollection_13*%WriteLines/Write/WriteImpl/GroupByKeyB5
python_type&apache_beam.transforms.core.GroupByKey
·
Aref_AppliedPTransform_WriteLines-Write-WriteImpl-FinalizeWrite_25õ

beam:transform:pardo:v13
python_side_input0ref_PCollection_PCollection_94
python_side_input1ref_PCollection_PCollection_144
python_side_input2ref_PCollection_PCollection_15%
Noneref_PCollection_PCollection_8"&
Noneref_PCollection_PCollection_16*(WriteLines/Write/WriteImpl/FinalizeWrite2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2{
beam:display_data:labelled:v1Z
Transform Function_finalize_write2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
Ì
3ref_AppliedPTransform_WriteLines-Write-WriteImpl_12µ
%
#beam:transform:generic_composite:v1:ref_AppliedPTransform_WriteLines-Write-WriteImpl-DoOnce_13Cref_AppliedPTransform_WriteLines-Write-WriteImpl-InitializeWrite_18Lref_AppliedPTransform_WriteLines-Write-WriteImpl-WindowInto-WindowIntoFn-_19@ref_AppliedPTransform_WriteLines-Write-WriteImpl-WriteBundles_208ref_AppliedPTransform_WriteLines-Write-WriteImpl-Pair_21>ref_AppliedPTransform_WriteLines-Write-WriteImpl-GroupByKey_22;ref_AppliedPTransform_WriteLines-Write-WriteImpl-Extract_23?ref_AppliedPTransform_WriteLines-Write-WriteImpl-PreFinalize_24Aref_AppliedPTransform_WriteLines-Write-WriteImpl-FinalizeWrite_25%
Noneref_PCollection_PCollection_5"&
Noneref_PCollection_PCollection_16*WriteLines/Write/WriteImpl:%ref_Environment_default_environment_1B.
python_typeapache_beam.io.iobase.WriteImpl
º
Cref_AppliedPTransform_WriteLines-Write-WriteImpl-InitializeWrite_18Ù

beam:transform:pardo:v1%
Noneref_PCollection_PCollection_8"%
Noneref_PCollection_PCollection_9**WriteLines/Write/WriteImpl/InitializeWrite2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2t
beam:display_data:labelled:v1S
Transform Function<lambda>2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
≈
/ref_AppliedPTransform_ReadOrders-Read-Impulse_4ë

beam:transform:impulse:v1"%
Noneref_PCollection_PCollection_1*ReadOrders/Read/ImpulseB2
python_type#apache_beam.transforms.core.Impulse
∫
:ref_AppliedPTransform_WriteLines-Write-WriteImpl-DoOnce_13˚
"
 beam:transform:pickled_python:v1Bref_AppliedPTransform_WriteLines-Write-WriteImpl-DoOnce-Impulse_14Zref_AppliedPTransform_WriteLines-Write-WriteImpl-DoOnce-FlatMap-lambda-at-core-py-4094-_15Iref_AppliedPTransform_WriteLines-Write-WriteImpl-DoOnce-MaybeReshuffle_16Fref_AppliedPTransform_WriteLines-Write-WriteImpl-DoOnce-Map-decode-_17"%
Noneref_PCollection_PCollection_8*!WriteLines/Write/WriteImpl/DoOnce:%ref_Environment_default_environment_1B1
python_type"apache_beam.transforms.core.Create
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
Î
)ref_AppliedPTransform_AppliedPTransform_1Ω
 "ref_AppliedPTransform_ReadOrders_2ref_AppliedPTransform_Format_9#ref_AppliedPTransform_WriteLines_10*)ref_AppliedPTransform_AppliedPTransform_1:%ref_Environment_default_environment_1
‚
Lref_AppliedPTransform_WriteLines-Write-WriteImpl-WindowInto-WindowIntoFn-_19ë

beam:transform:window_into:v1%
Noneref_PCollection_PCollection_5"&
Noneref_PCollection_PCollection_10*3WriteLines/Write/WriteImpl/WindowInto(WindowIntoFn)2ã
beam:display_data:labelled:v1j
Transform Function(apache_beam.transforms.core.WindowIntoFn2fn:&apache_beam.transforms.core.WindowInto:%ref_Environment_default_environment_1B5
python_type&apache_beam.transforms.core.WindowInto
Í
Bref_AppliedPTransform_WriteLines-Write-WriteImpl-DoOnce-Impulse_14£

beam:transform:impulse:v1"%
Noneref_PCollection_PCollection_6*)WriteLines/Write/WriteImpl/DoOnce/ImpulseB2
python_type#apache_beam.transforms.core.Impulse
Ï
Zref_AppliedPTransform_WriteLines-Write-WriteImpl-DoOnce-FlatMap-lambda-at-core-py-4094-_15ç

beam:transform:pardo:v1%
Noneref_PCollection_PCollection_6"%
Noneref_PCollection_PCollection_7*CWriteLines/Write/WriteImpl/DoOnce/FlatMap(<lambda at core.py:4094>)2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2t
beam:display_data:labelled:v1S
Transform Function<lambda>2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
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
Æ
#ref_AppliedPTransform_WriteLines_10Ü
%
#beam:transform:generic_composite:v1)ref_AppliedPTransform_WriteLines-Write_11%
Noneref_PCollection_PCollection_5"&
Noneref_PCollection_PCollection_16*
WriteLines:%ref_Environment_default_environment_1B0
python_type!apache_beam.io.textio.WriteToText
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
Æ
;ref_AppliedPTransform_WriteLines-Write-WriteImpl-Extract_23Ó

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_13"&
Noneref_PCollection_PCollection_14*"WriteLines/Write/WriteImpl/Extract2ç
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
python_type,apache_beam.io.iobase.SDFBoundedSourceReaderè
ref_PCollection_PCollection_15m
-38WriteLines/Write/WriteImpl/PreFinalize.Noneref_Coder_FastPrimitivesCoder_6"ref_Windowing_Windowing_1à
ref_PCollection_PCollection_6g
041WriteLines/Write/WriteImpl/DoOnce/Impulse.Noneref_Coder_BytesCoder_1"ref_Windowing_Windowing_1í
ref_PCollection_PCollection_9q
142WriteLines/Write/WriteImpl/InitializeWrite.Noneref_Coder_FastPrimitivesCoder_6"ref_Windowing_Windowing_1Ñ
ref_PCollection_PCollection_2c
!26ReadOrders/Read/EmitSource.None!ref_Coder__MemoizingPickleCoder_3"ref_Windowing_Windowing_1ï
ref_PCollection_PCollection_10s
:51WriteLines/Write/WriteImpl/WindowInto(WindowIntoFn).Noneref_Coder_StrUtf8Coder_7"ref_Windowing_Windowing_1ä
ref_PCollection_PCollection_16h
/40WriteLines/Write/WriteImpl/FinalizeWrite.Noneref_Coder_StrUtf8Coder_7"ref_Windowing_Windowing_1´
ref_PCollection_PCollection_3â
I66ReadOrders/Read/SDFBoundedSourceReader/ParDo(SDFBoundedSourceDoFn).Noneref_Coder_FastPrimitivesCoder_6"ref_Windowing_Windowing_1£
ref_PCollection_PCollection_7Å
J67WriteLines/Write/WriteImpl/DoOnce/FlatMap(<lambda at core.py:4094>).Noneref_Coder_BytesCoder_1"ref_Windowing_Windowing_1ã
ref_PCollection_PCollection_14i
)34WriteLines/Write/WriteImpl/Extract.Noneref_Coder_FastPrimitivesCoder_6"ref_Windowing_Windowing_1v
ref_PCollection_PCollection_1U
23ReadOrders/Read/Impulse.Noneref_Coder_BytesCoder_1"ref_Windowing_Windowing_1ê
ref_PCollection_PCollection_11n
.39WriteLines/Write/WriteImpl/WriteBundles.Noneref_Coder_FastPrimitivesCoder_6"ref_Windowing_Windowing_1ñ
ref_PCollection_PCollection_4u
546ReadOrders/ParDo(_ArrowTableToRowDictionaries).Noneref_Coder_FastPrimitivesCoder_6"ref_Windowing_Windowing_1
ref_PCollection_PCollection_12]
&31WriteLines/Write/WriteImpl/Pair.Noneref_Coder_TupleCoder_8"ref_Windowing_Windowing_1f
ref_PCollection_PCollection_5E
6Format.Noneref_Coder_StrUtf8Coder_7"ref_Windowing_Windowing_1ï
ref_PCollection_PCollection_8t
445WriteLines/Write/WriteImpl/DoOnce/Map(decode).Noneref_Coder_FastPrimitivesCoder_6"ref_Windowing_Windowing_1Ü
ref_PCollection_PCollection_13d
,37WriteLines/Write/WriteImpl/GroupByKey.Noneref_Coder_TupleCoder_10"ref_Windowing_Windowing_1ï
ref_Windowing_Windowing_1x
"
 beam:window_fn:global_windows:v1ref_Coder_GlobalWindowCoder_2": (08HZ%ref_Environment_default_environment_1"1
ref_Coder_BytesCoder_1

beam:coder:bytes:v1"Ö
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
Ü_MemoizingPickleCoder$eNprYJ3qywABPZKJBYnJGanxSamJuXrJ+SmpRcVQakqPaLxvam5+ZlVmXnpAZnJ2TqozSGLK5CmajVNqp/RwJYN1FmdWpU7xFihO0gMAF9kf5w=="Ö
ref_Coder_TupleCoder_8k

beam:coder:kv:v14ref_Coder_DeterministicFastPrimitivesCoderV2_v2_69_9ref_Coder_FastPrimitivesCoder_6"@
ref_Coder_GlobalWindowCoder_2

beam:coder:global_window:v1"´
4ref_Coder_DeterministicFastPrimitivesCoderV2_v2_69_9Ú
Ô
beam:coder:pickled_python:v1Œ
ÀDeterministicFastPrimitivesCoderV2$eNptjzFLA0EQhWORKoJICkkrCNokkCKQ1ogiWqTScphdJ7khe3vL7t7BFYJ2FtM5+b9JTkUEp3gzDO97w7z1t8OjXlcywoC2IDCE5dhWLxTTd1M5v6FMsWTPKbO9xZSXkUvO3FBaHDxPU/3Uq3d91Us5hdrvV65lv4YuQYueDP+hfhg5gRU6Z9BufoHBku3G0R+jSUYGkDIFcGjIqVw8R870yJ7SpBu/9L4MbnIXqzpctw/UqpxBnQgiOTychxU7CpiLpB9yDM3+Sa48ZFyr9JspzOZam/EOxXVvSA=="9
ref_Coder_StrUtf8Coder_7

beam:coder:string_utf8:v1"ﬂ
ref_Coder_FastPrimitivesCoder_6ª
∏
beam:coder:pickled_python:v1ó
îFastPrimitivesCoder$eNprYJ2axgABPZKJBYnJGanxSamJuXrJ+SmpRcVQakqPsFticUlAUWZuZklmWWqxM0h4yuQpmo1Taqf08MenJebkJCUmZ8eD1U/JYOjhDshMzs5JRVGYVJykBwDOwCqZ"Å
ref_Coder_TupleCoder_10f

beam:coder:kv:v14ref_Coder_DeterministicFastPrimitivesCoderV2_v2_69_9ref_Coder_IterableCoder_11*>
%ref_Environment_default_environment_1beam:env:default:v1)ref_AppliedPTransform_AppliedPTransform_1")beam:requirement:pardo:splittable_dofn:v1