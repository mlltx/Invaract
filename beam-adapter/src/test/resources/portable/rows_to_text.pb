
◊è
°
=ref_AppliedPTransform_WriteCsv-Write-WriteImpl-PreFinalize_24ﬂ

beam:transform:pardo:v13
python_side_input0ref_PCollection_PCollection_94
python_side_input1ref_PCollection_PCollection_14%
Noneref_PCollection_PCollection_8"&
Noneref_PCollection_PCollection_15*$WriteCsv/Write/WriteImpl/PreFinalize2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2y
beam:display_data:labelled:v1X
Transform Function_pre_finalize2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
Ò
ref_AppliedPTransform_Typed_9œ

beam:transform:pardo:v1%
Noneref_PCollection_PCollection_4"%
Noneref_PCollection_PCollection_5*Typed2ç
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
è
<ref_AppliedPTransform_WriteCsv-Write-WriteImpl-GroupByKey_22Œ
 
beam:transform:group_by_key:v1&
Noneref_PCollection_PCollection_12"&
Noneref_PCollection_PCollection_13*#WriteCsv/Write/WriteImpl/GroupByKeyB5
python_type&apache_beam.transforms.core.GroupByKey
§
6ref_AppliedPTransform_WriteCsv-Write-WriteImpl-Pair_21È

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_11"&
Noneref_PCollection_PCollection_12*WriteCsv/Write/WriteImpl/Pair2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2t
beam:display_data:labelled:v1S
Transform Function<lambda>2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
™
9ref_AppliedPTransform_WriteCsv-Write-WriteImpl-Extract_23Ï

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_13"&
Noneref_PCollection_PCollection_14* WriteCsv/Write/WriteImpl/Extract2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2t
beam:display_data:labelled:v1S
Transform Function<lambda>2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
Ë
Xref_AppliedPTransform_WriteCsv-Write-WriteImpl-DoOnce-FlatMap-lambda-at-core-py-4094-_15ã

beam:transform:pardo:v1%
Noneref_PCollection_PCollection_6"%
Noneref_PCollection_PCollection_7*AWriteCsv/Write/WriteImpl/DoOnce/FlatMap(<lambda at core.py:4094>)2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2t
beam:display_data:labelled:v1S
Transform Function<lambda>2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
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
≈
/ref_AppliedPTransform_ReadOrders-Read-Impulse_4ë

beam:transform:impulse:v1"%
Noneref_PCollection_PCollection_1*ReadOrders/Read/ImpulseB2
python_type#apache_beam.transforms.core.Impulse
Ê
@ref_AppliedPTransform_WriteCsv-Write-WriteImpl-DoOnce-Impulse_14°

beam:transform:impulse:v1"%
Noneref_PCollection_PCollection_6*'WriteCsv/Write/WriteImpl/DoOnce/ImpulseB2
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
∏
Aref_AppliedPTransform_WriteCsv-Write-WriteImpl-InitializeWrite_18Ú

beam:transform:pardo:v1%
Noneref_PCollection_PCollection_8"%
Noneref_PCollection_PCollection_9*(WriteCsv/Write/WriteImpl/InitializeWrite2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2t
beam:display_data:labelled:v1S
Transform Function<lambda>2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
›
?ref_AppliedPTransform_WriteCsv-Write-WriteImpl-FinalizeWrite_25ô

beam:transform:pardo:v13
python_side_input0ref_PCollection_PCollection_94
python_side_input1ref_PCollection_PCollection_144
python_side_input2ref_PCollection_PCollection_15%
Noneref_PCollection_PCollection_8"&
Noneref_PCollection_PCollection_16*&WriteCsv/Write/WriteImpl/FinalizeWrite2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2{
beam:display_data:labelled:v1Z
Transform Function_finalize_write2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
Á
Gref_AppliedPTransform_WriteCsv-Write-WriteImpl-DoOnce-MaybeReshuffle_16õ
"
 beam:transform:pickled_python:v1%
Noneref_PCollection_PCollection_7"%
Noneref_PCollection_PCollection_7*.WriteCsv/Write/WriteImpl/DoOnce/MaybeReshuffle:%ref_Environment_default_environment_1BP
python_typeAapache_beam.transforms.core.Create.expand.<locals>.MaybeReshuffle
Ÿ
>ref_AppliedPTransform_WriteCsv-Write-WriteImpl-WriteBundles_20ñ

beam:transform:pardo:v13
python_side_input0ref_PCollection_PCollection_9&
Noneref_PCollection_PCollection_10"&
Noneref_PCollection_PCollection_11*%WriteCsv/Write/WriteImpl/WriteBundles2Ñ
beam:display_data:labelled:v1c
Transform Function&apache_beam.io.iobase._WriteBundleDoFn2fn:!apache_beam.transforms.core.ParDo2b
beam:display_data:labelled:v1A
compressionauto2compression:apache_beam.io.textio._TextSink2ó
beam:display_data:labelled:v1v
File Pattern7data/out/report-%(shard_num)05d-of-%(num_shards)05d.csv2file_pattern:apache_beam.io.textio._TextSink2o
beam:display_data:labelled:v1N
Append Trailing New Lines2append_newline:apache_beam.io.textio._TextSink:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
º
Dref_AppliedPTransform_WriteCsv-Write-WriteImpl-DoOnce-Map-decode-_17Û

beam:transform:pardo:v1%
Noneref_PCollection_PCollection_7"%
Noneref_PCollection_PCollection_8*+WriteCsv/Write/WriteImpl/DoOnce/Map(decode)2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2r
beam:display_data:labelled:v1Q
Transform Functiondecode2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
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
Ë
)ref_AppliedPTransform_AppliedPTransform_1∫
 "ref_AppliedPTransform_ReadOrders_2ref_AppliedPTransform_Typed_9!ref_AppliedPTransform_WriteCsv_10*)ref_AppliedPTransform_AppliedPTransform_1:%ref_Environment_default_environment_1
Æ
8ref_AppliedPTransform_WriteCsv-Write-WriteImpl-DoOnce_13Ò
"
 beam:transform:pickled_python:v1@ref_AppliedPTransform_WriteCsv-Write-WriteImpl-DoOnce-Impulse_14Xref_AppliedPTransform_WriteCsv-Write-WriteImpl-DoOnce-FlatMap-lambda-at-core-py-4094-_15Gref_AppliedPTransform_WriteCsv-Write-WriteImpl-DoOnce-MaybeReshuffle_16Dref_AppliedPTransform_WriteCsv-Write-WriteImpl-DoOnce-Map-decode-_17"%
Noneref_PCollection_PCollection_8*WriteCsv/Write/WriteImpl/DoOnce:%ref_Environment_default_environment_1B1
python_type"apache_beam.transforms.core.Create
◊
1ref_AppliedPTransform_WriteCsv-Write-WriteImpl_12°
%
#beam:transform:generic_composite:v18ref_AppliedPTransform_WriteCsv-Write-WriteImpl-DoOnce_13Aref_AppliedPTransform_WriteCsv-Write-WriteImpl-InitializeWrite_18Jref_AppliedPTransform_WriteCsv-Write-WriteImpl-WindowInto-WindowIntoFn-_19>ref_AppliedPTransform_WriteCsv-Write-WriteImpl-WriteBundles_206ref_AppliedPTransform_WriteCsv-Write-WriteImpl-Pair_21<ref_AppliedPTransform_WriteCsv-Write-WriteImpl-GroupByKey_229ref_AppliedPTransform_WriteCsv-Write-WriteImpl-Extract_23=ref_AppliedPTransform_WriteCsv-Write-WriteImpl-PreFinalize_24?ref_AppliedPTransform_WriteCsv-Write-WriteImpl-FinalizeWrite_25%
Noneref_PCollection_PCollection_5"&
Noneref_PCollection_PCollection_16*WriteCsv/Write/WriteImpl:%ref_Environment_default_environment_1B.
python_typeapache_beam.io.iobase.WriteImpl
ﬁ
Jref_AppliedPTransform_WriteCsv-Write-WriteImpl-WindowInto-WindowIntoFn-_19è

beam:transform:window_into:v1%
Noneref_PCollection_PCollection_5"&
Noneref_PCollection_PCollection_10*1WriteCsv/Write/WriteImpl/WindowInto(WindowIntoFn)2ã
beam:display_data:labelled:v1j
Transform Function(apache_beam.transforms.core.WindowIntoFn2fn:&apache_beam.transforms.core.WindowInto:%ref_Environment_default_environment_1B5
python_type&apache_beam.transforms.core.WindowInto
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
®
!ref_AppliedPTransform_WriteCsv_10Ç
%
#beam:transform:generic_composite:v1'ref_AppliedPTransform_WriteCsv-Write_11%
Noneref_PCollection_PCollection_5"&
Noneref_PCollection_PCollection_16*WriteCsv:%ref_Environment_default_environment_1B0
python_type!apache_beam.io.textio.WriteToText
î
'ref_AppliedPTransform_WriteCsv-Write_11Ë
%
#beam:transform:generic_composite:v11ref_AppliedPTransform_WriteCsv-Write-WriteImpl_12%
Noneref_PCollection_PCollection_5"&
Noneref_PCollection_PCollection_16*WriteCsv/Write2k
beam:display_data:labelled:v1J
sinkapache_beam.io.textio._TextSink2sink:apache_beam.io.iobase.Write2b
beam:display_data:labelled:v1A
compressionauto2compression:apache_beam.io.textio._TextSink2ó
beam:display_data:labelled:v1v
File Pattern7data/out/report-%(shard_num)05d-of-%(num_shards)05d.csv2file_pattern:apache_beam.io.textio._TextSink2o
beam:display_data:labelled:v1N
Append Trailing New Lines2append_newline:apache_beam.io.textio._TextSink:%ref_Environment_default_environment_1B*
python_typeapache_beam.io.iobase.Write
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
python_type,apache_beam.io.iobase.SDFBoundedSourceReaderç
ref_PCollection_PCollection_15k
+36WriteCsv/Write/WriteImpl/PreFinalize.Noneref_Coder_FastPrimitivesCoder_6"ref_Windowing_Windowing_1Ü
ref_PCollection_PCollection_6e
.39WriteCsv/Write/WriteImpl/DoOnce/Impulse.Noneref_Coder_BytesCoder_1"ref_Windowing_Windowing_1ê
ref_PCollection_PCollection_9o
/40WriteCsv/Write/WriteImpl/InitializeWrite.Noneref_Coder_FastPrimitivesCoder_6"ref_Windowing_Windowing_1Ñ
ref_PCollection_PCollection_2c
!26ReadOrders/Read/EmitSource.None!ref_Coder__MemoizingPickleCoder_3"ref_Windowing_Windowing_1è
ref_PCollection_PCollection_10m
849WriteCsv/Write/WriteImpl/WindowInto(WindowIntoFn).Noneref_Coder_RowCoder_8"ref_Windowing_Windowing_1â
ref_PCollection_PCollection_16g
-38WriteCsv/Write/WriteImpl/FinalizeWrite.Noneref_Coder_StrUtf8Coder_13"ref_Windowing_Windowing_1´
ref_PCollection_PCollection_3â
I66ReadOrders/Read/SDFBoundedSourceReader/ParDo(SDFBoundedSourceDoFn).Noneref_Coder_FastPrimitivesCoder_6"ref_Windowing_Windowing_1†
ref_PCollection_PCollection_7
H65WriteCsv/Write/WriteImpl/DoOnce/FlatMap(<lambda at core.py:4094>).Noneref_Coder_BytesCoder_1"ref_Windowing_Windowing_1â
ref_PCollection_PCollection_14g
'32WriteCsv/Write/WriteImpl/Extract.Noneref_Coder_FastPrimitivesCoder_6"ref_Windowing_Windowing_1v
ref_PCollection_PCollection_1U
23ReadOrders/Read/Impulse.Noneref_Coder_BytesCoder_1"ref_Windowing_Windowing_1é
ref_PCollection_PCollection_11l
,37WriteCsv/Write/WriteImpl/WriteBundles.Noneref_Coder_FastPrimitivesCoder_6"ref_Windowing_Windowing_1Ñ
ref_PCollection_PCollection_4c
.39ReadOrders/ParDo(_ArrowTableToBeamRows).Noneref_Coder_RowCoder_7"ref_Windowing_Windowing_1}
ref_PCollection_PCollection_12[
$29WriteCsv/Write/WriteImpl/Pair.Noneref_Coder_TupleCoder_9"ref_Windowing_Windowing_1a
ref_PCollection_PCollection_5@
5Typed.Noneref_Coder_RowCoder_8"ref_Windowing_Windowing_1ì
ref_PCollection_PCollection_8r
243WriteCsv/Write/WriteImpl/DoOnce/Map(decode).Noneref_Coder_FastPrimitivesCoder_6"ref_Windowing_Windowing_1Ñ
ref_PCollection_PCollection_13b
*35WriteCsv/Write/WriteImpl/GroupByKey.Noneref_Coder_TupleCoder_11"ref_Windowing_Windowing_1ï
ref_Windowing_Windowing_1x
"
 beam:window_fn:global_windows:v1ref_Coder_GlobalWindowCoder_2": (08HZ%ref_Environment_default_environment_1"1
ref_Coder_BytesCoder_1

beam:coder:bytes:v1"Ö
ref_Coder_TupleCoder_4k

beam:coder:kv:v14ref_Coder__SDFBoundedSourceWrapperRestrictionCoder_5ref_Coder_FastPrimitivesCoder_6"Ç
ref_Coder_TupleCoder_11g

beam:coder:kv:v15ref_Coder_DeterministicFastPrimitivesCoderV2_v2_69_10ref_Coder_IterableCoder_12"®
5ref_Coder_DeterministicFastPrimitivesCoderV2_v2_69_10Ó
Î
beam:coder:pickled_python:v1 
«DeterministicFastPrimitivesCoderV2$eNptj7FLA1EMxuvQqYIUB3HVRZcWOgiuVhRx6aRjyHumvdDc3eO9dwc3CHZzyGb8f22vighm+BLC9/tC3oaf44NBX3qKAX1B4AjLia9fKKbvZnp2S5liyRWnzP4OU15ELjlzS2m+8zzN7MMuN/ZqFzqGptqupONqBX2CFQM9/of6YfQIliji0K9/gdGC/Vroj9ElpyNImQIIOhLT8+fImeapnfbDXh/KINP7WDfhpnukzvQEmkQQSXB3HJYsFDAXyd71ENrti1xXkHFlOmxncHVtjZt8AeJ7bnU="K
ref_Coder_RowCoder_73
1
beam:coder:row:v1


id

amount"”
!ref_Coder__MemoizingPickleCoder_3≠
™
beam:coder:pickled_python:v1â
Ü_MemoizingPickleCoder$eNprYJ3qywABPZKJBYnJGanxSamJuXrJ+SmpRcVQakqPaLxvam5+ZlVmXnpAZnJ2TqozSGLK5CmajVNqp/RwJYN1FmdWpU7xFihO0gMAF9kf5w=="Å
4ref_Coder__SDFBoundedSourceWrapperRestrictionCoder_5»
≈
beam:coder:pickled_python:v1§
°_SDFBoundedSourceWrapperRestrictionCoder$eNprYJ3qwwABPaKJBYnJGanxSamJuXqZ+UCUlFicOqVHIz7Yxc0pvzQvJTUlOL+0KDk1vCixoCC1KCi1uKQoM7kkMz/POT8ltWjK5CmajVNqpyTpAQDfhR+5"@
ref_Coder_GlobalWindowCoder_2

beam:coder:global_window:v1"o
ref_Coder_RowCoder_8W
U
beam:coder:row:v1@

id

amount$4a7170af-76a6-43ae-a3db-cb10267d45ab"Ü
ref_Coder_TupleCoder_9l

beam:coder:kv:v15ref_Coder_DeterministicFastPrimitivesCoderV2_v2_69_10ref_Coder_FastPrimitivesCoder_6"Y
ref_Coder_IterableCoder_12;

beam:coder:iterable:v1ref_Coder_FastPrimitivesCoder_6"ﬂ
ref_Coder_FastPrimitivesCoder_6ª
∏
beam:coder:pickled_python:v1ó
îFastPrimitivesCoder$eNprYJ2axgABPZKJBYnJGanxSamJuXrJ+SmpRcVQakqPsFticUlAUWZuZklmWWqxM0h4yuQpmo1Taqf08MenJebkJCUmZ8eD1U/JYOjhDshMzs5JRVGYVJykBwDOwCqZ":
ref_Coder_StrUtf8Coder_13

beam:coder:string_utf8:v1*>
%ref_Environment_default_environment_1beam:env:default:v1)ref_AppliedPTransform_AppliedPTransform_1")beam:requirement:pardo:splittable_dofn:v1