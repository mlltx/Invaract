
¢ç
è
Aref_AppliedPTransform_WriteOutput-Write-WriteImpl-WriteBundles_18…

beam:transform:pardo:v13
python_side_input0ref_PCollection_PCollection_7%
Noneref_PCollection_PCollection_8"%
Noneref_PCollection_PCollection_9*(WriteOutput/Write/WriteImpl/WriteBundles2Ñ
beam:display_data:labelled:v1c
Transform Function&apache_beam.io.iobase._WriteBundleDoFn2fn:!apache_beam.transforms.core.ParDo2b
beam:display_data:labelled:v1A
compressionauto2compression:apache_beam.io.textio._TextSink2…
beam:display_data:labelled:v1ß
File PatternhRuntimeValueProvider(option: output, type: str, default_value: None)-%(shard_num)05d-of-%(num_shards)05d2file_pattern:apache_beam.io.textio._TextSink2o
beam:display_data:labelled:v1N
Append Trailing New Lines2append_newline:apache_beam.io.textio._TextSink:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
¥	
=ref_AppliedPTransform_ReadInput-Read-SDFBoundedSourceReader_6Ú
%
#beam:transform:generic_composite:v1Yref_AppliedPTransform_ReadInput-Read-SDFBoundedSourceReader-ParDo-SDFBoundedSourceDoFn-_7%
Noneref_PCollection_PCollection_2"%
Noneref_PCollection_PCollection_3*%ReadInput/Read/SDFBoundedSourceReader2Ø
beam:display_data:labelled:v1ç
File PatternCRuntimeValueProvider(option: input, type: str, default_value: None)2file_pattern:*apache_beam.io.iobase.SDFBoundedSourceDoFn2r
beam:display_data:labelled:v1Q
Compression Typeauto2compression:*apache_beam.io.iobase.SDFBoundedSourceDoFn2x
beam:display_data:labelled:v1W
Strip Trailing New Lines2strip_newline:*apache_beam.io.iobase.SDFBoundedSourceDoFn2j
beam:display_data:labelled:v1I
Buffer Size(Ä@2buffer_size:*apache_beam.io.iobase.SDFBoundedSourceDoFn2É
beam:display_data:labelled:v1b
Coder&apache_beam.coders.coders.StrUtf8Coder2coder:*apache_beam.io.iobase.SDFBoundedSourceDoFn2Ç
beam:display_data:labelled:v1a
source!apache_beam.io.textio._TextSource2source:,apache_beam.io.iobase.SDFBoundedSourceReader:%ref_Environment_default_environment_1B;
python_type,apache_beam.io.iobase.SDFBoundedSourceReader
É

Yref_AppliedPTransform_ReadInput-Read-SDFBoundedSourceReader-ParDo-SDFBoundedSourceDoFn-_7•	

beam:transform:pardo:v1%
Noneref_PCollection_PCollection_2"%
Noneref_PCollection_PCollection_3*AReadInput/Read/SDFBoundedSourceReader/ParDo(SDFBoundedSourceDoFn)2à
beam:display_data:labelled:v1g
Transform Function*apache_beam.io.iobase.SDFBoundedSourceDoFn2fn:!apache_beam.transforms.core.ParDo2Ø
beam:display_data:labelled:v1ç
File PatternCRuntimeValueProvider(option: input, type: str, default_value: None)2file_pattern:*apache_beam.io.iobase.SDFBoundedSourceDoFn2r
beam:display_data:labelled:v1Q
Compression Typeauto2compression:*apache_beam.io.iobase.SDFBoundedSourceDoFn2x
beam:display_data:labelled:v1W
Strip Trailing New Lines2strip_newline:*apache_beam.io.iobase.SDFBoundedSourceDoFn2j
beam:display_data:labelled:v1I
Buffer Size(Ä@2buffer_size:*apache_beam.io.iobase.SDFBoundedSourceDoFn2É
beam:display_data:labelled:v1b
Coder&apache_beam.coders.coders.StrUtf8Coder2coder:*apache_beam.io.iobase.SDFBoundedSourceDoFn2Ä
beam:display_data:labelled:v1_
source!apache_beam.io.textio._TextSource2source:*apache_beam.io.iobase.SDFBoundedSourceDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
ß
@ref_AppliedPTransform_WriteOutput-Write-WriteImpl-PreFinalize_22‚

beam:transform:pardo:v13
python_side_input0ref_PCollection_PCollection_74
python_side_input1ref_PCollection_PCollection_12%
Noneref_PCollection_PCollection_6"&
Noneref_PCollection_PCollection_13*'WriteOutput/Write/WriteImpl/PreFinalize2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2y
beam:display_data:labelled:v1X
Transform Function_pre_finalize2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
Ó
[ref_AppliedPTransform_WriteOutput-Write-WriteImpl-DoOnce-FlatMap-lambda-at-core-py-4094-_13é

beam:transform:pardo:v1%
Noneref_PCollection_PCollection_4"%
Noneref_PCollection_PCollection_5*DWriteOutput/Write/WriteImpl/DoOnce/FlatMap(<lambda at core.py:4094>)2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2t
beam:display_data:labelled:v1S
Transform Function<lambda>2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
¯
4ref_AppliedPTransform_WriteOutput-Write-WriteImpl_10ø
%
#beam:transform:generic_composite:v1;ref_AppliedPTransform_WriteOutput-Write-WriteImpl-DoOnce_11Dref_AppliedPTransform_WriteOutput-Write-WriteImpl-InitializeWrite_16Mref_AppliedPTransform_WriteOutput-Write-WriteImpl-WindowInto-WindowIntoFn-_17Aref_AppliedPTransform_WriteOutput-Write-WriteImpl-WriteBundles_189ref_AppliedPTransform_WriteOutput-Write-WriteImpl-Pair_19?ref_AppliedPTransform_WriteOutput-Write-WriteImpl-GroupByKey_20<ref_AppliedPTransform_WriteOutput-Write-WriteImpl-Extract_21@ref_AppliedPTransform_WriteOutput-Write-WriteImpl-PreFinalize_22Bref_AppliedPTransform_WriteOutput-Write-WriteImpl-FinalizeWrite_23%
Noneref_PCollection_PCollection_3"&
Noneref_PCollection_PCollection_14*WriteOutput/Write/WriteImpl:%ref_Environment_default_environment_1B.
python_typeapache_beam.io.iobase.WriteImpl
¬
Gref_AppliedPTransform_WriteOutput-Write-WriteImpl-DoOnce-Map-decode-_15ˆ

beam:transform:pardo:v1%
Noneref_PCollection_PCollection_5"%
Noneref_PCollection_PCollection_6*.WriteOutput/Write/WriteImpl/DoOnce/Map(decode)2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2r
beam:display_data:labelled:v1Q
Transform Functiondecode2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
©
9ref_AppliedPTransform_WriteOutput-Write-WriteImpl-Pair_19Î

beam:transform:pardo:v1%
Noneref_PCollection_PCollection_9"&
Noneref_PCollection_PCollection_10* WriteOutput/Write/WriteImpl/Pair2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2t
beam:display_data:labelled:v1S
Transform Function<lambda>2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
ï
?ref_AppliedPTransform_WriteOutput-Write-WriteImpl-GroupByKey_20—
 
beam:transform:group_by_key:v1&
Noneref_PCollection_PCollection_10"&
Noneref_PCollection_PCollection_11*&WriteOutput/Write/WriteImpl/GroupByKeyB5
python_type&apache_beam.transforms.core.GroupByKey
æ
Dref_AppliedPTransform_WriteOutput-Write-WriteImpl-InitializeWrite_16ı

beam:transform:pardo:v1%
Noneref_PCollection_PCollection_6"%
Noneref_PCollection_PCollection_7*+WriteOutput/Write/WriteImpl/InitializeWrite2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2t
beam:display_data:labelled:v1S
Transform Function<lambda>2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
∞
<ref_AppliedPTransform_WriteOutput-Write-WriteImpl-Extract_21Ô

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_11"&
Noneref_PCollection_PCollection_12*#WriteOutput/Write/WriteImpl/Extract2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2t
beam:display_data:labelled:v1S
Transform Function<lambda>2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
Ø
#ref_AppliedPTransform_WriteOutput_8á
%
#beam:transform:generic_composite:v1)ref_AppliedPTransform_WriteOutput-Write_9%
Noneref_PCollection_PCollection_3"&
Noneref_PCollection_PCollection_14*WriteOutput:%ref_Environment_default_environment_1B0
python_type!apache_beam.io.textio.WriteToText
„
Bref_AppliedPTransform_WriteOutput-Write-WriteImpl-FinalizeWrite_23ú

beam:transform:pardo:v13
python_side_input0ref_PCollection_PCollection_74
python_side_input1ref_PCollection_PCollection_124
python_side_input2ref_PCollection_PCollection_13%
Noneref_PCollection_PCollection_6"&
Noneref_PCollection_PCollection_14*)WriteOutput/Write/WriteImpl/FinalizeWrite2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2{
beam:display_data:labelled:v1Z
Transform Function_finalize_write2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
Œ
)ref_AppliedPTransform_WriteOutput-Write_9†
%
#beam:transform:generic_composite:v14ref_AppliedPTransform_WriteOutput-Write-WriteImpl_10%
Noneref_PCollection_PCollection_3"&
Noneref_PCollection_PCollection_14*WriteOutput/Write2k
beam:display_data:labelled:v1J
sinkapache_beam.io.textio._TextSink2sink:apache_beam.io.iobase.Write2b
beam:display_data:labelled:v1A
compressionauto2compression:apache_beam.io.textio._TextSink2…
beam:display_data:labelled:v1ß
File PatternhRuntimeValueProvider(option: output, type: str, default_value: None)-%(shard_num)05d-of-%(num_shards)05d2file_pattern:apache_beam.io.textio._TextSink2o
beam:display_data:labelled:v1N
Append Trailing New Lines2append_newline:apache_beam.io.textio._TextSink:%ref_Environment_default_environment_1B*
python_typeapache_beam.io.iobase.Write
 
)ref_AppliedPTransform_AppliedPTransform_1ú
 !ref_AppliedPTransform_ReadInput_2#ref_AppliedPTransform_WriteOutput_8*)ref_AppliedPTransform_AppliedPTransform_1:%ref_Environment_default_environment_1
ô
1ref_AppliedPTransform_ReadInput-Read-EmitSource_5„

beam:transform:pardo:v1%
Noneref_PCollection_PCollection_1"%
Noneref_PCollection_PCollection_2*ReadInput/Read/EmitSource2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2t
beam:display_data:labelled:v1S
Transform Function<lambda>2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
Ï
Cref_AppliedPTransform_WriteOutput-Write-WriteImpl-DoOnce-Impulse_12§

beam:transform:impulse:v1"%
Noneref_PCollection_PCollection_4**WriteOutput/Write/WriteImpl/DoOnce/ImpulseB2
python_type#apache_beam.transforms.core.Impulse
¿
;ref_AppliedPTransform_WriteOutput-Write-WriteImpl-DoOnce_11Ä
"
 beam:transform:pickled_python:v1Cref_AppliedPTransform_WriteOutput-Write-WriteImpl-DoOnce-Impulse_12[ref_AppliedPTransform_WriteOutput-Write-WriteImpl-DoOnce-FlatMap-lambda-at-core-py-4094-_13Jref_AppliedPTransform_WriteOutput-Write-WriteImpl-DoOnce-MaybeReshuffle_14Gref_AppliedPTransform_WriteOutput-Write-WriteImpl-DoOnce-Map-decode-_15"%
Noneref_PCollection_PCollection_6*"WriteOutput/Write/WriteImpl/DoOnce:%ref_Environment_default_environment_1B1
python_type"apache_beam.transforms.core.Create
Ì
Jref_AppliedPTransform_WriteOutput-Write-WriteImpl-DoOnce-MaybeReshuffle_14û
"
 beam:transform:pickled_python:v1%
Noneref_PCollection_PCollection_5"%
Noneref_PCollection_PCollection_5*1WriteOutput/Write/WriteImpl/DoOnce/MaybeReshuffle:%ref_Environment_default_environment_1BP
python_typeAapache_beam.transforms.core.Create.expand.<locals>.MaybeReshuffle
√
.ref_AppliedPTransform_ReadInput-Read-Impulse_4ê

beam:transform:impulse:v1"%
Noneref_PCollection_PCollection_1*ReadInput/Read/ImpulseB2
python_type#apache_beam.transforms.core.Impulse
Å
!ref_AppliedPTransform_ReadInput_2€
%
#beam:transform:generic_composite:v1&ref_AppliedPTransform_ReadInput-Read_3"%
Noneref_PCollection_PCollection_3*	ReadInput:%ref_Environment_default_environment_1B1
python_type"apache_beam.io.textio.ReadFromText
„
Mref_AppliedPTransform_WriteOutput-Write-WriteImpl-WindowInto-WindowIntoFn-_17ë

beam:transform:window_into:v1%
Noneref_PCollection_PCollection_3"%
Noneref_PCollection_PCollection_8*4WriteOutput/Write/WriteImpl/WindowInto(WindowIntoFn)2ã
beam:display_data:labelled:v1j
Transform Function(apache_beam.transforms.core.WindowIntoFn2fn:&apache_beam.transforms.core.WindowInto:%ref_Environment_default_environment_1B5
python_type&apache_beam.transforms.core.WindowInto
À
&ref_AppliedPTransform_ReadInput-Read_3†

beam:transform:read:v1.ref_AppliedPTransform_ReadInput-Read-Impulse_41ref_AppliedPTransform_ReadInput-Read-EmitSource_5=ref_AppliedPTransform_ReadInput-Read-SDFBoundedSourceReader_6"%
Noneref_PCollection_PCollection_3*ReadInput/Read2u
beam:display_data:labelled:v1T
Read Source!apache_beam.io.textio._TextSource2source:apache_beam.io.iobase.Read2¶
beam:display_data:labelled:v1Ñ
File PatternCRuntimeValueProvider(option: input, type: str, default_value: None)2file_pattern:!apache_beam.io.textio._TextSource2i
beam:display_data:labelled:v1H
Compression Typeauto2compression:!apache_beam.io.textio._TextSource2o
beam:display_data:labelled:v1N
Strip Trailing New Lines2strip_newline:!apache_beam.io.textio._TextSource2a
beam:display_data:labelled:v1@
Buffer Size(Ä@2buffer_size:!apache_beam.io.textio._TextSource2z
beam:display_data:labelled:v1Y
Coder&apache_beam.coders.coders.StrUtf8Coder2coder:!apache_beam.io.textio._TextSource:%ref_Environment_default_environment_1B)
python_typeapache_beam.io.iobase.Readì
ref_PCollection_PCollection_7r
243WriteOutput/Write/WriteImpl/InitializeWrite.Noneref_Coder_FastPrimitivesCoder_6"ref_Windowing_Windowing_1ã
ref_PCollection_PCollection_14i
041WriteOutput/Write/WriteImpl/FinalizeWrite.Noneref_Coder_StrUtf8Coder_7"ref_Windowing_Windowing_1u
ref_PCollection_PCollection_1T
22ReadInput/Read/Impulse.Noneref_Coder_BytesCoder_1"ref_Windowing_Windowing_1â
ref_PCollection_PCollection_4h
142WriteOutput/Write/WriteImpl/DoOnce/Impulse.Noneref_Coder_BytesCoder_1"ref_Windowing_Windowing_1ñ
ref_PCollection_PCollection_6u
546WriteOutput/Write/WriteImpl/DoOnce/Map(decode).Noneref_Coder_FastPrimitivesCoder_6"ref_Windowing_Windowing_1á
ref_PCollection_PCollection_11e
-38WriteOutput/Write/WriteImpl/GroupByKey.Noneref_Coder_TupleCoder_10"ref_Windowing_Windowing_1ê
ref_PCollection_PCollection_9o
/40WriteOutput/Write/WriteImpl/WriteBundles.Noneref_Coder_FastPrimitivesCoder_6"ref_Windowing_Windowing_1Ä
ref_PCollection_PCollection_10^
'32WriteOutput/Write/WriteImpl/Pair.Noneref_Coder_TupleCoder_8"ref_Windowing_Windowing_1É
ref_PCollection_PCollection_2b
 25ReadInput/Read/EmitSource.None!ref_Coder__MemoizingPickleCoder_3"ref_Windowing_Windowing_1å
ref_PCollection_PCollection_12j
*35WriteOutput/Write/WriteImpl/Extract.Noneref_Coder_FastPrimitivesCoder_6"ref_Windowing_Windowing_1§
ref_PCollection_PCollection_5Ç
K68WriteOutput/Write/WriteImpl/DoOnce/FlatMap(<lambda at core.py:4094>).Noneref_Coder_BytesCoder_1"ref_Windowing_Windowing_1ï
ref_PCollection_PCollection_8t
;52WriteOutput/Write/WriteImpl/WindowInto(WindowIntoFn).Noneref_Coder_StrUtf8Coder_7"ref_Windowing_Windowing_1ê
ref_PCollection_PCollection_13n
.39WriteOutput/Write/WriteImpl/PreFinalize.Noneref_Coder_FastPrimitivesCoder_6"ref_Windowing_Windowing_1£
ref_PCollection_PCollection_3Å
H65ReadInput/Read/SDFBoundedSourceReader/ParDo(SDFBoundedSourceDoFn).Noneref_Coder_StrUtf8Coder_7"ref_Windowing_Windowing_1ï
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
ÀDeterministicFastPrimitivesCoderV2$eNptj7FLA1EMxuvQqYJIBXF1EF1a6CC4qijiYCcdQ96Z9kLf3T3eyzu4QdDNIZvx/7U9FRGa4UsI3+8LeR1+HuwM+tIjDFiUBI6wmhTNM8X000yPr0koVlxzEi5uMMk8csXCLaWrjedxZh929mYvdqr7kOv1yndcL6FPsHKg4y3UL6N7sEDvHRarP2A052Ll6Z/RJacjSEIBPDrypidPkYUesoQs037+1rsq+OltbHK47O6pMz2EnAgiedzchwV7CihlsnfdhXb9JTc1CC5Nh+0Mzi8su8kXRu9v4A=="9
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