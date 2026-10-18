
Û¯
Å
!ref_AppliedPTransform_ReadLines_2€
%
#beam:transform:generic_composite:v1&ref_AppliedPTransform_ReadLines-Read_3"%
Noneref_PCollection_PCollection_3*	ReadLines:%ref_Environment_default_environment_1B1
python_type"apache_beam.io.textio.ReadFromText
µ
9ref_AppliedPTransform_WriteAvro-Write-WriteImpl-DoOnce_33˜
"
 beam:transform:pickled_python:v1Aref_AppliedPTransform_WriteAvro-Write-WriteImpl-DoOnce-Impulse_34Yref_AppliedPTransform_WriteAvro-Write-WriteImpl-DoOnce-FlatMap-lambda-at-core-py-4094-_35Href_AppliedPTransform_WriteAvro-Write-WriteImpl-DoOnce-MaybeReshuffle_36Eref_AppliedPTransform_WriteAvro-Write-WriteImpl-DoOnce-Map-decode-_37"&
Noneref_PCollection_PCollection_20* WriteAvro/Write/WriteImpl/DoOnce:%ref_Environment_default_environment_1B1
python_type"apache_beam.transforms.core.Create
·
Kref_AppliedPTransform_WriteAvro-Write-WriteImpl-WindowInto-WindowIntoFn-_39ë

beam:transform:window_into:v1&
Noneref_PCollection_PCollection_17"&
Noneref_PCollection_PCollection_22*2WriteAvro/Write/WriteImpl/WindowInto(WindowIntoFn)2ã
beam:display_data:labelled:v1j
Transform Function(apache_beam.transforms.core.WindowIntoFn2fn:&apache_beam.transforms.core.WindowInto:%ref_Environment_default_environment_1B5
python_type&apache_beam.transforms.core.WindowInto
◊
@ref_AppliedPTransform_WriteLines-Write-WriteImpl-WriteBundles_18í

beam:transform:pardo:v13
python_side_input0ref_PCollection_PCollection_7%
Noneref_PCollection_PCollection_8"%
Noneref_PCollection_PCollection_9*'WriteLines/Write/WriteImpl/WriteBundles2Ñ
beam:display_data:labelled:v1c
Transform Function&apache_beam.io.iobase._WriteBundleDoFn2fn:!apache_beam.transforms.core.ParDo2b
beam:display_data:labelled:v1A
compressionauto2compression:apache_beam.io.textio._TextSink2ì
beam:display_data:labelled:v1r
File Pattern3data/out/lines/-%(shard_num)05d-of-%(num_shards)05d2file_pattern:apache_beam.io.textio._TextSink2o
beam:display_data:labelled:v1N
Append Trailing New Lines2append_newline:apache_beam.io.textio._TextSink:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
«
<ref_AppliedPTransform_WriteRecords-Write-WriteImpl-DoOnce_55Ü
"
 beam:transform:pickled_python:v1Dref_AppliedPTransform_WriteRecords-Write-WriteImpl-DoOnce-Impulse_56\ref_AppliedPTransform_WriteRecords-Write-WriteImpl-DoOnce-FlatMap-lambda-at-core-py-4094-_57Kref_AppliedPTransform_WriteRecords-Write-WriteImpl-DoOnce-MaybeReshuffle_58Href_AppliedPTransform_WriteRecords-Write-WriteImpl-DoOnce-Map-decode-_59"&
Noneref_PCollection_PCollection_34*#WriteRecords/Write/WriteImpl/DoOnce:%ref_Environment_default_environment_1B1
python_type"apache_beam.transforms.core.Create
Í
Yref_AppliedPTransform_ReadAvro-Read-SDFBoundedSourceReader-ParDo-SDFBoundedSourceDoFn-_29å

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_16"&
Noneref_PCollection_PCollection_17*@ReadAvro/Read/SDFBoundedSourceReader/ParDo(SDFBoundedSourceDoFn)2à
beam:display_data:labelled:v1g
Transform Function*apache_beam.io.iobase.SDFBoundedSourceDoFn2fn:!apache_beam.transforms.core.ParDo2~
beam:display_data:labelled:v1]
File Patterndata/in/avro/*.avro2file_pattern:*apache_beam.io.iobase.SDFBoundedSourceDoFn2r
beam:display_data:labelled:v1Q
Compression Typeauto2compression:*apache_beam.io.iobase.SDFBoundedSourceDoFn2Ñ
beam:display_data:labelled:v1c
source%apache_beam.io.avroio._FastAvroSource2source:*apache_beam.io.iobase.SDFBoundedSourceDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
Ï
Zref_AppliedPTransform_WriteLines-Write-WriteImpl-DoOnce-FlatMap-lambda-at-core-py-4094-_13ç

beam:transform:pardo:v1%
Noneref_PCollection_PCollection_4"%
Noneref_PCollection_PCollection_5*CWriteLines/Write/WriteImpl/DoOnce/FlatMap(<lambda at core.py:4094>)2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2t
beam:display_data:labelled:v1S
Transform Function<lambda>2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
Û
(ref_AppliedPTransform_WriteAvro-Write_31∆
%
#beam:transform:generic_composite:v12ref_AppliedPTransform_WriteAvro-Write-WriteImpl_32&
Noneref_PCollection_PCollection_17"&
Noneref_PCollection_PCollection_28*WriteAvro/Write2o
beam:display_data:labelled:v1N
sink#apache_beam.io.avroio._FastAvroSink2sink:apache_beam.io.iobase.Write2n
beam:display_data:labelled:v1M
compressionuncompressed2compression:#apache_beam.io.avroio._FastAvroSink2ö
beam:display_data:labelled:v1y
File Pattern6data/out/avro/part-%(shard_num)05d-of-%(num_shards)05d2file_pattern:#apache_beam.io.avroio._FastAvroSink2]
beam:display_data:labelled:v1<
codecdeflate2codec:#apache_beam.io.avroio._FastAvroSink2◊
beam:display_data:labelled:v1µ
schema~{'type': 'record', 'name': 'Report', 'fields': [{'name': 'id', 'type': 'long'}, {'name': 'amount', 'type': ['null', 'long']}]}2schema:#apache_beam.io.avroio._FastAvroSink:%ref_Environment_default_environment_1B*
python_typeapache_beam.io.iobase.Write
¬
Eref_AppliedPTransform_WriteRecords-Write-WriteImpl-InitializeWrite_60¯

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_34"&
Noneref_PCollection_PCollection_35*,WriteRecords/Write/WriteImpl/InitializeWrite2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2t
beam:display_data:labelled:v1S
Transform Function<lambda>2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
„
2ref_AppliedPTransform_WriteAvro-Write-WriteImpl_32¨
%
#beam:transform:generic_composite:v19ref_AppliedPTransform_WriteAvro-Write-WriteImpl-DoOnce_33Bref_AppliedPTransform_WriteAvro-Write-WriteImpl-InitializeWrite_38Kref_AppliedPTransform_WriteAvro-Write-WriteImpl-WindowInto-WindowIntoFn-_39?ref_AppliedPTransform_WriteAvro-Write-WriteImpl-WriteBundles_407ref_AppliedPTransform_WriteAvro-Write-WriteImpl-Pair_41=ref_AppliedPTransform_WriteAvro-Write-WriteImpl-GroupByKey_42:ref_AppliedPTransform_WriteAvro-Write-WriteImpl-Extract_43>ref_AppliedPTransform_WriteAvro-Write-WriteImpl-PreFinalize_44@ref_AppliedPTransform_WriteAvro-Write-WriteImpl-FinalizeWrite_45&
Noneref_PCollection_PCollection_17"&
Noneref_PCollection_PCollection_28*WriteAvro/Write/WriteImpl:%ref_Environment_default_environment_1B.
python_typeapache_beam.io.iobase.WriteImpl
—	
Yref_AppliedPTransform_ReadLines-Read-SDFBoundedSourceReader-ParDo-SDFBoundedSourceDoFn-_7Û

beam:transform:pardo:v1%
Noneref_PCollection_PCollection_2"%
Noneref_PCollection_PCollection_3*AReadLines/Read/SDFBoundedSourceReader/ParDo(SDFBoundedSourceDoFn)2à
beam:display_data:labelled:v1g
Transform Function*apache_beam.io.iobase.SDFBoundedSourceDoFn2fn:!apache_beam.transforms.core.ParDo2~
beam:display_data:labelled:v1]
File Patterndata/in/lines/*.txt2file_pattern:*apache_beam.io.iobase.SDFBoundedSourceDoFn2r
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
›
)ref_AppliedPTransform_AppliedPTransform_1Ø
 !ref_AppliedPTransform_ReadLines_2"ref_AppliedPTransform_WriteLines_8!ref_AppliedPTransform_ReadAvro_24"ref_AppliedPTransform_WriteAvro_30$ref_AppliedPTransform_ReadRecords_46%ref_AppliedPTransform_WriteRecords_52*)ref_AppliedPTransform_AppliedPTransform_1:%ref_Environment_default_environment_1
Á
Nref_AppliedPTransform_WriteRecords-Write-WriteImpl-WindowInto-WindowIntoFn-_61î

beam:transform:window_into:v1&
Noneref_PCollection_PCollection_31"&
Noneref_PCollection_PCollection_36*5WriteRecords/Write/WriteImpl/WindowInto(WindowIntoFn)2ã
beam:display_data:labelled:v1j
Transform Function(apache_beam.transforms.core.WindowIntoFn2fn:&apache_beam.transforms.core.WindowInto:%ref_Environment_default_environment_1B5
python_type&apache_beam.transforms.core.WindowInto
Ò
Kref_AppliedPTransform_WriteRecords-Write-WriteImpl-DoOnce-MaybeReshuffle_58°
"
 beam:transform:pickled_python:v1&
Noneref_PCollection_PCollection_33"&
Noneref_PCollection_PCollection_33*2WriteRecords/Write/WriteImpl/DoOnce/MaybeReshuffle:%ref_Environment_default_environment_1BP
python_typeAapache_beam.transforms.core.Create.expand.<locals>.MaybeReshuffle
ì
>ref_AppliedPTransform_WriteLines-Write-WriteImpl-GroupByKey_20–
 
beam:transform:group_by_key:v1&
Noneref_PCollection_PCollection_10"&
Noneref_PCollection_PCollection_11*%WriteLines/Write/WriteImpl/GroupByKeyB5
python_type&apache_beam.transforms.core.GroupByKey
†
4ref_AppliedPTransform_ReadRecords-Read-EmitSource_49Á

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_29"&
Noneref_PCollection_PCollection_30*ReadRecords/Read/EmitSource2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2t
beam:display_data:labelled:v1S
Transform Function<lambda>2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
·
Aref_AppliedPTransform_WriteLines-Write-WriteImpl-FinalizeWrite_23õ

beam:transform:pardo:v13
python_side_input0ref_PCollection_PCollection_74
python_side_input1ref_PCollection_PCollection_124
python_side_input2ref_PCollection_PCollection_13%
Noneref_PCollection_PCollection_6"&
Noneref_PCollection_PCollection_14*(WriteLines/Write/WriteImpl/FinalizeWrite2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2{
beam:display_data:labelled:v1Z
Transform Function_finalize_write2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
Î
Iref_AppliedPTransform_WriteLines-Write-WriteImpl-DoOnce-MaybeReshuffle_14ù
"
 beam:transform:pickled_python:v1%
Noneref_PCollection_PCollection_5"%
Noneref_PCollection_PCollection_5*0WriteLines/Write/WriteImpl/DoOnce/MaybeReshuffle:%ref_Environment_default_environment_1BP
python_typeAapache_beam.transforms.core.Create.expand.<locals>.MaybeReshuffle
Î
!ref_AppliedPTransform_ReadAvro_24≈
%
#beam:transform:generic_composite:v1&ref_AppliedPTransform_ReadAvro-Read_25"&
Noneref_PCollection_PCollection_17*ReadAvro2y
beam:display_data:labelled:v1X
File Patterndata/in/avro/*.avro2file_pattern:%apache_beam.io.avroio._FastAvroSource2m
beam:display_data:labelled:v1L
Compression Typeauto2compression:%apache_beam.io.avroio._FastAvroSource:%ref_Environment_default_environment_1B1
python_type"apache_beam.io.avroio.ReadFromAvro
ó
@ref_AppliedPTransform_WriteRecords-Write-WriteImpl-GroupByKey_64“
 
beam:transform:group_by_key:v1&
Noneref_PCollection_PCollection_38"&
Noneref_PCollection_PCollection_39*'WriteRecords/Write/WriteImpl/GroupByKeyB5
python_type&apache_beam.transforms.core.GroupByKey
•
>ref_AppliedPTransform_WriteAvro-Write-WriteImpl-PreFinalize_44‚

beam:transform:pardo:v14
python_side_input0ref_PCollection_PCollection_214
python_side_input1ref_PCollection_PCollection_26&
Noneref_PCollection_PCollection_20"&
Noneref_PCollection_PCollection_27*%WriteAvro/Write/WriteImpl/PreFinalize2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2y
beam:display_data:labelled:v1X
Transform Function_pre_finalize2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
õ
=ref_AppliedPTransform_ReadAvro-Read-SDFBoundedSourceReader_28Ÿ
%
#beam:transform:generic_composite:v1Yref_AppliedPTransform_ReadAvro-Read-SDFBoundedSourceReader-ParDo-SDFBoundedSourceDoFn-_29&
Noneref_PCollection_PCollection_16"&
Noneref_PCollection_PCollection_17*$ReadAvro/Read/SDFBoundedSourceReader2~
beam:display_data:labelled:v1]
File Patterndata/in/avro/*.avro2file_pattern:*apache_beam.io.iobase.SDFBoundedSourceDoFn2r
beam:display_data:labelled:v1Q
Compression Typeauto2compression:*apache_beam.io.iobase.SDFBoundedSourceDoFn2Ü
beam:display_data:labelled:v1e
source%apache_beam.io.avroio._FastAvroSource2source:,apache_beam.io.iobase.SDFBoundedSourceReader:%ref_Environment_default_environment_1B;
python_type,apache_beam.io.iobase.SDFBoundedSourceReader
≤
=ref_AppliedPTransform_WriteRecords-Write-WriteImpl-Extract_65

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_39"&
Noneref_PCollection_PCollection_40*$WriteRecords/Write/WriteImpl/Extract2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2t
beam:display_data:labelled:v1S
Transform Function<lambda>2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
¨
:ref_AppliedPTransform_WriteAvro-Write-WriteImpl-Extract_43Ì

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_25"&
Noneref_PCollection_PCollection_26*!WriteAvro/Write/WriteImpl/Extract2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2t
beam:display_data:labelled:v1S
Transform Function<lambda>2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
í
$ref_AppliedPTransform_ReadRecords_46È
%
#beam:transform:generic_composite:v1)ref_AppliedPTransform_ReadRecords-Read_47"&
Noneref_PCollection_PCollection_31*ReadRecords:%ref_Environment_default_environment_1B9
python_type*apache_beam.io.tfrecordio.ReadFromTFRecord
Ú
\ref_AppliedPTransform_WriteRecords-Write-WriteImpl-DoOnce-FlatMap-lambda-at-core-py-4094-_57ë

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_32"&
Noneref_PCollection_PCollection_33*EWriteRecords/Write/WriteImpl/DoOnce/FlatMap(<lambda at core.py:4094>)2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2t
beam:display_data:labelled:v1S
Transform Function<lambda>2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
ô
&ref_AppliedPTransform_ReadLines-Read_3Ó

beam:transform:read:v1.ref_AppliedPTransform_ReadLines-Read-Impulse_41ref_AppliedPTransform_ReadLines-Read-EmitSource_5=ref_AppliedPTransform_ReadLines-Read-SDFBoundedSourceReader_6"%
Noneref_PCollection_PCollection_3*ReadLines/Read2u
beam:display_data:labelled:v1T
Read Source!apache_beam.io.textio._TextSource2source:apache_beam.io.iobase.Read2u
beam:display_data:labelled:v1T
File Patterndata/in/lines/*.txt2file_pattern:!apache_beam.io.textio._TextSource2i
beam:display_data:labelled:v1H
Compression Typeauto2compression:!apache_beam.io.textio._TextSource2o
beam:display_data:labelled:v1N
Strip Trailing New Lines2strip_newline:!apache_beam.io.textio._TextSource2a
beam:display_data:labelled:v1@
Buffer Size(Ä@2buffer_size:!apache_beam.io.textio._TextSource2z
beam:display_data:labelled:v1Y
Coder&apache_beam.coders.coders.StrUtf8Coder2coder:!apache_beam.io.textio._TextSource:%ref_Environment_default_environment_1B)
python_typeapache_beam.io.iobase.Read
¶
7ref_AppliedPTransform_WriteAvro-Write-WriteImpl-Pair_41Í

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_23"&
Noneref_PCollection_PCollection_24*WriteAvro/Write/WriteImpl/Pair2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2t
beam:display_data:labelled:v1S
Transform Function<lambda>2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
º
Cref_AppliedPTransform_WriteLines-Write-WriteImpl-InitializeWrite_16Ù

beam:transform:pardo:v1%
Noneref_PCollection_PCollection_6"%
Noneref_PCollection_PCollection_7**WriteLines/Write/WriteImpl/InitializeWrite2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2t
beam:display_data:labelled:v1S
Transform Function<lambda>2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
Á
Cref_AppliedPTransform_WriteRecords-Write-WriteImpl-FinalizeWrite_67ü

beam:transform:pardo:v14
python_side_input0ref_PCollection_PCollection_354
python_side_input1ref_PCollection_PCollection_404
python_side_input2ref_PCollection_PCollection_41&
Noneref_PCollection_PCollection_34"&
Noneref_PCollection_PCollection_42**WriteRecords/Write/WriteImpl/FinalizeWrite2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2{
beam:display_data:labelled:v1Z
Transform Function_finalize_write2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
√
.ref_AppliedPTransform_ReadLines-Read-Impulse_4ê

beam:transform:impulse:v1"%
Noneref_PCollection_PCollection_1*ReadLines/Read/ImpulseB2
python_type#apache_beam.transforms.core.Impulse
Ω
%ref_AppliedPTransform_WriteRecords_52ì
%
#beam:transform:generic_composite:v1+ref_AppliedPTransform_WriteRecords-Write_53&
Noneref_PCollection_PCollection_31"&
Noneref_PCollection_PCollection_42*WriteRecords:%ref_Environment_default_environment_1B8
python_type)apache_beam.io.tfrecordio.WriteToTFRecord
ö
1ref_AppliedPTransform_ReadAvro-Read-EmitSource_27‰

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_15"&
Noneref_PCollection_PCollection_16*ReadAvro/Read/EmitSource2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2t
beam:display_data:labelled:v1S
Transform Function<lambda>2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
¨
"ref_AppliedPTransform_WriteLines_8Ö
%
#beam:transform:generic_composite:v1(ref_AppliedPTransform_WriteLines-Write_9%
Noneref_PCollection_PCollection_3"&
Noneref_PCollection_PCollection_14*
WriteLines:%ref_Environment_default_environment_1B0
python_type!apache_beam.io.textio.WriteToText
È
Aref_AppliedPTransform_WriteAvro-Write-WriteImpl-DoOnce-Impulse_34£

beam:transform:impulse:v1"&
Noneref_PCollection_PCollection_18*(WriteAvro/Write/WriteImpl/DoOnce/ImpulseB2
python_type#apache_beam.transforms.core.Impulse
·
Lref_AppliedPTransform_WriteLines-Write-WriteImpl-WindowInto-WindowIntoFn-_17ê

beam:transform:window_into:v1%
Noneref_PCollection_PCollection_3"%
Noneref_PCollection_PCollection_8*3WriteLines/Write/WriteImpl/WindowInto(WindowIntoFn)2ã
beam:display_data:labelled:v1j
Transform Function(apache_beam.transforms.core.WindowIntoFn2fn:&apache_beam.transforms.core.WindowInto:%ref_Environment_default_environment_1B5
python_type&apache_beam.transforms.core.WindowInto
Ó
)ref_AppliedPTransform_ReadRecords-Read_47¿

beam:transform:read:v11ref_AppliedPTransform_ReadRecords-Read-Impulse_484ref_AppliedPTransform_ReadRecords-Read-EmitSource_49@ref_AppliedPTransform_ReadRecords-Read-SDFBoundedSourceReader_50"&
Noneref_PCollection_PCollection_31*ReadRecords/Read2}
beam:display_data:labelled:v1\
Read Source)apache_beam.io.tfrecordio._TFRecordSource2source:apache_beam.io.iobase.Read2{
beam:display_data:labelled:v1Z
File Patterndata/in/records/*2file_pattern:)apache_beam.io.tfrecordio._TFRecordSource2q
beam:display_data:labelled:v1P
Compression Typeauto2compression:)apache_beam.io.tfrecordio._TFRecordSource:%ref_Environment_default_environment_1B)
python_typeapache_beam.io.iobase.Read
Ô
Dref_AppliedPTransform_WriteRecords-Write-WriteImpl-DoOnce-Impulse_56¶

beam:transform:impulse:v1"&
Noneref_PCollection_PCollection_32*+WriteRecords/Write/WriteImpl/DoOnce/ImpulseB2
python_type#apache_beam.transforms.core.Impulse
Ú
"ref_AppliedPTransform_WriteAvro_30À
%
#beam:transform:generic_composite:v1(ref_AppliedPTransform_WriteAvro-Write_31&
Noneref_PCollection_PCollection_17"&
Noneref_PCollection_PCollection_28*	WriteAvro2n
beam:display_data:labelled:v1M
compressionuncompressed2compression:#apache_beam.io.avroio._FastAvroSink2ö
beam:display_data:labelled:v1y
File Pattern6data/out/avro/part-%(shard_num)05d-of-%(num_shards)05d2file_pattern:#apache_beam.io.avroio._FastAvroSink2]
beam:display_data:labelled:v1<
codecdeflate2codec:#apache_beam.io.avroio._FastAvroSink2◊
beam:display_data:labelled:v1µ
schema~{'type': 'record', 'name': 'Report', 'fields': [{'name': 'id', 'type': 'long'}, {'name': 'amount', 'type': ['null', 'long']}]}2schema:#apache_beam.io.avroio._FastAvroSink:%ref_Environment_default_environment_1B0
python_type!apache_beam.io.avroio.WriteToAvro
´
Aref_AppliedPTransform_WriteRecords-Write-WriteImpl-PreFinalize_66Â

beam:transform:pardo:v14
python_side_input0ref_PCollection_PCollection_354
python_side_input1ref_PCollection_PCollection_40&
Noneref_PCollection_PCollection_34"&
Noneref_PCollection_PCollection_41*(WriteRecords/Write/WriteImpl/PreFinalize2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2y
beam:display_data:labelled:v1X
Transform Function_pre_finalize2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
À
+ref_AppliedPTransform_WriteRecords-Write_53õ
%
#beam:transform:generic_composite:v15ref_AppliedPTransform_WriteRecords-Write-WriteImpl_54&
Noneref_PCollection_PCollection_31"&
Noneref_PCollection_PCollection_42*WriteRecords/Write2s
beam:display_data:labelled:v1R
sink'apache_beam.io.tfrecordio._TFRecordSink2sink:apache_beam.io.iobase.Write2j
beam:display_data:labelled:v1I
compressionauto2compression:'apache_beam.io.tfrecordio._TFRecordSink2¢
beam:display_data:labelled:v1Ä
File Pattern9data/out/records/part-%(shard_num)05d-of-%(num_shards)05d2file_pattern:'apache_beam.io.tfrecordio._TFRecordSink:%ref_Environment_default_environment_1B*
python_typeapache_beam.io.iobase.Write
Ç	
=ref_AppliedPTransform_ReadLines-Read-SDFBoundedSourceReader_6¿
%
#beam:transform:generic_composite:v1Yref_AppliedPTransform_ReadLines-Read-SDFBoundedSourceReader-ParDo-SDFBoundedSourceDoFn-_7%
Noneref_PCollection_PCollection_2"%
Noneref_PCollection_PCollection_3*%ReadLines/Read/SDFBoundedSourceReader2~
beam:display_data:labelled:v1]
File Patterndata/in/lines/*.txt2file_pattern:*apache_beam.io.iobase.SDFBoundedSourceDoFn2r
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
ï
(ref_AppliedPTransform_WriteLines-Write_9Ë
%
#beam:transform:generic_composite:v13ref_AppliedPTransform_WriteLines-Write-WriteImpl_10%
Noneref_PCollection_PCollection_3"&
Noneref_PCollection_PCollection_14*WriteLines/Write2k
beam:display_data:labelled:v1J
sinkapache_beam.io.textio._TextSink2sink:apache_beam.io.iobase.Write2b
beam:display_data:labelled:v1A
compressionauto2compression:apache_beam.io.textio._TextSink2ì
beam:display_data:labelled:v1r
File Pattern3data/out/lines/-%(shard_num)05d-of-%(num_shards)05d2file_pattern:apache_beam.io.textio._TextSink2o
beam:display_data:labelled:v1N
Append Trailing New Lines2append_newline:apache_beam.io.textio._TextSink:%ref_Environment_default_environment_1B*
python_typeapache_beam.io.iobase.Write
Æ
;ref_AppliedPTransform_WriteLines-Write-WriteImpl-Extract_21Ó

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_11"&
Noneref_PCollection_PCollection_12*"WriteLines/Write/WriteImpl/Extract2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2t
beam:display_data:labelled:v1S
Transform Function<lambda>2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
•
?ref_AppliedPTransform_WriteLines-Write-WriteImpl-PreFinalize_22·

beam:transform:pardo:v13
python_side_input0ref_PCollection_PCollection_74
python_side_input1ref_PCollection_PCollection_12%
Noneref_PCollection_PCollection_6"&
Noneref_PCollection_PCollection_13*&WriteLines/Write/WriteImpl/PreFinalize2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2y
beam:display_data:labelled:v1X
Transform Function_pre_finalize2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
Ì
3ref_AppliedPTransform_WriteLines-Write-WriteImpl_10µ
%
#beam:transform:generic_composite:v1:ref_AppliedPTransform_WriteLines-Write-WriteImpl-DoOnce_11Cref_AppliedPTransform_WriteLines-Write-WriteImpl-InitializeWrite_16Lref_AppliedPTransform_WriteLines-Write-WriteImpl-WindowInto-WindowIntoFn-_17@ref_AppliedPTransform_WriteLines-Write-WriteImpl-WriteBundles_188ref_AppliedPTransform_WriteLines-Write-WriteImpl-Pair_19>ref_AppliedPTransform_WriteLines-Write-WriteImpl-GroupByKey_20;ref_AppliedPTransform_WriteLines-Write-WriteImpl-Extract_21?ref_AppliedPTransform_WriteLines-Write-WriteImpl-PreFinalize_22Aref_AppliedPTransform_WriteLines-Write-WriteImpl-FinalizeWrite_23%
Noneref_PCollection_PCollection_3"&
Noneref_PCollection_PCollection_14*WriteLines/Write/WriteImpl:%ref_Environment_default_environment_1B.
python_typeapache_beam.io.iobase.WriteImpl
∫
:ref_AppliedPTransform_WriteLines-Write-WriteImpl-DoOnce_11˚
"
 beam:transform:pickled_python:v1Bref_AppliedPTransform_WriteLines-Write-WriteImpl-DoOnce-Impulse_12Zref_AppliedPTransform_WriteLines-Write-WriteImpl-DoOnce-FlatMap-lambda-at-core-py-4094-_13Iref_AppliedPTransform_WriteLines-Write-WriteImpl-DoOnce-MaybeReshuffle_14Fref_AppliedPTransform_WriteLines-Write-WriteImpl-DoOnce-Map-decode-_15"%
Noneref_PCollection_PCollection_6*!WriteLines/Write/WriteImpl/DoOnce:%ref_Environment_default_environment_1B1
python_type"apache_beam.transforms.core.Create
ß
8ref_AppliedPTransform_WriteLines-Write-WriteImpl-Pair_19Í

beam:transform:pardo:v1%
Noneref_PCollection_PCollection_9"&
Noneref_PCollection_PCollection_10*WriteLines/Write/WriteImpl/Pair2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2t
beam:display_data:labelled:v1S
Transform Function<lambda>2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
Î
Href_AppliedPTransform_WriteAvro-Write-WriteImpl-DoOnce-MaybeReshuffle_36û
"
 beam:transform:pickled_python:v1&
Noneref_PCollection_PCollection_19"&
Noneref_PCollection_PCollection_19*/WriteAvro/Write/WriteImpl/DoOnce/MaybeReshuffle:%ref_Environment_default_environment_1BP
python_typeAapache_beam.transforms.core.Create.expand.<locals>.MaybeReshuffle
≥
?ref_AppliedPTransform_WriteAvro-Write-WriteImpl-WriteBundles_40Ô

beam:transform:pardo:v14
python_side_input0ref_PCollection_PCollection_21&
Noneref_PCollection_PCollection_22"&
Noneref_PCollection_PCollection_23*&WriteAvro/Write/WriteImpl/WriteBundles2Ñ
beam:display_data:labelled:v1c
Transform Function&apache_beam.io.iobase._WriteBundleDoFn2fn:!apache_beam.transforms.core.ParDo2n
beam:display_data:labelled:v1M
compressionuncompressed2compression:#apache_beam.io.avroio._FastAvroSink2ö
beam:display_data:labelled:v1y
File Pattern6data/out/avro/part-%(shard_num)05d-of-%(num_shards)05d2file_pattern:#apache_beam.io.avroio._FastAvroSink2]
beam:display_data:labelled:v1<
codecdeflate2codec:#apache_beam.io.avroio._FastAvroSink2◊
beam:display_data:labelled:v1µ
schema~{'type': 'record', 'name': 'Report', 'fields': [{'name': 'id', 'type': 'long'}, {'name': 'amount', 'type': ['null', 'long']}]}2schema:#apache_beam.io.avroio._FastAvroSink:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
ô
1ref_AppliedPTransform_ReadLines-Read-EmitSource_5„

beam:transform:pardo:v1%
Noneref_PCollection_PCollection_1"%
Noneref_PCollection_PCollection_2*ReadLines/Read/EmitSource2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2t
beam:display_data:labelled:v1S
Transform Function<lambda>2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
Ñ
Bref_AppliedPTransform_WriteRecords-Write-WriteImpl-WriteBundles_62Ω

beam:transform:pardo:v14
python_side_input0ref_PCollection_PCollection_35&
Noneref_PCollection_PCollection_36"&
Noneref_PCollection_PCollection_37*)WriteRecords/Write/WriteImpl/WriteBundles2Ñ
beam:display_data:labelled:v1c
Transform Function&apache_beam.io.iobase._WriteBundleDoFn2fn:!apache_beam.transforms.core.ParDo2j
beam:display_data:labelled:v1I
compressionauto2compression:'apache_beam.io.tfrecordio._TFRecordSink2¢
beam:display_data:labelled:v1Ä
File Pattern9data/out/records/part-%(shard_num)05d-of-%(num_shards)05d2file_pattern:'apache_beam.io.tfrecordio._TFRecordSink:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
Ú
\ref_AppliedPTransform_ReadRecords-Read-SDFBoundedSourceReader-ParDo-SDFBoundedSourceDoFn-_51ë

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_30"&
Noneref_PCollection_PCollection_31*CReadRecords/Read/SDFBoundedSourceReader/ParDo(SDFBoundedSourceDoFn)2à
beam:display_data:labelled:v1g
Transform Function*apache_beam.io.iobase.SDFBoundedSourceDoFn2fn:!apache_beam.transforms.core.ParDo2|
beam:display_data:labelled:v1[
File Patterndata/in/records/*2file_pattern:*apache_beam.io.iobase.SDFBoundedSourceDoFn2r
beam:display_data:labelled:v1Q
Compression Typeauto2compression:*apache_beam.io.iobase.SDFBoundedSourceDoFn2à
beam:display_data:labelled:v1g
source)apache_beam.io.tfrecordio._TFRecordSource2source:*apache_beam.io.iobase.SDFBoundedSourceDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
Í
Bref_AppliedPTransform_WriteLines-Write-WriteImpl-DoOnce-Impulse_12£

beam:transform:impulse:v1"%
Noneref_PCollection_PCollection_4*)WriteLines/Write/WriteImpl/DoOnce/ImpulseB2
python_type#apache_beam.transforms.core.Impulse
√
.ref_AppliedPTransform_ReadAvro-Read-Impulse_26ê

beam:transform:impulse:v1"&
Noneref_PCollection_PCollection_15*ReadAvro/Read/ImpulseB2
python_type#apache_beam.transforms.core.Impulse
Ñ
5ref_AppliedPTransform_WriteRecords-Write-WriteImpl_54 
%
#beam:transform:generic_composite:v1<ref_AppliedPTransform_WriteRecords-Write-WriteImpl-DoOnce_55Eref_AppliedPTransform_WriteRecords-Write-WriteImpl-InitializeWrite_60Nref_AppliedPTransform_WriteRecords-Write-WriteImpl-WindowInto-WindowIntoFn-_61Bref_AppliedPTransform_WriteRecords-Write-WriteImpl-WriteBundles_62:ref_AppliedPTransform_WriteRecords-Write-WriteImpl-Pair_63@ref_AppliedPTransform_WriteRecords-Write-WriteImpl-GroupByKey_64=ref_AppliedPTransform_WriteRecords-Write-WriteImpl-Extract_65Aref_AppliedPTransform_WriteRecords-Write-WriteImpl-PreFinalize_66Cref_AppliedPTransform_WriteRecords-Write-WriteImpl-FinalizeWrite_67&
Noneref_PCollection_PCollection_31"&
Noneref_PCollection_PCollection_42*WriteRecords/Write/WriteImpl:%ref_Environment_default_environment_1B.
python_typeapache_beam.io.iobase.WriteImpl
¿
Fref_AppliedPTransform_WriteLines-Write-WriteImpl-DoOnce-Map-decode-_15ı

beam:transform:pardo:v1%
Noneref_PCollection_PCollection_5"%
Noneref_PCollection_PCollection_6*-WriteLines/Write/WriteImpl/DoOnce/Map(decode)2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2r
beam:display_data:labelled:v1Q
Transform Functiondecode2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
·
@ref_AppliedPTransform_WriteAvro-Write-WriteImpl-FinalizeWrite_45ú

beam:transform:pardo:v14
python_side_input0ref_PCollection_PCollection_214
python_side_input1ref_PCollection_PCollection_264
python_side_input2ref_PCollection_PCollection_27&
Noneref_PCollection_PCollection_20"&
Noneref_PCollection_PCollection_28*'WriteAvro/Write/WriteImpl/FinalizeWrite2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2{
beam:display_data:labelled:v1Z
Transform Function_finalize_write2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
¿
Eref_AppliedPTransform_WriteAvro-Write-WriteImpl-DoOnce-Map-decode-_37ˆ

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_19"&
Noneref_PCollection_PCollection_20*,WriteAvro/Write/WriteImpl/DoOnce/Map(decode)2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2r
beam:display_data:labelled:v1Q
Transform Functiondecode2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
’
&ref_AppliedPTransform_ReadAvro-Read_25™

beam:transform:read:v1.ref_AppliedPTransform_ReadAvro-Read-Impulse_261ref_AppliedPTransform_ReadAvro-Read-EmitSource_27=ref_AppliedPTransform_ReadAvro-Read-SDFBoundedSourceReader_28"&
Noneref_PCollection_PCollection_17*ReadAvro/Read2y
beam:display_data:labelled:v1X
Read Source%apache_beam.io.avroio._FastAvroSource2source:apache_beam.io.iobase.Read2y
beam:display_data:labelled:v1X
File Patterndata/in/avro/*.avro2file_pattern:%apache_beam.io.avroio._FastAvroSource2m
beam:display_data:labelled:v1L
Compression Typeauto2compression:%apache_beam.io.avroio._FastAvroSource:%ref_Environment_default_environment_1B)
python_typeapache_beam.io.iobase.Read
º
Bref_AppliedPTransform_WriteAvro-Write-WriteImpl-InitializeWrite_38ı

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_20"&
Noneref_PCollection_PCollection_21*)WriteAvro/Write/WriteImpl/InitializeWrite2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2t
beam:display_data:labelled:v1S
Transform Function<lambda>2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
ë
=ref_AppliedPTransform_WriteAvro-Write-WriteImpl-GroupByKey_42œ
 
beam:transform:group_by_key:v1&
Noneref_PCollection_PCollection_24"&
Noneref_PCollection_PCollection_25*$WriteAvro/Write/WriteImpl/GroupByKeyB5
python_type&apache_beam.transforms.core.GroupByKey
∆
Href_AppliedPTransform_WriteRecords-Write-WriteImpl-DoOnce-Map-decode-_59˘

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_33"&
Noneref_PCollection_PCollection_34*/WriteRecords/Write/WriteImpl/DoOnce/Map(decode)2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2r
beam:display_data:labelled:v1Q
Transform Functiondecode2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
Ï
Yref_AppliedPTransform_WriteAvro-Write-WriteImpl-DoOnce-FlatMap-lambda-at-core-py-4094-_35é

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_18"&
Noneref_PCollection_PCollection_19*BWriteAvro/Write/WriteImpl/DoOnce/FlatMap(<lambda at core.py:4094>)2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2t
beam:display_data:labelled:v1S
Transform Function<lambda>2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDo
¶
@ref_AppliedPTransform_ReadRecords-Read-SDFBoundedSourceReader_50·
%
#beam:transform:generic_composite:v1\ref_AppliedPTransform_ReadRecords-Read-SDFBoundedSourceReader-ParDo-SDFBoundedSourceDoFn-_51&
Noneref_PCollection_PCollection_30"&
Noneref_PCollection_PCollection_31*'ReadRecords/Read/SDFBoundedSourceReader2|
beam:display_data:labelled:v1[
File Patterndata/in/records/*2file_pattern:*apache_beam.io.iobase.SDFBoundedSourceDoFn2r
beam:display_data:labelled:v1Q
Compression Typeauto2compression:*apache_beam.io.iobase.SDFBoundedSourceDoFn2ä
beam:display_data:labelled:v1i
source)apache_beam.io.tfrecordio._TFRecordSource2source:,apache_beam.io.iobase.SDFBoundedSourceReader:%ref_Environment_default_environment_1B;
python_type,apache_beam.io.iobase.SDFBoundedSourceReader
…
1ref_AppliedPTransform_ReadRecords-Read-Impulse_48ì

beam:transform:impulse:v1"&
Noneref_PCollection_PCollection_29*ReadRecords/Read/ImpulseB2
python_type#apache_beam.transforms.core.Impulse
¨
:ref_AppliedPTransform_WriteRecords-Write-WriteImpl-Pair_63Ì

beam:transform:pardo:v1&
Noneref_PCollection_PCollection_37"&
Noneref_PCollection_PCollection_38*!WriteRecords/Write/WriteImpl/Pair2ç
beam:display_data:labelled:v1l
Transform Function/apache_beam.transforms.core.CallableWrapperDoFn2fn:!apache_beam.transforms.core.ParDo2t
beam:display_data:labelled:v1S
Transform Function<lambda>2fn:/apache_beam.transforms.core.CallableWrapperDoFn:%ref_Environment_default_environment_1B0
python_type!apache_beam.transforms.core.ParDoÅ
ref_PCollection_PCollection_38_
(33WriteRecords/Write/WriteImpl/Pair.Noneref_Coder_TupleCoder_8"ref_Windowing_Windowing_1™
ref_PCollection_PCollection_17á
G64ReadAvro/Read/SDFBoundedSourceReader/ParDo(SDFBoundedSourceDoFn).Noneref_Coder_FastPrimitivesCoder_6"ref_Windowing_Windowing_1è
ref_PCollection_PCollection_9n
.39WriteLines/Write/WriteImpl/WriteBundles.Noneref_Coder_FastPrimitivesCoder_6"ref_Windowing_Windowing_1ë
ref_PCollection_PCollection_41o
/40WriteRecords/Write/WriteImpl/PreFinalize.Noneref_Coder_FastPrimitivesCoder_6"ref_Windowing_Windowing_1É
ref_PCollection_PCollection_2b
 25ReadLines/Read/EmitSource.None!ref_Coder__MemoizingPickleCoder_3"ref_Windowing_Windowing_1
ref_PCollection_PCollection_10]
&31WriteLines/Write/WriteImpl/Pair.Noneref_Coder_TupleCoder_8"ref_Windowing_Windowing_1É
ref_PCollection_PCollection_16a
24ReadAvro/Read/EmitSource.None!ref_Coder__MemoizingPickleCoder_3"ref_Windowing_Windowing_1õ
ref_PCollection_PCollection_22y
950WriteAvro/Write/WriteImpl/WindowInto(WindowIntoFn).Noneref_Coder_FastPrimitivesCoder_6"ref_Windowing_Windowing_1í
ref_PCollection_PCollection_7q
142WriteLines/Write/WriteImpl/InitializeWrite.Noneref_Coder_FastPrimitivesCoder_6"ref_Windowing_Windowing_1¶
ref_PCollection_PCollection_33É
L69WriteRecords/Write/WriteImpl/DoOnce/FlatMap(<lambda at core.py:4094>).Noneref_Coder_BytesCoder_1"ref_Windowing_Windowing_1ä
ref_PCollection_PCollection_14h
/40WriteLines/Write/WriteImpl/FinalizeWrite.Noneref_Coder_StrUtf8Coder_7"ref_Windowing_Windowing_1ç
ref_PCollection_PCollection_40k
+36WriteRecords/Write/WriteImpl/Extract.Noneref_Coder_FastPrimitivesCoder_6"ref_Windowing_Windowing_1Ö
ref_PCollection_PCollection_25c
+36WriteAvro/Write/WriteImpl/GroupByKey.Noneref_Coder_TupleCoder_10"ref_Windowing_Windowing_1~
ref_PCollection_PCollection_24\
%30WriteAvro/Write/WriteImpl/Pair.Noneref_Coder_TupleCoder_8"ref_Windowing_Windowing_1Ü
ref_PCollection_PCollection_11d
,37WriteLines/Write/WriteImpl/GroupByKey.Noneref_Coder_TupleCoder_10"ref_Windowing_Windowing_1î
ref_PCollection_PCollection_8s
:51WriteLines/Write/WriteImpl/WindowInto(WindowIntoFn).Noneref_Coder_StrUtf8Coder_7"ref_Windowing_Windowing_1ã
ref_PCollection_PCollection_12i
)34WriteLines/Write/WriteImpl/Extract.Noneref_Coder_FastPrimitivesCoder_6"ref_Windowing_Windowing_1£
ref_PCollection_PCollection_5Å
J67WriteLines/Write/WriteImpl/DoOnce/FlatMap(<lambda at core.py:4094>).Noneref_Coder_BytesCoder_1"ref_Windowing_Windowing_1è
ref_PCollection_PCollection_23m
-38WriteAvro/Write/WriteImpl/WriteBundles.Noneref_Coder_FastPrimitivesCoder_6"ref_Windowing_Windowing_1è
ref_PCollection_PCollection_13m
-38WriteLines/Write/WriteImpl/PreFinalize.Noneref_Coder_FastPrimitivesCoder_6"ref_Windowing_Windowing_1à
ref_PCollection_PCollection_39f
.39WriteRecords/Write/WriteImpl/GroupByKey.Noneref_Coder_TupleCoder_10"ref_Windowing_Windowing_1â
ref_PCollection_PCollection_28g
.39WriteAvro/Write/WriteImpl/FinalizeWrite.Noneref_Coder_StrUtf8Coder_7"ref_Windowing_Windowing_1ò
ref_PCollection_PCollection_34v
647WriteRecords/Write/WriteImpl/DoOnce/Map(decode).Noneref_Coder_FastPrimitivesCoder_6"ref_Windowing_Windowing_1≠
ref_PCollection_PCollection_31ä
J67ReadRecords/Read/SDFBoundedSourceReader/ParDo(SDFBoundedSourceDoFn).Noneref_Coder_FastPrimitivesCoder_6"ref_Windowing_Windowing_1å
ref_PCollection_PCollection_42j
142WriteRecords/Write/WriteImpl/FinalizeWrite.Noneref_Coder_StrUtf8Coder_7"ref_Windowing_Windowing_1x
ref_PCollection_PCollection_29V
24ReadRecords/Read/Impulse.Noneref_Coder_BytesCoder_1"ref_Windowing_Windowing_1u
ref_PCollection_PCollection_15S
21ReadAvro/Read/Impulse.Noneref_Coder_BytesCoder_1"ref_Windowing_Windowing_1ï
ref_PCollection_PCollection_20s
344WriteAvro/Write/WriteImpl/DoOnce/Map(decode).Noneref_Coder_FastPrimitivesCoder_6"ref_Windowing_Windowing_1ï
ref_PCollection_PCollection_6t
445WriteLines/Write/WriteImpl/DoOnce/Map(decode).Noneref_Coder_FastPrimitivesCoder_6"ref_Windowing_Windowing_1ä
ref_PCollection_PCollection_26h
(33WriteAvro/Write/WriteImpl/Extract.Noneref_Coder_FastPrimitivesCoder_6"ref_Windowing_Windowing_1é
ref_PCollection_PCollection_27l
,37WriteAvro/Write/WriteImpl/PreFinalize.Noneref_Coder_FastPrimitivesCoder_6"ref_Windowing_Windowing_1à
ref_PCollection_PCollection_18f
/40WriteAvro/Write/WriteImpl/DoOnce/Impulse.Noneref_Coder_BytesCoder_1"ref_Windowing_Windowing_1ã
ref_PCollection_PCollection_32i
243WriteRecords/Write/WriteImpl/DoOnce/Impulse.Noneref_Coder_BytesCoder_1"ref_Windowing_Windowing_1£
ref_PCollection_PCollection_3Å
H65ReadLines/Read/SDFBoundedSourceReader/ParDo(SDFBoundedSourceDoFn).Noneref_Coder_StrUtf8Coder_7"ref_Windowing_Windowing_1ï
ref_PCollection_PCollection_35s
344WriteRecords/Write/WriteImpl/InitializeWrite.Noneref_Coder_FastPrimitivesCoder_6"ref_Windowing_Windowing_1u
ref_PCollection_PCollection_1T
22ReadLines/Read/Impulse.Noneref_Coder_BytesCoder_1"ref_Windowing_Windowing_1£
ref_PCollection_PCollection_19Ä
I66WriteAvro/Write/WriteImpl/DoOnce/FlatMap(<lambda at core.py:4094>).Noneref_Coder_BytesCoder_1"ref_Windowing_Windowing_1û
ref_PCollection_PCollection_36|
<53WriteRecords/Write/WriteImpl/WindowInto(WindowIntoFn).Noneref_Coder_FastPrimitivesCoder_6"ref_Windowing_Windowing_1í
ref_PCollection_PCollection_37p
041WriteRecords/Write/WriteImpl/WriteBundles.Noneref_Coder_FastPrimitivesCoder_6"ref_Windowing_Windowing_1à
ref_PCollection_PCollection_4g
041WriteLines/Write/WriteImpl/DoOnce/Impulse.Noneref_Coder_BytesCoder_1"ref_Windowing_Windowing_1Ü
ref_PCollection_PCollection_30d
"27ReadRecords/Read/EmitSource.None!ref_Coder__MemoizingPickleCoder_3"ref_Windowing_Windowing_1í
ref_PCollection_PCollection_21p
041WriteAvro/Write/WriteImpl/InitializeWrite.Noneref_Coder_FastPrimitivesCoder_6"ref_Windowing_Windowing_1ï
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