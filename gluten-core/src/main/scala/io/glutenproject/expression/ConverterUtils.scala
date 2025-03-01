/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.glutenproject.expression

import io.glutenproject.backendsapi.BackendsApiManager
import io.glutenproject.execution.{BasicScanExecTransformer, BatchScanExecTransformer, FileSourceScanExecTransformer}
import io.glutenproject.substrait.`type`._
import io.glutenproject.substrait.rel.LocalFilesNode.ReadFileFormat
import io.substrait.proto.Type
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.optimizer._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.hive.HiveTableScanExecTransformer
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import java.util.Locale
import java.util.{ArrayList => JArrayList, List => JList}

import org.apache.spark.sql.execution.datasources.GlutenTextBasedScanWrapper
import org.apache.spark.sql.execution.datasources.v2.json.JsonScan
import org.apache.spark.sql.execution.datasources.v2.text.TextScan

import scala.collection.JavaConverters._

object ConverterUtils extends Logging {

  def getAttrFromExpr(fieldExpr: Expression, skipAlias: Boolean = false): AttributeReference = {
    fieldExpr match {
      case a: Cast =>
        getAttrFromExpr(a.child)
      case a: AggregateExpression =>
        getAttrFromExpr(a.aggregateFunction.children.head)
      case a: AttributeReference =>
        a
      case a: Alias =>
        if (skipAlias && a.child.isInstanceOf[AttributeReference]) {
          getAttrFromExpr(a.child)
        } else {
          a.toAttribute.asInstanceOf[AttributeReference]
        }
      case a: KnownFloatingPointNormalized =>
        getAttrFromExpr(a.child)
      case a: NormalizeNaNAndZero =>
        getAttrFromExpr(a.child)
      case c: Coalesce =>
        getAttrFromExpr(c.children.head)
      case i: IsNull =>
        getAttrFromExpr(i.child)
      case a: Add =>
        getAttrFromExpr(a.left)
      case s: Subtract =>
        getAttrFromExpr(s.left)
      case m: Multiply =>
        getAttrFromExpr(m.left)
      case d: Divide =>
        getAttrFromExpr(d.left)
      case u: Upper =>
        getAttrFromExpr(u.child)
      case ss: Substring =>
        getAttrFromExpr(ss.children.head)
      case other =>
        throw new UnsupportedOperationException(
          s"makeStructField is unable to parse from $other (${other.getClass}).")
    }
  }

  def normalizeColName(name: String): String = {
    val caseSensitive = SQLConf.get.caseSensitiveAnalysis
    if (caseSensitive) name else name.toLowerCase(Locale.ROOT)
  }

  def getShortAttributeName(attr: Attribute): String = {
    val name = normalizeColName(attr.name)
    val subIndex = name.indexOf("(")
    if (subIndex != -1) {
      name.substring(0, subIndex)
    } else {
      name
    }
  }

  def genColumnNameWithoutExprId(attr: Attribute): String = {
    getShortAttributeName(attr)
  }

  def genColumnNameWithExprId(attr: Attribute): String = {
    getShortAttributeName(attr) + "#" + attr.exprId.id
  }

  def collectAttributeTypeNodes(attributes: JList[Attribute]): JArrayList[TypeNode] = {
    collectAttributeTypeNodes(attributes.asScala)
  }

  def collectAttributeTypeNodes(attributes: Seq[Attribute]): JArrayList[TypeNode] = {
    val typeList = new JArrayList[TypeNode]()
    attributes.foreach(attr => typeList.add(getTypeNode(attr.dataType, attr.nullable)))
    typeList
  }

  def collectAttributeNamesWithExprId(attributes: JList[Attribute]): JArrayList[String] = {
    collectAttributeNamesWithExprId(attributes.asScala)
  }

  def collectAttributeNamesWithExprId(attributes: Seq[Attribute]): JArrayList[String] = {
    collectAttributeNamesDFS(attributes)(genColumnNameWithExprId)
  }

  // TODO: This is used only by `BasicScanExecTransformer`,
  //  perhaps we can remove this in the future and use `withExprId` version consistently.
  def collectAttributeNamesWithoutExprId(attributes: Seq[Attribute]): JArrayList[String] = {
    collectAttributeNamesDFS(attributes)(genColumnNameWithoutExprId)
  }

  private def collectAttributeNamesDFS(
    attributes: Seq[Attribute])(f: Attribute => String): JArrayList[String] = {
    val nameList = new JArrayList[String]()
    attributes.foreach(
      attr => {
        nameList.add(f(attr))
        if (BackendsApiManager.getSettings.supportStructType()) {
          attr.dataType match {
            case struct: StructType =>
              val nestedNames = collectStructFieldNames(struct)
              nameList.addAll(nestedNames)
            case _ =>
          }
        }
      }
    )
    nameList
  }

  def collectStructFieldNames(dataType: DataType): JArrayList[String] = {
    val nameList = new JArrayList[String]()
    dataType match {
      case structType: StructType =>
        structType.fields.foreach(
          field => {
            nameList.add(normalizeColName(field.name))
            val nestedNames = collectStructFieldNames(field.dataType)
            nameList.addAll(nestedNames)
          }
        )
      case _ =>
    }
    nameList
  }

  def isNullable(nullability: Type.Nullability): Boolean = {
    nullability == Type.Nullability.NULLABILITY_NULLABLE
  }

  def parseFromSubstraitType(substraitType: Type): (DataType, Boolean) = {
    substraitType.getKindCase match {
      case Type.KindCase.BOOL =>
        (BooleanType, isNullable(substraitType.getBool.getNullability))
      case Type.KindCase.I8 =>
        (ByteType, isNullable(substraitType.getI8.getNullability))
      case Type.KindCase.I16 =>
        (ShortType, isNullable(substraitType.getI16.getNullability))
      case Type.KindCase.I32 =>
        (IntegerType, isNullable(substraitType.getI32.getNullability))
      case Type.KindCase.I64 =>
        (LongType, isNullable(substraitType.getI64.getNullability))
      case Type.KindCase.FP32 =>
        (FloatType, isNullable(substraitType.getFp32.getNullability))
      case Type.KindCase.FP64 =>
        (DoubleType, isNullable(substraitType.getFp64.getNullability))
      case Type.KindCase.STRING =>
        (StringType, isNullable(substraitType.getString.getNullability))
      case Type.KindCase.BINARY =>
        (BinaryType, isNullable(substraitType.getBinary.getNullability))
      case Type.KindCase.TIMESTAMP =>
        (TimestampType, isNullable(substraitType.getTimestamp.getNullability))
      case Type.KindCase.DATE =>
        (DateType, isNullable(substraitType.getDate.getNullability))
      case Type.KindCase.DECIMAL =>
        val decimal = substraitType.getDecimal
        val precision = decimal.getPrecision
        val scale = decimal.getScale
        (DecimalType(precision, scale), isNullable(decimal.getNullability))
      case Type.KindCase.STRUCT =>
        val struct_ = substraitType.getStruct
        val fields = new JArrayList[StructField]
        for (typ <- struct_.getTypesList.asScala) {
          val (field, nullable) = parseFromSubstraitType(typ)
          fields.add(StructField("", field, nullable))
        }
        (StructType(fields),
          isNullable(substraitType.getStruct.getNullability))
      case Type.KindCase.LIST =>
        val list = substraitType.getList
        val (elementType, containsNull) = parseFromSubstraitType(list.getType)
        (ArrayType(elementType, containsNull),
          isNullable(substraitType.getList.getNullability))
      case Type.KindCase.MAP =>
        val map = substraitType.getMap
        val (keyType, _) = parseFromSubstraitType(map.getKey)
        val (valueType, valueContainsNull) = parseFromSubstraitType(map.getValue())
        (MapType(keyType, valueType, valueContainsNull),
          isNullable(substraitType.getMap.getNullability))
      case Type.KindCase.NOTHING =>
        (NullType, true)
      case unsupported =>
        throw new UnsupportedOperationException(s"Type $unsupported not supported.")
    }
  }

  def getTypeNode(datatype: DataType, nullable: Boolean): TypeNode = {
    datatype match {
      case BooleanType =>
        TypeBuilder.makeBoolean(nullable)
      case FloatType =>
        TypeBuilder.makeFP32(nullable)
      case DoubleType =>
        TypeBuilder.makeFP64(nullable)
      case LongType =>
        TypeBuilder.makeI64(nullable)
      case IntegerType =>
        TypeBuilder.makeI32(nullable)
      case ShortType =>
        TypeBuilder.makeI16(nullable)
      case ByteType =>
        TypeBuilder.makeI8(nullable)
      case StringType =>
        TypeBuilder.makeString(nullable)
      case BinaryType =>
        TypeBuilder.makeBinary(nullable)
      case DateType =>
        TypeBuilder.makeDate(nullable)
      case DecimalType() =>
        val decimalType = datatype.asInstanceOf[DecimalType]
        val precision = decimalType.precision
        val scale = decimalType.scale
        TypeBuilder.makeDecimal(nullable, precision, scale)
      case TimestampType =>
        TypeBuilder.makeTimestamp(nullable)
      case m: MapType =>
        TypeBuilder.makeMap(nullable, getTypeNode(m.keyType, nullable = false),
          getTypeNode(m.valueType, m.valueContainsNull))
      case a: ArrayType =>
        TypeBuilder.makeList(nullable, getTypeNode(a.elementType, a.containsNull))
      case s: StructType =>
        val fieldNodes = new JArrayList[TypeNode]
        val fieldNames = new JArrayList[String]
        for (structField <- s.fields) {
          fieldNodes.add(getTypeNode(structField.dataType, structField.nullable))
          fieldNames.add(structField.name)
        }
        TypeBuilder.makeStruct(nullable, fieldNodes, fieldNames)
      case _: NullType =>
        TypeBuilder.makeNothing()
      case unknown =>
        throw new UnsupportedOperationException(s"Type $unknown not supported.")
    }
  }

  def printBatch(cb: ColumnarBatch): Unit = {
    var batch = ""
    for (rowId <- 0 until cb.numRows()) {
      var row = ""
      for (colId <- 0 until cb.numCols()) {
        row += (cb.column(colId).getUTF8String(rowId) + " ")
      }
      batch += (row + "\n")
    }
    logWarning(s"batch:\n$batch")
  }

  def ifEquals(left: Seq[AttributeReference], right: Seq[NamedExpression]): Boolean = {
    if (left.size != right.size) return false
    for (i <- left.indices) {
      if (left(i).exprId != right(i).exprId) return false
    }
    true
  }

  override def toString: String = {
    s"ConverterUtils"
  }

  def powerOfTen(pow: Int): (String, Int, Int) = {
    val POWERS_OF_10: Array[(String, Int, Int)] = Array(
      ("1", 1, 0),
      ("10", 2, 0),
      ("100", 3, 0),
      ("1000", 4, 0),
      ("10000", 5, 0),
      ("100000", 6, 0),
      ("1000000", 7, 0),
      ("10000000", 8, 0),
      ("100000000", 9, 0),
      ("1000000000", 10, 0),
      ("10000000000", 11, 0),
      ("100000000000", 12, 0),
      ("1000000000000", 13, 0),
      ("10000000000000", 14, 0),
      ("100000000000000", 15, 0),
      ("1000000000000000", 16, 0),
      ("10000000000000000", 17, 0),
      ("100000000000000000", 18, 0),
      ("1000000000000000000", 19, 0),
      ("10000000000000000000", 20, 0),
      ("100000000000000000000", 21, 0),
      ("1000000000000000000000", 22, 0),
      ("10000000000000000000000", 23, 0),
      ("100000000000000000000000", 24, 0),
      ("1000000000000000000000000", 25, 0),
      ("10000000000000000000000000", 26, 0),
      ("100000000000000000000000000", 27, 0),
      ("1000000000000000000000000000", 28, 0),
      ("10000000000000000000000000000", 29, 0),
      ("100000000000000000000000000000", 30, 0),
      ("1000000000000000000000000000000", 31, 0),
      ("10000000000000000000000000000000", 32, 0),
      ("100000000000000000000000000000000", 33, 0),
      ("1000000000000000000000000000000000", 34, 0),
      ("10000000000000000000000000000000000", 35, 0),
      ("100000000000000000000000000000000000", 36, 0),
      ("1000000000000000000000000000000000000", 37, 0))
    POWERS_OF_10(pow)
  }

  // This enum is used to specify the function arg.
  object FunctionConfig extends Enumeration {
    type Config = Value
    val REQ, OPT, NON = Value
  }

  /**
   * Get the signature name of a type based on Substrait's definition in
   * https://substrait.io/extensions/#function-signature-compound-names.
   * @param dataType: the input data type.
   * @return the corresponding signature name.
   */
  def getTypeSigName(dataType: DataType): String = {
    dataType match {
      case BooleanType => // TODO: Not in Substrait yet.
        "bool"
      case ByteType => "i8"
      case ShortType => "i16"
      case IntegerType => "i32"
      case LongType => "i64"
      case FloatType => "fp32"
      case DoubleType => "fp64"
      case DateType => "date"
      case TimestampType => "ts"
      case StringType => "str"
      case BinaryType => "vbin"
      case DecimalType() =>
        val decimalType = dataType.asInstanceOf[DecimalType]
        val precision = decimalType.precision
        val scale = decimalType.scale
        // TODO: different with Substrait due to more details here.
        "dec<" + precision + "," + scale + ">"
      case ArrayType(_, _) =>
        "list"
      case StructType(fields) =>
        // TODO: different with Substrait due to more details here.
        var sigName = "struct<"
        var index = 0
        fields.foreach(field => {
          sigName = sigName.concat(getTypeSigName(field.dataType))
          sigName = sigName.concat(if (index < fields.length - 1) "," else "")
          index += 1
        })
        sigName = sigName.concat(">")
        sigName
      case MapType(_, _, _) =>
        "map"
      case CharType(_) =>
        "fchar"
      case NullType =>
        "nothing"
      case other =>
        throw new UnsupportedOperationException(s"Type $other not supported.")
    }
  }

  // This method is used to create a function name with input types.
  // The format would be aligned with that specified in Substrait.
  // The function name Format:
  // <function name>:<short_arg_type0>_<short_arg_type1>_..._<short_arg_typeN>
  def makeFuncName(funcName: String, datatypes: Seq[DataType],
                   config: FunctionConfig.Config = FunctionConfig.NON): String = {
    var typedFuncName = config match {
      case FunctionConfig.REQ =>
        funcName.concat(":req_")
      case FunctionConfig.OPT =>
        funcName.concat(":opt_")
      case FunctionConfig.NON =>
        funcName.concat(":")
      case other =>
        throw new UnsupportedOperationException(s"$other is not supported.")
    }

    for (idx <- datatypes.indices) {
      typedFuncName = typedFuncName.concat(getTypeSigName(datatypes(idx)))
      // For the last item, no need to append _.
      if (idx < datatypes.size - 1) {
        typedFuncName = typedFuncName.concat("_")
      }
    }
    typedFuncName
  }

  def convertJoinType(joinType: JoinType): String = {
    joinType match {
      case Inner =>
        "Inner"
      case FullOuter =>
        "Outer"
      case LeftOuter | RightOuter =>
        "Left"
      case LeftSemi =>
        "Semi"
      case LeftAnti =>
        "Anti"
      case other =>
        throw new UnsupportedOperationException(s"Unsupported join type: $other")
    }
  }

  def getFileFormat(scan: BasicScanExecTransformer): ReadFileFormat = {
    scan match {
      case f: BatchScanExecTransformer =>
        f.scan.getClass.getSimpleName match {
          case "OrcScan" => ReadFileFormat.OrcReadFormat
          case "ParquetScan" => ReadFileFormat.ParquetReadFormat
          case "DwrfScan" => ReadFileFormat.DwrfReadFormat
          case "ClickHouseScan" => ReadFileFormat.MergeTreeReadFormat
          case _ => ReadFileFormat.UnknownFormat
        }
      case f: FileSourceScanExecTransformer =>
        f.relation.fileFormat.getClass.getSimpleName match {
          case "OrcFileFormat" => ReadFileFormat.OrcReadFormat
          case "ParquetFileFormat" => ReadFileFormat.ParquetReadFormat
          case "DwrfFileFormat" => ReadFileFormat.DwrfReadFormat
          case "DeltaMergeTreeFileFormat" => ReadFileFormat.MergeTreeReadFormat
          case "CSVFileFormat" => ReadFileFormat.TextReadFormat
          case _ => ReadFileFormat.UnknownFormat
        }
      case f: HiveTableScanExecTransformer =>
        f.getScan match {
          case Some(fileScan) =>
            fileScan match {
              case scanWrapper: GlutenTextBasedScanWrapper =>
                scanWrapper.getScan match {
                  case _: JsonScan => ReadFileFormat.JsonReadFormat
                  case _: TextScan => ReadFileFormat.TextReadFormat
                  case _ => ReadFileFormat.UnknownFormat
                }
              case _ =>
                ReadFileFormat.UnknownFormat
            }
          case _ => ReadFileFormat.UnknownFormat
        }
      case _ => ReadFileFormat.UnknownFormat
    }
  }

  // A prefix used in the iterator path.
  final val ITERATOR_PREFIX = "iterator:"
}
