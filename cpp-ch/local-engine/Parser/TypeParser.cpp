#include <optional>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeSet.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <Parser/TypeParser.h>
#include <Parser/FunctionParser.h>
#include <Parser/SerializedPlanParser.h>
#include <Poco/StringTokenizer.h>
#include <Common/Exception.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_TYPE;
}
}

namespace local_engine
{
std::unordered_map<String, String> TypeParser::type_names_mapping
    = {{"BooleanType", "UInt8"},
       {"ByteType", "Int8"},
       {"ShortType", "Int16"},
       {"IntegerType", "Int32"},
       {"LongType", "Int64"},
       {"FloatType", "Float32"},
       {"DoubleType", "Float64"},
       {"StringType", "String"},
       {"DateType", "Date"}};

String TypeParser::getCHTypeName(const String & spark_type_name)
{
    auto it = type_names_mapping.find(spark_type_name);
    if (it == type_names_mapping.end())
        throw DB::Exception(DB::ErrorCodes::UNKNOWN_TYPE, "Unsupported substrait type: {}", spark_type_name);
    return it->second;
}

DB::DataTypePtr TypeParser::getCHTypeByName(const String & spark_type_name)
{
    auto ch_type_name = getCHTypeName(spark_type_name);
    return DB::DataTypeFactory::instance().get(ch_type_name);
}

DB::DataTypePtr TypeParser::parseType(const substrait::Type & substrait_type, std::list<String> * field_names)
{
    DB::DataTypePtr ch_type = nullptr;
    std::string_view field_name;
    if (field_names)
    {
        assert(!field_names->empty());
        field_name = field_names->front();
        field_names->pop_front();
    }

    if (substrait_type.has_bool_())
    {
        ch_type = DB::DataTypeFactory::instance().get("Bool");
        ch_type = tryWrapNullable(substrait_type.bool_().nullability(), ch_type);
    }
    else if (substrait_type.has_i8())
    {
        ch_type = std::make_shared<DB::DataTypeInt8>();
        ch_type = tryWrapNullable(substrait_type.i8().nullability(), ch_type);
    }
    else if (substrait_type.has_i16())
    {
        ch_type = std::make_shared<DB::DataTypeInt16>();
        ch_type = tryWrapNullable(substrait_type.i16().nullability(), ch_type);
    }
    else if (substrait_type.has_i32())
    {
        ch_type = std::make_shared<DB::DataTypeInt32>();
        ch_type = tryWrapNullable(substrait_type.i32().nullability(), ch_type);
    }
    else if (substrait_type.has_i64())
    {
        ch_type = std::make_shared<DB::DataTypeInt64>();
        ch_type = tryWrapNullable(substrait_type.i64().nullability(), ch_type);
    }
    else if (substrait_type.has_string())
    {
        ch_type = std::make_shared<DB::DataTypeString>();
        ch_type = tryWrapNullable(substrait_type.string().nullability(), ch_type);
    }
    else if (substrait_type.has_binary())
    {
        ch_type = std::make_shared<DB::DataTypeString>();
        ch_type = tryWrapNullable(substrait_type.binary().nullability(), ch_type);
    }
    else if (substrait_type.has_fixed_char())
    {
        const auto & fixed_char = substrait_type.fixed_char();
        ch_type = std::make_shared<DB::DataTypeFixedString>(fixed_char.length());
        ch_type = tryWrapNullable(fixed_char.nullability(), ch_type);
    }
    else if (substrait_type.has_fixed_binary())
    {
        const auto & fixed_binary = substrait_type.fixed_binary();
        ch_type = std::make_shared<DB::DataTypeFixedString>(fixed_binary.length());
        ch_type = tryWrapNullable(fixed_binary.nullability(), ch_type);
    }
    else if (substrait_type.has_fp32())
    {
        ch_type = std::make_shared<DB::DataTypeFloat32>();
        ch_type = tryWrapNullable(substrait_type.fp32().nullability(), ch_type);
    }
    else if (substrait_type.has_fp64())
    {
        ch_type = std::make_shared<DB::DataTypeFloat64>();
        ch_type = tryWrapNullable(substrait_type.fp64().nullability(), ch_type);
    }
    else if (substrait_type.has_timestamp())
    {
        ch_type = std::make_shared<DB::DataTypeDateTime64>(6);
        ch_type = tryWrapNullable(substrait_type.timestamp().nullability(), ch_type);
    }
    else if (substrait_type.has_date())
    {
        ch_type = std::make_shared<DB::DataTypeDate32>();
        ch_type = tryWrapNullable(substrait_type.date().nullability(), ch_type);
    }
    else if (substrait_type.has_decimal())
    {
        UInt32 precision = substrait_type.decimal().precision();
        UInt32 scale = substrait_type.decimal().scale();
        if (precision > DB::DataTypeDecimal128::maxPrecision())
            throw DB::Exception(DB::ErrorCodes::UNKNOWN_TYPE, "Spark doesn't support decimal type with precision {}", precision);
        ch_type = DB::createDecimal<DB::DataTypeDecimal>(precision, scale);
        ch_type = tryWrapNullable(substrait_type.decimal().nullability(), ch_type);
    }
    else if (substrait_type.has_struct_())
    {
        DB::DataTypes struct_field_types(substrait_type.struct_().types().size());
        DB::Strings struct_field_names;
        for (int i = 0; i < static_cast<int>(struct_field_types.size()); ++i)
        {
            if (field_names)
                struct_field_names.push_back(field_names->front());

            struct_field_types[i] = parseType(substrait_type.struct_().types()[i], field_names);
        }
        if (!struct_field_names.empty())
            ch_type = std::make_shared<DB::DataTypeTuple>(struct_field_types, struct_field_names);
        else
            ch_type = std::make_shared<DB::DataTypeTuple>(struct_field_types);
        ch_type = tryWrapNullable(substrait_type.struct_().nullability(), ch_type);
    }
    else if (substrait_type.has_list())
    {
        auto ch_nested_type = parseType(substrait_type.list().type());
        ch_type = std::make_shared<DB::DataTypeArray>(ch_nested_type);
        ch_type = tryWrapNullable(substrait_type.list().nullability(), ch_type);
    }
    else if (substrait_type.has_map())
    {
        if (substrait_type.map().key().has_nothing())
        {
            // special case
            ch_type = std::make_shared<DB::DataTypeMap>(std::make_shared<DB::DataTypeNothing>(), std::make_shared<DB::DataTypeNothing>());
            ch_type = tryWrapNullable(substrait_type.map().nullability(), ch_type);
        }
        else
        {
            auto ch_key_type = parseType(substrait_type.map().key());
            auto ch_val_type = parseType(substrait_type.map().value());
            ch_type = std::make_shared<DB::DataTypeMap>(ch_key_type, ch_val_type);
            ch_type = tryWrapNullable(substrait_type.map().nullability(), ch_type);
        }
    }
    else if (substrait_type.has_nothing())
    {
        ch_type = std::make_shared<DB::DataTypeNothing>();
        ch_type = tryWrapNullable(substrait::Type_Nullability::Type_Nullability_NULLABILITY_NULLABLE, ch_type);
    }
    else
        throw DB::Exception(DB::ErrorCodes::UNKNOWN_TYPE, "Spark doesn't support type {}", substrait_type.DebugString());

    /// TODO(taiyang-li): consider Time/IntervalYear/IntervalDay/TimestampTZ/UUID/VarChar/FixedBinary/UserDefined
    return ch_type;
}


DB::Block TypeParser::buildBlockFromNamedStruct(const substrait::NamedStruct & struct_)
{
    DB::ColumnsWithTypeAndName internal_cols;
    internal_cols.reserve(struct_.names_size());
    std::list<std::string> field_names;
    for (int i = 0; i < struct_.names_size(); ++i)
    {
        field_names.emplace_back(struct_.names(i));
    }

    for (int i = 0; i < struct_.struct_().types_size(); ++i)
    {
        auto name = field_names.front();
        const auto & type = struct_.struct_().types(i);
        auto data_type = parseType(type, &field_names);
        // This is a partial aggregate data column.
        // It's type is special, must be a struct type contains all arguments types.
        Poco::StringTokenizer name_parts(name, "#");
        if (name_parts.count() >= 4)
        {
            auto nested_data_type = DB::removeNullable(data_type);
            const auto * tuple_type = typeid_cast<const DB::DataTypeTuple *>(nested_data_type.get());
            if (!tuple_type)
            {
                throw DB::Exception(DB::ErrorCodes::UNKNOWN_TYPE, "Tuple is expected, but got {}", data_type->getName());
            }

            auto args_types = tuple_type->getElements();
            AggregateFunctionProperties properties;
            auto tmp_ctx = DB::Context::createCopy(SerializedPlanParser::global_context);
            SerializedPlanParser tmp_plan_parser(tmp_ctx);
            auto function_parser = FunctionParserFactory::instance().get(name_parts[3], &tmp_plan_parser);
            auto agg_function_name = function_parser->getCHFunctionName(args_types);
            data_type = AggregateFunctionFactory::instance()
                            .get(agg_function_name, args_types, function_parser->getDefaultFunctionParameters(), properties)
                            ->getStateType();
        }
        internal_cols.push_back(ColumnWithTypeAndName(data_type, name));
    }
    DB::Block res(std::move(internal_cols));
    return res;
}

bool TypeParser::isTypeMatched(const substrait::Type & substrait_type, const DataTypePtr & ch_type)
{
    return parseType(substrait_type)->equals(*ch_type);
}

DB::DataTypePtr TypeParser::tryWrapNullable(substrait::Type_Nullability nullable, DB::DataTypePtr nested_type)
{
    if (nullable == substrait::Type_Nullability::Type_Nullability_NULLABILITY_NULLABLE && !nested_type->isNullable())
        return std::make_shared<DB::DataTypeNullable>(nested_type);
    return nested_type;
}
}
