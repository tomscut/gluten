#pragma once

#include <memory>
#include <jni.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Parser/CHColumnToSparkRow.h>
#include <Parser/TypeParser.h>
#include <base/StringRef.h>
#include <base/types.h>
#include <jni/jni_common.h>
#include <substrait/type.pb.h>
#include <Common/JNIUtils.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_TYPE;
    extern const int CANNOT_PARSE_PROTOBUF_SCHEMA;
}
}
namespace local_engine
{
using namespace DB;
using namespace std;
struct SparkRowToCHColumnHelper
{
    DataTypes data_types;
    Block header;
    MutableColumns mutable_columns;
    UInt64 rows;

    SparkRowToCHColumnHelper(vector<string> & names, vector<string> & types) : data_types(names.size())
    {
        assert(names.size() == types.size());

        ColumnsWithTypeAndName columns(names.size());
        for (size_t i = 0; i < names.size(); ++i)
        {
            data_types[i] = parseType(types[i]);
            columns[i] = ColumnWithTypeAndName(data_types[i], names[i]);
        }

        header = Block(columns);
        resetMutableColumns();
    }

    ~SparkRowToCHColumnHelper() = default;

    void resetMutableColumns()
    {
        rows = 0;
        mutable_columns = header.mutateColumns();
    }

    static DataTypePtr parseType(const string & type)
    {
        auto substrait_type = std::make_unique<substrait::Type>();
        auto ok = substrait_type->ParseFromString(type);
        if (!ok)
            throw Exception(ErrorCodes::CANNOT_PARSE_PROTOBUF_SCHEMA, "Parse substrait::Type from string failed");
        return TypeParser::parseType(*substrait_type);
    }
};

class SparkRowToCHColumn
{
public:
    static jclass spark_row_interator_class;
    static jmethodID spark_row_interator_hasNext;
    static jmethodID spark_row_interator_next;
    static jmethodID spark_row_iterator_nextBatch;

    // case 1: rows are batched (this is often directly converted from Block)
    static std::unique_ptr<Block> convertSparkRowInfoToCHColumn(const SparkRowInfo & spark_row_info, const Block & header);

    // case 2: provided with a sequence of spark UnsafeRow, convert them to a Block
    static Block * convertSparkRowItrToCHColumn(jobject java_iter, vector<string> & names, vector<string> & types)
    {
        SparkRowToCHColumnHelper helper(names, types);

        GET_JNIENV(env)
        while (safeCallBooleanMethod(env, java_iter, spark_row_interator_hasNext))
        {
            jobject rows_buf = safeCallObjectMethod(env, java_iter, spark_row_iterator_nextBatch);
            auto * rows_buf_ptr = static_cast<char *>(env->GetDirectBufferAddress(rows_buf));
            int len = *(reinterpret_cast<int *>(rows_buf_ptr));

            // len = -1 means reaching the buf's end.
            // len = 0 indicates no columns in the this row. e.g. count(1)/count(*)
            while (len >= 0)
            {
                rows_buf_ptr += 4;
                appendSparkRowToCHColumn(helper, rows_buf_ptr, len);

                rows_buf_ptr += len;
                len = *(reinterpret_cast<int *>(rows_buf_ptr));
            }

            // Try to release reference.
            env->DeleteLocalRef(rows_buf);
        }
        return getBlock(helper);
    }

    static void freeBlock(Block * block)
    {
        delete block;
        block = nullptr;
    }

private:
    static void appendSparkRowToCHColumn(SparkRowToCHColumnHelper & helper, char * buffer, int32_t length);
    static Block * getBlock(SparkRowToCHColumnHelper & helper);
};

class VariableLengthDataReader
{
public:
    explicit VariableLengthDataReader(const DataTypePtr & type_);
    virtual ~VariableLengthDataReader() = default;

    virtual Field read(const char * buffer, size_t length) const;
    virtual StringRef readUnalignedBytes(const char * buffer, size_t length) const;

private:
    virtual Field readDecimal(const char * buffer, size_t length) const;
    virtual Field readString(const char * buffer, size_t length) const;
    virtual Field readArray(const char * buffer, size_t length) const;
    virtual Field readMap(const char * buffer, size_t length) const;
    virtual Field readStruct(const char * buffer, size_t length) const;

    const DataTypePtr type;
    const DataTypePtr type_without_nullable;
    const WhichDataType which;
};

class FixedLengthDataReader
{
public:
    explicit FixedLengthDataReader(const DB::DataTypePtr & type_);
    virtual ~FixedLengthDataReader() = default;

    virtual Field read(const char * buffer) const;
    virtual StringRef unsafeRead(const char * buffer) const;

private:
    const DB::DataTypePtr type;
    const DB::DataTypePtr type_without_nullable;
    const DB::WhichDataType which;
    size_t value_size;
};
class SparkRowReader
{
public:
    explicit SparkRowReader(const DataTypes & field_types_)
        : field_types(field_types_)
        , num_fields(field_types.size())
        , bit_set_width_in_bytes(static_cast<int32_t>(calculateBitSetWidthInBytes(num_fields)))
        , field_offsets(num_fields)
        , support_raw_datas(num_fields)
        , is_big_endians_in_spark_row(num_fields)
        , fixed_length_data_readers(num_fields)
        , variable_length_data_readers(num_fields)
    {
        for (size_t ordinal = 0; ordinal < num_fields; ++ordinal)
        {
            const auto type_without_nullable = removeNullable(field_types[ordinal]);
            field_offsets[ordinal] = bit_set_width_in_bytes + ordinal * 8L;
            support_raw_datas[ordinal] = BackingDataLengthCalculator::isDataTypeSupportRawData(type_without_nullable);
            is_big_endians_in_spark_row[ordinal] = BackingDataLengthCalculator::isBigEndianInSparkRow(type_without_nullable);
            if (BackingDataLengthCalculator::isFixedLengthDataType(type_without_nullable))
                fixed_length_data_readers[ordinal] = std::make_shared<FixedLengthDataReader>(field_types[ordinal]);
            else if (BackingDataLengthCalculator::isVariableLengthDataType(type_without_nullable))
                variable_length_data_readers[ordinal] = std::make_shared<VariableLengthDataReader>(field_types[ordinal]);
            else
                throw Exception(ErrorCodes::UNKNOWN_TYPE, "SparkRowReader doesn't support type {}", field_types[ordinal]->getName());
        }
    }

    const DataTypes & getFieldTypes() const { return field_types; }

    bool supportRawData(size_t ordinal) const
    {
        assertIndexIsValid(ordinal);
        return support_raw_datas[ordinal];
    }

    bool isBigEndianInSparkRow(size_t ordinal) const
    {
        assertIndexIsValid(ordinal);
        return is_big_endians_in_spark_row[ordinal];
    }

    std::shared_ptr<FixedLengthDataReader> getFixedLengthDataReader(int ordinal) const
    {
        assertIndexIsValid(ordinal);
        return fixed_length_data_readers[ordinal];
    }

    std::shared_ptr<VariableLengthDataReader> getVariableLengthDataReader(int ordinal) const
    {
        assertIndexIsValid(ordinal);
        return variable_length_data_readers[ordinal];
    }

    void assertIndexIsValid([[maybe_unused]] size_t index) const
    {
        assert(index >= 0);
        assert(index < num_fields);
    }

    bool isNullAt(size_t ordinal) const
    {
        assertIndexIsValid(ordinal);
        return isBitSet(buffer, ordinal);
    }

    const char * getRawDataForFixedNumber(size_t ordinal) const
    {
        assertIndexIsValid(ordinal);
        return reinterpret_cast<const char *>(getFieldOffset(ordinal));
    }

    int8_t getByte(size_t ordinal) const
    {
        assertIndexIsValid(ordinal);
        return *reinterpret_cast<const int8_t *>(getFieldOffset(ordinal));
    }

    uint8_t getUnsignedByte(size_t ordinal) const
    {
        assertIndexIsValid(ordinal);
        return *reinterpret_cast<const uint8_t *>(getFieldOffset(ordinal));
    }

    int16_t getShort(size_t ordinal) const
    {
        assertIndexIsValid(ordinal);
        return *reinterpret_cast<const int16_t *>(getFieldOffset(ordinal));
    }

    uint16_t getUnsignedShort(size_t ordinal) const
    {
        assertIndexIsValid(ordinal);
        return *reinterpret_cast<const uint16_t *>(getFieldOffset(ordinal));
    }

    int32_t getInt(size_t ordinal) const
    {
        assertIndexIsValid(ordinal);
        return *reinterpret_cast<const int32_t *>(getFieldOffset(ordinal));
    }

    uint32_t getUnsignedInt(size_t ordinal) const
    {
        assertIndexIsValid(ordinal);
        return *reinterpret_cast<const uint32_t *>(getFieldOffset(ordinal));
    }

    int64_t getLong(size_t ordinal) const
    {
        assertIndexIsValid(ordinal);
        return *reinterpret_cast<const int64_t *>(getFieldOffset(ordinal));
    }

    float_t getFloat(size_t ordinal) const
    {
        assertIndexIsValid(ordinal);
        return *reinterpret_cast<const float_t *>(getFieldOffset(ordinal));
    }

    double_t getDouble(size_t ordinal) const
    {
        assertIndexIsValid(ordinal);
        return *reinterpret_cast<const double_t *>(getFieldOffset(ordinal));
    }

    StringRef getString(size_t ordinal) const
    {
        assertIndexIsValid(ordinal);
        int64_t offset_and_size = getLong(ordinal);
        int32_t offset = static_cast<int32_t>(offset_and_size >> 32);
        int32_t size = static_cast<int32_t>(offset_and_size);
        return StringRef(reinterpret_cast<const char *>(this->buffer + offset), size);
    }

    int32_t getStringSize(size_t ordinal) const
    {
        assertIndexIsValid(ordinal);
        return static_cast<int32_t>(getLong(ordinal));
    }

    void pointTo(const char * buffer_, int32_t length_)
    {
        buffer = buffer_;
        length = length_;
    }

    StringRef getStringRef(size_t ordinal) const
    {
        assertIndexIsValid(ordinal);
        if (!support_raw_datas[ordinal])
            throw Exception(
                ErrorCodes::UNKNOWN_TYPE, "SparkRowReader::getStringRef doesn't support type {}", field_types[ordinal]->getName());

        if (isNullAt(ordinal))
            return StringRef();

        const auto & fixed_length_data_reader = fixed_length_data_readers[ordinal];
        const auto & variable_length_data_reader = variable_length_data_readers[ordinal];
        if (fixed_length_data_reader)
            return fixed_length_data_reader->unsafeRead(getFieldOffset(ordinal));
        else if (variable_length_data_reader)
        {
            int64_t offset_and_size = 0;
            memcpy(&offset_and_size, buffer + bit_set_width_in_bytes + ordinal * 8, 8);
            const int64_t offset = BackingDataLengthCalculator::extractOffset(offset_and_size);
            const int64_t size = BackingDataLengthCalculator::extractSize(offset_and_size);
            return variable_length_data_reader->readUnalignedBytes(buffer + offset, size);
        }
        else
            throw Exception(
                ErrorCodes::UNKNOWN_TYPE, "SparkRowReader::getStringRef doesn't support type {}", field_types[ordinal]->getName());
    }

    Field getField(size_t ordinal) const
    {
        assertIndexIsValid(ordinal);

        if (isNullAt(ordinal))
            return Null{};

        const auto & fixed_length_data_reader = fixed_length_data_readers[ordinal];
        const auto & variable_length_data_reader = variable_length_data_readers[ordinal];

        if (fixed_length_data_reader)
            return fixed_length_data_reader->read(getFieldOffset(ordinal));
        else if (variable_length_data_reader)
        {
            int64_t offset_and_size = 0;
            memcpy(&offset_and_size, buffer + bit_set_width_in_bytes + ordinal * 8, 8);
            const int64_t offset = BackingDataLengthCalculator::extractOffset(offset_and_size);
            const int64_t size = BackingDataLengthCalculator::extractSize(offset_and_size);
            return variable_length_data_reader->read(buffer + offset, size);
        }
        else
            throw Exception(ErrorCodes::UNKNOWN_TYPE, "SparkRowReader::getField doesn't support type {}", field_types[ordinal]->getName());
    }

private:
    const char * getFieldOffset(size_t ordinal) const { return buffer + field_offsets[ordinal]; }

    const DataTypes field_types;
    const size_t num_fields;
    const int32_t bit_set_width_in_bytes;
    std::vector<int64_t> field_offsets;
    std::vector<bool> support_raw_datas;
    std::vector<bool> is_big_endians_in_spark_row;
    std::vector<std::shared_ptr<FixedLengthDataReader>> fixed_length_data_readers;
    std::vector<std::shared_ptr<VariableLengthDataReader>> variable_length_data_readers;

    const char * buffer;
    int32_t length;
};

}
