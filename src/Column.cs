namespace codecrafters_sqlite;

using static System.Buffers.Binary.BinaryPrimitives;
using static System.Text.Encoding;

public enum SerialType {
    Null,
    Int8, Int16, Int24, Int32, Int48, Int64,
    Float64,
    Zero, One,
    Blob,
    Text
}

public record Column(SerialType Type, ReadOnlyMemory<byte> Content) {
    public static Column Parse(int serialType, ReadOnlyMemory<byte> stream) {
        static bool IsText(int serialType) => serialType >= 13 && serialType % 2 == 1;
        static int GetTextLen(int serialType) => (serialType - 13) / 2;

        return serialType switch {
            0 => new(SerialType.Null, ReadOnlyMemory<byte>.Empty),
            1 => new(SerialType.Int8, stream[..1]),
            2 => new(SerialType.Int16, stream[..2]),
            3 => new(SerialType.Int24, stream[..3]),
            4 => new(SerialType.Int32, stream[..4]),
            5 => new(SerialType.Int48, stream[..6]),
            6 => new(SerialType.Int64, stream[..8]),
            7 => new(SerialType.Float64, stream[..8]),
            8 => new(SerialType.Zero, ReadOnlyMemory<byte>.Empty),
            9 => new(SerialType.One, ReadOnlyMemory<byte>.Empty),
            var t when IsText(t) => new(SerialType.Text, stream[..GetTextLen(t)]),
            _ => throw new NotSupportedException($"Can't parse column with serial type {serialType}.")
        };
    }

    public byte ToByte() => Type switch {
        SerialType.Int8 => Content.Span[0],
        _ => throw new NotSupportedException($"Can't convert column with serial type {Type} to byte.")
    };

    public long ToLong() => Type switch {
        SerialType.Int16 => ReadInt16BigEndian(Content.Span),
        SerialType.Int24 => ReadInt24BigEndian(Content.Span),
        SerialType.Int64 => ReadInt64BigEndian(Content.Span),
        _ => throw new NotSupportedException($"Can't convert column with serial type {Type} to long.")
    };

    public string ToUtf8String() => Type switch {
        SerialType.Text => UTF8.GetString(Content.Span),
        _ => throw new NotSupportedException($"Can't convert column with serial type {Type} to UTF8 string.")
    };

    public IValue ToValue() => Type switch {
        SerialType.Null => new NullValue(),
        SerialType.Text => new StrValue(UTF8.GetString(Content.Span)),
        _ => throw new NotSupportedException($"Can't convert column with serial type {Type} to IValue.")
    };

    private static int ReadInt24BigEndian(ReadOnlySpan<byte> source) => (ReadInt16BigEndian(source) << 8) + source[2];
}
