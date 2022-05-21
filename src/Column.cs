namespace codecrafters_sqlite;

using static System.Text.Encoding;

public enum SerialType {
    Null,
    Int8,
    Text
}

public record Column(SerialType Type, ReadOnlyMemory<byte> Content) {
    public static Column Parse(int serialType, ReadOnlyMemory<byte> stream) {
        static bool IsText(int serialType) => serialType >= 13 && serialType % 2 == 1;
        static int GetTextLen(int serialType) => (serialType - 13) / 2;

        return serialType switch {
            0 => new(SerialType.Null, ReadOnlyMemory<byte>.Empty),
            1 => new(SerialType.Int8, stream[0..1]),
            var t when IsText(t) => new(SerialType.Text, stream[..GetTextLen(t)]),
            _ => throw new NotSupportedException($"Can't parse column with serial type {serialType}.")
        };
    }

    public string Render() => Type switch {
        SerialType.Null => "NULL",
        SerialType.Int8 => ToByte().ToString(),
        SerialType.Text => ToUtf8String(),
        _ => throw new NotSupportedException($"Can't print column with serial type {Type}."),
    };

    public byte ToByte() => Type == SerialType.Int8
        ? Content.Span[0]
        : throw new NotSupportedException($"Can't convert column with serial type {Type} to byte.");

    public string ToUtf8String() => Type == SerialType.Text
        ? UTF8.GetString(Content.Span)
        : throw new NotSupportedException($"Can't convert column with serial type {Type} to UTF8 string.");
}
