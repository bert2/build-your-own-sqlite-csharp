namespace codecrafters_sqlite;

public record Record(Column[] Columns) {
    /// <summary>
    /// Reads SQLite's "Record Format" as mentioned here:
    /// https://www.sqlite.org/fileformat.html#record_format
    /// </summary>
    public static Record Parse(ReadOnlyMemory<byte> stream) => new(ParseColumns(stream).ToArray());

    public Column this[int i] => Columns[i];

    private static IEnumerable<Column> ParseColumns(ReadOnlyMemory<byte> stream) {
        var (headerSize, headerOffset) = Varint.Parse(stream);
        var contentOffset = checked((int)headerSize);

        while (headerOffset < headerSize) {
            var (serialType, bytesRead) = Varint.Parse(stream[headerOffset..]);
            var column = Column.Parse(checked((int)serialType), stream[contentOffset..]);

            yield return column;

            headerOffset += bytesRead;
            contentOffset += column.Content.Length;
        }
    }
}
