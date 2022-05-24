namespace codecrafters_sqlite;

public record Record(Column[] Columns) {
    /// <summary>
    /// Reads SQLite's "Record Format" as mentioned here:
    /// https://www.sqlite.org/fileformat.html#record_format
    /// </summary>
    public static Record Parse(ReadOnlyMemory<byte> stream) => new(ParseColumns(stream).ToArray());

    public Column this[int i] => Columns[i];

    private static IEnumerable<Column> ParseColumns(ReadOnlyMemory<byte> stream) {
        var (headerSize, headerOffset) = Varint32.Parse(stream);
        var contentOffset = headerSize;

        while (headerOffset < headerSize) {
            var (serialType, bytesRead) = Varint32.Parse(stream[headerOffset..]);
            var column = Column.Parse(serialType, stream[contentOffset..]);

            yield return column;

            headerOffset += bytesRead;
            contentOffset += column.Content.Length;
        }
    }
}
