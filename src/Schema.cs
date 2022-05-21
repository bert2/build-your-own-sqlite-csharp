namespace codecrafters_sqlite;

public record Schema(
    string Type,
    string Name,
    string TableName,
    byte RootPage,
    string Sql) {
    public static IEnumerable<Schema> ParseAll(Db db) => Page
        .Parse(pageNum: 1, db)
        .Records
        .Select(Parse);

    /// <summary>Parses a record into a schema</summary>
    public static Schema Parse(Record record) => new(
        Type: record.Columns[0].ToUtf8String(),
        Name: record.Columns[1].ToUtf8String(),
        TableName: record.Columns[2].ToUtf8String(),
        RootPage: record.Columns[3].ToByte(),
        Sql: record.Columns[4].ToUtf8String());
}
