namespace codecrafters_sqlite;

public record Schema(
    string Type,
    string Name,
    string TableName,
    byte RootPage,
    string Sql) {
    public static IEnumerable<Schema> ParseAll(Db db) {
        var page = Page.Parse(pageNum: 1, db);
        return page
            .CellPointers()
            .Select(ptr => LeafTblCell.Parse(page.Data[ptr..]).Payload)
            .Select(Parse);
    }

    /// <summary>Parses a record into a schema</summary>
    public static Schema Parse(Record record) => new(
        Type: record[0].ToUtf8String(),
        Name: record[1].ToUtf8String(),
        TableName: record[2].ToUtf8String(),
        RootPage: record[3].ToByte(),
        Sql: record[4].ToUtf8String());
}
