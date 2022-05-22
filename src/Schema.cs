namespace codecrafters_sqlite;

public record DbSchema(Dictionary<string, ObjSchema> Objs) {
    public static DbSchema Parse(Db db) {
        var page = Page.Parse(pageNum: 1, db);
        return new(BTree
            .FullTblScan(page, db)
            .Select(cell => ObjSchema.Parse(cell.Payload))
            .ToDictionary(obj => obj.Name));
    }

    public IEnumerable<ObjSchema> Tbls => Objs.Values.Where(obj => obj.IsTbl);

    public ObjSchema Tbl(string name)
        => Objs.TryGetValue(name, out var obj) && obj.IsTbl
            ? obj
            : throw new InvalidOperationException($"Unkown table '{name}'.");

    public ObjSchema Idx(string name)
        => Objs.TryGetValue(name, out var obj) && obj.IsIdx
            ? obj
            : throw new InvalidOperationException($"Unkown index '{name}'.");
}

public record ObjSchema(
    string Type,
    string Name,
    string TableName,
    byte RootPage,
    string Sql) {
    public bool IsTbl => Type == "table";
    public bool IsIdx => Type == "index";
    public static ObjSchema Parse(Record record) => new(
        Type: record[0].ToUtf8String()!,
        Name: record[1].ToUtf8String()!,
        TableName: record[2].ToUtf8String()!,
        RootPage: record[3].ToByte(),
        Sql: record[4].ToUtf8String()!);
}
