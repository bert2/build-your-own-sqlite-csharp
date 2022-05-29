namespace codecrafters_sqlite;

using Nullable.Extensions.Util;

public record DbSchema(Dictionary<string, ObjSchema> Objs, Dictionary<string, ObjSchema> Idxs) {
    public static DbSchema Parse(Db db) {
        var page = Page.Parse(pageNum: 1, db);
        var objs = BTree
            .TblScan(page, db)
            .Select(cell => ObjSchema.Parse(cell.Payload))
            .ToArray();
        return new(
            objs.ToDictionary(obj => obj.Name),
            objs.Where(obj => obj.IsIdx).ToDictionary(obj => obj.TblName));
    }

    public IEnumerable<ObjSchema> Tbls => Objs.Values.Where(obj => obj.IsTbl);

    public ObjSchema Tbl(string name)
        => Objs.TryGetValue(name, out var obj) && obj.IsTbl
            ? obj
            : throw new InvalidOperationException($"Unkown table '{name}'.");

    public ObjSchema? Idx(string tblName) => Idxs.TryGetValue(tblName);
}

public record ObjSchema(
    string Type,
    string Name,
    string TblName,
    int RootPage,
    string Sql) {
    public bool IsTbl => Type == "table";
    public bool IsIdx => Type == "index";
    public static ObjSchema Parse(Record record) => new(
        Type: record[0].ToUtf8String()!,
        Name: record[1].ToUtf8String()!,
        TblName: record[2].ToUtf8String()!,
        RootPage: record[3].ToInt(),
        Sql: record[4].ToUtf8String()!);
}
