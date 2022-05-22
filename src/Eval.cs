namespace codecrafters_sqlite;

using System;
using MoreLinq;

public interface IValue : IEquatable<IValue> {
    string Render();
}

public record NullValue : IValue {
    public string Render() => "NULL";
    public bool Equals(IValue? other) => other is NullValue;
}

public record IntValue(long Val) : IValue {
    public string Render() => Val.ToString();
    public bool Equals(IValue? other) => other is IntValue i && Equals(i);
}

public record StrValue(string Val) : IValue {
    public string Render() => Val;
    public bool Equals(IValue? other) => other is StrValue s && Equals(s);
}

public record Eval {
    private readonly Dictionary<string, int> colIdxs;
    public Eval(ObjSchema tblSchema) => colIdxs = Sql
        .ParseCreateTblStmt(tblSchema.Sql)
        .Cols
        .Index()
        .ToDictionary(x => x.Value, x => x.Key);
    public IValue ColValue(string col, LeafTblCell cell)
        => col == "id" ? new IntValue(cell.RowId) : cell.Payload[colIdxs[col]].ToValue();
}
