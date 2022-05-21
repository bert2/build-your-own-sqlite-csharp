namespace codecrafters_sqlite;

public record Db(ushort PageSize, ReadOnlyMemory<byte> Data)
{
    public static Db FromFile(string path)
    {
        var data = File.ReadAllBytes(path);
        return new(DbHeader.PageSize(data), data.AsMemory());
    }

    public byte this[int i] => Data.Span[i];

    public ReadOnlyMemory<byte> this[Range r] => Data[r];

    public ReadOnlyMemory<byte> Slice(int start, int length) => Data.Slice(start, length);

    public ReadOnlyMemory<byte> Page(byte pageNum) => Data.Slice((pageNum - 1) * PageSize, PageSize);

    public static implicit operator ReadOnlyMemory<byte>(Db db) => db.Data;
}
