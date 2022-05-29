namespace codecrafters_sqlite;

public sealed class Db : IDisposable {
    private readonly FileStream fs;
    private readonly BinaryReader reader;

    public readonly ushort PageSize;

    public static Db FromFile(string path) => new(path);

    public Db(string path) {
        fs = new FileStream(path, FileMode.Open, FileAccess.Read);
        reader = new BinaryReader(fs);
        PageSize = DbHeader.PageSize(reader.ReadBytes(100));
    }

    public ReadOnlyMemory<byte> Page(int pageNum) {
        var pageStart = (pageNum - 1) * PageSize;
        _ = reader.BaseStream.Seek(pageStart, SeekOrigin.Begin);
        return reader.ReadBytes(PageSize);
    }

    public void Dispose() {
        reader.Dispose();
        fs.Dispose();
    }
}
