namespace codecrafters_sqlite;

using static System.Buffers.Binary.BinaryPrimitives;

public record Page(int Num, PageHeader Header, ReadOnlyMemory<byte> Data) {
    public static Page Parse(int pageNum, Db db) {
        var dbHeaderOffset = pageNum == 1 ? DbHeader.Size : 0;
        var pageData = db.Page(pageNum);
        var header = PageHeader.Parse(pageData.Slice(dbHeaderOffset, PageHeader.MaxSize));
        return new(pageNum, header, pageData);
    }

    public IEnumerable<ushort> CellPtrs() {
        var cellPtrsOffset = Header.Size() + (Num == 1 ? DbHeader.Size : 0);
        return Data[cellPtrsOffset..]
            .Chunk(2)
            .Take(Header.NumberOfCells)
            .Select(bytes => ReadUInt16BigEndian(bytes.Span));
    }
}
