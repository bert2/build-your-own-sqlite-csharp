namespace codecrafters_sqlite;

using static System.Buffers.Binary.BinaryPrimitives;

public record Page(PageHeader Header, IEnumerable<Record> Records) {
    public static Page Parse(byte pageNum, Db db) {
        var dbHeaderOffset = pageNum == 1 ? DbHeader.Size : 0;
        var pageData = db.Page(pageNum);

        var header = PageHeader.Parse(pageData.Slice(dbHeaderOffset, PageHeader.MaxSize));
        var cellPtrsOffset = dbHeaderOffset + header.Size();

        var records = pageData[cellPtrsOffset..]
            .Chunk(2)
            .Take(header.NumberOfCells)
            .Select(bytes => ReadUInt16BigEndian(bytes.Span))
            .Select(cellPtr => {
                var stream = pageData[cellPtr..];

                var (_payloadSize, bytesRead1) = Varint.Parse(stream);
                var (_rowId, bytesRead2) = Varint.Parse(stream[bytesRead1..]);

                return Record.Parse(stream[(bytesRead1 + bytesRead2)..]);
            });

        return new(header, records);
    }
}
