namespace codecrafters_sqlite;

public static class Extensions {
    /// <summary>
    /// Splits a `System.ReadOnlyMemory` into smaller chunks. Works like `IEnumerable.Chunk()`
    /// but doesn't allocate additional arrays.
    /// </summary>
    /// <seealso>https://docs.microsoft.com/dotnet/api/system.linq.enumerable.chunk</seealso>
    public static IEnumerable<ReadOnlyMemory<T>> Chunk<T>(this ReadOnlyMemory<T> source, int size) {
        for (var i = 0; i < source.Length; i += size) {
            yield return i + size < source.Length
                ? source.Slice(i, length: size)
                : source[i..];
        }
    }

    public static string Join(this IEnumerable<string> source, string sep) => string.Join(sep, source);

    public static IEnumerable<T> Yield<T>(this T x) => Enumerable.Repeat(x, 1);
}
