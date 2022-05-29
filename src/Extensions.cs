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

    public static string Join(this IEnumerable<string> source, char sep) => string.Join(sep, source);

    public static IEnumerable<T> Yield<T>(this T x) => Enumerable.Repeat(x, 1);

    /// <summary>
    /// Appends a lazily calculated value to the end of the sequence.
    /// </summary>
    /// <seealso>https://docs.microsoft.com/en-us/dotnet/api/system.linq.enumerable.append</seealso>
    public static IEnumerable<T> Append<T>(this IEnumerable<T> source, Func<T> element) {
        foreach (var item in source)
            yield return item;
        yield return element();
    }

    public static T? As<T>(this object x) where T: class => x as T;

    public static bool LessThan(this string? a, string? b) => (a, b) switch {
        (null, null) => false,
        (null, _   ) => true,
        (_   , null) => false,
        _            => a.CompareTo(b) < 0
    };

    public static bool LessOrEqualThan(this string? a, string? b) => (a, b) switch {
        (null, null) => true,
        (null, _   ) => true,
        (_   , null) => false,
        _            => a.CompareTo(b) <= 0
    };
}
