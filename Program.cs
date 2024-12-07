using System.Buffers;
using System.Diagnostics;
using System.Text;
using System.Text.RegularExpressions;

namespace _1brc;

internal static class Program
{
    private const byte NewLineChar = (byte)'\n';
    private const byte SemiColChar = (byte)';';
    private const byte DotChar = (byte)'.';

    public static async Task Main(string[] args)
    {
        const string dataFilename = "/Users/eitan/development/1brc.data/measurements-1000000000.txt";
        const string compareToFilename = "/Users/eitan/development/1brc.data/measurements-1000000000.out";
        //
        // const string dataFilename = "/Users/eitan/development/1brc.data/measurements-10000000.txt";
        // const string compareToFilename = "/Users/eitan/development/1brc.data/measurements-10000000.out";

        var sw = Stopwatch.StartNew();
        var map = new Dictionary<string, CityState>();
        await using var fs = File.OpenRead(dataFilename);
        var readBuffer = new byte[5 * 1024 * 1024];
        int readLength;
        var tasks = new List<Task>();
        var semaphore = new SemaphoreSlim(Environment.ProcessorCount);
        var maps =
            Enumerable.Range(0, Environment.ProcessorCount)
                .Select(_ => new Dictionary<string, CityState>(40_000))
                .ToList();
        while ((readLength = await fs.ReadAsync(readBuffer)) > 1)
        {
            await semaphore.WaitAsync();
            var lastLineIndex = readBuffer.AsSpan(0, readLength).LastIndexOf(NewLineChar);
            fs.Position -= readLength - lastLineIndex - 1;
            var taskBufferArray = ArrayPool<byte>.Shared.Rent(lastLineIndex);
            readBuffer.AsSpan(0, lastLineIndex).CopyTo(taskBufferArray);
            tasks.Add(Task.Run(() =>
            {
                Dictionary<string, CityState> borrowedMap;
                lock (maps)
                {
                    borrowedMap = maps[0];
                    maps.RemoveAt(0);
                }

                ProcessBuffer(taskBufferArray.AsSpan(0, lastLineIndex), borrowedMap);
                ArrayPool<byte>.Shared.Return(taskBufferArray);
                lock (maps)
                {
                    maps.Add(borrowedMap);
                }

                semaphore.Release();
            }));
        }

        await Task.WhenAll(tasks);
        MergeResults(maps, map);


        var output = new List<OutputLine>(map.Keys.Count);
        foreach (var (key, state) in map.OrderBy(kvp => kvp.Key))
        {
            var avg = (float)Math.Round((double)state.Sum / state.Count) / 10;
            var min = (float)state.Min / 10;
            var max = (float)state.Max / 10;
            output.Add(new(key, min, avg, max));
        }

        Console.WriteLine(sw.Elapsed);
        Console.WriteLine((long)(1000000000 / sw.Elapsed.TotalSeconds) + " lines per second");

        ValidateOutput(output, compareToFilename);
    }

    private static void ValidateOutput(List<OutputLine> output, string compareToFilename)
    {
        var outputMap = output.ToDictionary(x => x.City);
        var compareFileLines = ReadCompareFile(compareToFilename);
        foreach (var item in compareFileLines)
        {
            if (!outputMap.TryGetValue(item.City, out var outputItem))
            {
                Console.WriteLine(item.City + " not found");
                continue;
            }

            if (item != outputItem)
            {
                Console.WriteLine($"{item} != {outputItem}");
            }
        }
    }

    private static void MergeResults(List<Dictionary<string, CityState>> res,
        Dictionary<string, CityState> map)
    {
        foreach (var dictionary in res)
        {
            foreach (var (key, value) in dictionary)
            {
                if (!map.TryGetValue(key, out var state))
                {
                    map[key] = value;
                }
                else
                {
                    state.Min = Math.Min(state.Min, value.Min);
                    state.Max = Math.Max(state.Max, value.Max);
                    state.Sum += value.Sum;
                    state.Count += value.Count;
                }
            }
        }
    }

    static OutputLine[] ReadCompareFile(string filename)
    {
        var rawInput = File.ReadAllText(filename)[1..^2];
        return Regex
            .Split(rawInput, @"([\w\s\(\),\.'\-]+)=([\-\.\d]+)/([\-\.\d]+)/([\-\.\d]+)(, )?",
                RegexOptions.CultureInvariant)
            .Chunk(6)
            .Select(chunk =>
                new OutputLine(chunk[1], float.Parse(chunk[2]), float.Parse(chunk[3]), float.Parse(chunk[4])))
            .ToArray();
    }

    class CityState
    {
        public long Min;
        public long Sum;
        public long Max;
        public int Count;
    }

    record OutputLine(string City, float Min, float Average, float Max);

    static void ProcessBuffer(Span<byte> readSpan, Dictionary<string, CityState> map)
    {
        var alternateLookup = map.GetAlternateLookup<ReadOnlySpan<char>>();
        Span<char> cityChars = stackalloc char[100];
        int newLineIndex;
        var lineIndex = 0;
        while ((newLineIndex = readSpan[lineIndex..].IndexOf(NewLineChar)) != -1)
        {
            var lineSpan = readSpan[lineIndex..(lineIndex + newLineIndex)];
            lineIndex += newLineIndex + 1;
            var semiColonIndex = lineSpan.IndexOf(SemiColChar);
            var cityRaw = lineSpan[..semiColonIndex];
            var cityLength = Encoding.UTF8.GetChars(cityRaw, cityChars);
            var city = cityChars[..cityLength];
            var measurement = lineSpan[(semiColonIndex + 1)..];
            var dotIndex = measurement.IndexOf(DotChar);
            measurement = measurement[..(dotIndex + 2)];
            var value = ParseMeasurement(measurement);
            if (alternateLookup.TryGetValue(city, out var state))
            {
                state.Min = state.Min < value ? state.Min : value;
                state.Max = state.Max > value ? state.Max : value;
                state.Sum += value;
                state.Count++;
            }
            else
            {
                state = new CityState { Min = value, Max = value, Sum = value, Count = 1 };
                alternateLookup[city] = state;
            }
        }
    }

    static int ParseMeasurement(ReadOnlySpan<byte> measurement)
    {
        var res = measurement[^1] - '0' + (measurement[^3] - '0') * 10;
        if (measurement.Length == 3)
        {
            return res;
        }

        if (measurement[^4] == '-')
        {
            return -res;
        }

        res += (measurement[^4] - '0') * 100;
        return measurement.Length == 4 ? res : -res;
    }
}