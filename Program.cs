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
        var readBuffer = new byte[2 * 1024 * 1024];
        int readLength;
        var tasks = new List<Task<Dictionary<string, CityState>>>();
        var semaphore = new SemaphoreSlim(Environment.ProcessorCount);
        while ((readLength = fs.Read(readBuffer)) > 1)
        {
            await semaphore.WaitAsync();
            var lastLineIndex = readBuffer.AsSpan(0, readLength).LastIndexOf(NewLineChar);
            fs.Position -= readLength - lastLineIndex - 1;
            var taskBufferArray = ArrayPool<byte>.Shared.Rent(lastLineIndex);
            readBuffer.AsSpan(0, lastLineIndex).CopyTo(taskBufferArray);
            tasks.Add(Task.Run(() =>
            {
                var res = ProcessBuffer(taskBufferArray.AsSpan(0, lastLineIndex));
                ArrayPool<byte>.Shared.Return(taskBufferArray);
                semaphore.Release();
                return res;
            }));
        }

        await MergeResults(tasks, map);

        var outputName = Path.GetFileNameWithoutExtension(dataFilename) + "_ours.out";
        var outFile = File.OpenWrite(outputName);
        var writer = new StreamWriter(outFile);
        var output = new List<OutputLine>(map.Keys.Count);
        foreach (var (key, state) in map.OrderBy(kvp => kvp.Key))
        {
            var avg = Math.Round((double)state.Sum / state.Count / 100) / 10;
            var min = Math.Round((double)state.Min / 100) / 10;
            var max = Math.Round((double)state.Max / 100) / 10;
            writer.WriteLine(key + ";" + min + ";" + avg + ";" + max);
            output.Add(new(key, (float)min, (float)avg, (float)max));
        }

        Console.WriteLine(sw.Elapsed);
        
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

    private static async Task MergeResults(List<Task<Dictionary<string, CityState>>> tasks,
        Dictionary<string, CityState> map)
    {
        var res = await Task.WhenAll(tasks);
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

    static Dictionary<string, CityState> ProcessBuffer(Span<byte> readSpan)
    {
        var res = new Dictionary<string, CityState>();
        var alternateLookup = res.GetAlternateLookup<ReadOnlySpan<char>>();
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
            long factor = (int)Math.Pow(10, measurement.Length - dotIndex);
            var signFactor = measurement[0] == '-' ? -1 : 1;
            var value = long.Parse(measurement[..dotIndex]) * 1000
                        + long.Parse(measurement[(dotIndex + 1)..]) * factor * signFactor;
            if (!alternateLookup.TryGetValue(city, out var state))
            {
                state = new CityState { Min = value, Max = value, Sum = value, Count = 1 };
                alternateLookup[city] = state;
            }
            else
            {
                state.Min = Math.Min(state.Min, value);
                state.Max = Math.Max(state.Max, value);
                state.Sum += value;
                state.Count++;
            }
        }

        return res;
    }
}