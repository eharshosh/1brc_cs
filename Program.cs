using System.Diagnostics;
using System.Text;
using System.Text.RegularExpressions;

namespace _1brc;

internal static class Program
{
    public static void Main(string[] args)
    {
        const string dataFilename = "/Users/eitan/development/1brc.data/measurements-1000000000.txt";
        const string compareToFilename = "/Users/eitan/development/1brc.data/measurements-1000000000.out";
        //
        // const string dataFilename = "/Users/eitan/development/1brc.data/measurements-10000000.txt";
        // const string compareToFilename = "/Users/eitan/development/1brc.data/measurements-10000000.out";

        var sw = Stopwatch.StartNew();
        var map = new Dictionary<string, CityState>();
        var alternateLookup = map.GetAlternateLookup<ReadOnlySpan<char>>();
        var lineCount = 0;
        using var fs = File.OpenRead(dataFilename);
        Span<byte> buffer = new byte[5 * 1024 * 1024];
        using var reader = new BufferedStream(fs, 1024 * 1024 * 10);
        Span<char> cityChars = stackalloc char[100];
        int readLength;
        const byte newLineChar = (byte)'\n';
        const byte semiColChar = (byte)';';
        const byte dotChar = (byte)'.';
        while ((readLength = reader.Read(buffer)) > 1)
        {
            var lastLineIndex = readLength - buffer[..readLength].LastIndexOf(newLineChar);
            reader.Position -= lastLineIndex - 1;

            var readSpan = buffer[..readLength];
            int newLineIndex;
            int lineIndex = 0;
            while ((newLineIndex = readSpan[lineIndex..].IndexOf(newLineChar)) != -1)
            {
                var lineSpan = readSpan[lineIndex..(lineIndex + newLineIndex)];
                lineIndex += newLineIndex + 1;
                var semiColonIndex = lineSpan.IndexOf(semiColChar);
                var cityRaw = lineSpan[..semiColonIndex];
                var cityLength = Encoding.UTF8.GetChars(cityRaw, cityChars);
                var measurement = lineSpan[(semiColonIndex + 1)..];
                var dotIndex = measurement.IndexOf(dotChar);
                var factor = (long)Math.Pow(10, measurement.Length - dotIndex);
                var signFactor = measurement[0] == '-' ? -1 : 1;
                var value = long.Parse(measurement[..dotIndex]) * 1000
                            + long.Parse(measurement[(dotIndex + 1)..]) * factor * signFactor;
                if (!alternateLookup.TryGetValue(cityChars[..cityLength], out var state))
                {
                    state = new CityState { Min = value, Max = value, Sum = value, Count = 1 };
                    alternateLookup[cityChars[..cityLength]] = state;
                }
                else
                {
                    state.Min = Math.Min(state.Min, value);
                    state.Max = Math.Max(state.Max, value);
                    state.Sum += value;
                    state.Count++;
                }

                if (++lineCount % 100_000_000 == 0)
                {
                    Console.WriteLine($"{lineCount}@{sw.Elapsed}");
                }
            }
        }

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


        var outputSorted = output.ToDictionary(x => x.City);
        var compareTo = ReadCompareTo(compareToFilename);
        foreach (var (city, item) in compareTo)
        {
            if (!outputSorted.TryGetValue(city, out var outputItem))
            {
                Console.WriteLine(city + " not found");
                continue;
            }

            if (item != outputItem)
            {
                Console.WriteLine($"{city}: {item} != {outputItem}");
            }
        }
    }

    static Dictionary<string, OutputLine> ReadCompareTo(string filename)
    {
        var rawInput = File.ReadAllText(filename)[1..^2];
        return Regex
            .Split(rawInput, @"([\w\s\(\),\.'\-]+)=([\-\.\d]+)/([\-\.\d]+)/([\-\.\d]+)(, )?",
                RegexOptions.CultureInvariant)
            .Chunk(6)
            .Select(chunk =>
                new OutputLine(chunk[1], float.Parse(chunk[2]), float.Parse(chunk[3]), float.Parse(chunk[4])))
            .ToDictionary(x => x.City);
    }

    class CityState
    {
        public long Min;
        public long Sum;
        public long Max;
        public int Count;
    }

    record OutputLine(string City, float Min, float Average, float Max);
}