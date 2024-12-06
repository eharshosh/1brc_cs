using System.Diagnostics;
using System.Text;
using System.Text.RegularExpressions;

 const string dataFilename = "/Users/eitan/development/1brc.data/measurements-1000000000.txt";
 const string compareToFilename = "/Users/eitan/development/1brc.data/measurements-1000000000.out";
// const string dataFilename = "/Users/eitan/development/1brc.data/measurements-10000000.txt";
// const string compareToFilename = "/Users/eitan/development/1brc.data/measurements-10000000.out";

using var fs = File.OpenRead(dataFilename);
using var reader = new StreamReader(fs, Encoding.UTF8);

var rawInput = File.ReadAllText(compareToFilename)[1..^2];
var compareTo = Regex
    .Split(rawInput, @"([\w\s\(\),\.'\-]+)=([\-\.\d]+)/([\-\.\d]+)/([\-\.\d]+)(, )?", RegexOptions.CultureInvariant)
    .Chunk(6)
    .Select(chunk => new OutputLine(chunk[1], float.Parse(chunk[2]), float.Parse(chunk[3]), float.Parse(chunk[4])))
    .ToDictionary(x => x.City);

var sw = Stopwatch.StartNew();
var map = new Dictionary<string, CityState>();
var alternateLookup = map.GetAlternateLookup<ReadOnlySpan<char>>();
var lineCount = 0;

while (reader.ReadLine() is { } line)
{
    if (++lineCount % 100_000_000 == 0)
    {
        Console.WriteLine($"{lineCount}@{sw.Elapsed}");
    }

    var lineSpan = line.AsSpan();
    var semiColonIndex = lineSpan.IndexOf(';');
    var city = lineSpan[..semiColonIndex];
    var measurement = lineSpan[(semiColonIndex + 1)..];
    var dotIndex = measurement.IndexOf('.');
    long factor = (int)Math.Pow(10, measurement.Length - dotIndex);
    var signFactor = measurement[0] == '-' ? -1 : 1;
    var value = long.Parse(measurement[..dotIndex]) * 1000
                + long.Parse(measurement[(dotIndex + 1)..]) * factor * signFactor;
    if (!alternateLookup.TryGetValue(city, out var state))
    {
        state = new CityState { min = value, max = value, sum = value, count = 1 };
        alternateLookup[city] = state;
        continue;
    }

    state.min = Math.Min(state.min, value);
    state.max = Math.Max(state.max, value);
    state.sum += value;
    state.count++;
}

var outputName = Path.GetFileNameWithoutExtension(dataFilename) + "_ours.out";
var outFile = File.OpenWrite(outputName);
var writer = new StreamWriter(outFile);
var output = new List<OutputLine>(map.Keys.Count);
foreach (var (key, state) in map.OrderBy(kvp => kvp.Key))
{
    var avg = Math.Round((double)state.sum / state.count / 100) / 10;
    var min = Math.Round((double)state.min / 100) / 10;
    var max = Math.Round((double)state.max / 100) / 10;
    writer.WriteLine(key + ";" + min + ";" + avg + ";" + max);
    output.Add(new(key, (float)min, (float)avg, (float)max));
}

Console.WriteLine(sw.Elapsed);


var outputSorted = output.ToDictionary(x => x.City);

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

class CityState
{
    public long min;
    public long sum;
    public long max;
    public long count;
}

record OutputLine(string City, float Min, float Average, float Max);