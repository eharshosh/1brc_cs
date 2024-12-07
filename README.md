Relatively-sane implementation of the [1 Billion Rows Challange](https://github.com/gunnarmorling/1brc) in C#.  

1. clone data from https://huggingface.co/datasets/nietras/1brc.data 
2. extract `measurements-1000000000.txt`  
3. run `dotnet run -c Release <path-to-1brc.data>/measurements-1000000000.txt <path-to-1brc.data>/measurements-1000000000.out`
