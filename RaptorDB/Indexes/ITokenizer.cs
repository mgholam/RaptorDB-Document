using System.Collections.Generic;

namespace RaptorDB
{
    public interface ITokenizer
    {
        Dictionary<string, int> GenerateWordFreq(string text);
    }
}