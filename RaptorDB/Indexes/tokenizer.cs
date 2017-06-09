using System.Collections.Generic;

namespace RaptorDB
{
    class tokenizer : ITokenizer
    {
        public Dictionary<string, int> GenerateWordFreq(string text)
        {
            Dictionary<string, int> dic = new Dictionary<string, int>(500);

            char[] chars = text.ToCharArray();
            int index = 0;
            int look = 0;
            int count = chars.Length;
            int lastlang = langtype(chars[0]);
            while (index < count)
            {
                int lang = -1;
                while (look < count)
                {
                    char c = chars[look];
                    lang = langtype(c);
                    if (lang == lastlang)
                        look++;
                    else
                        break;
                }
                if (lastlang > -1)
                    ParseString(dic, chars, look, index);
                index = look;
                lastlang = lang;
            }
            return dic;
        }

        private static int langtype(char c)
        {
            if (char.IsDigit(c))
                return 0;

            else if (char.IsWhiteSpace(c))
                return -1;

            else if (char.IsPunctuation(c))
                return -1;

            else if (char.IsLetter(c)) // FEATURE : language checking here
                return 1;

            else
                return -1;
        }

        private static void ParseString(Dictionary<string, int> dic, char[] chars, int end, int start)
        {
            // check if upper lower case mix -> extract words
            int uppers = 0;
            bool found = false;
            for (int i = start; i < end; i++)
            {
                if (char.IsUpper(chars[i]))
                    uppers++;
            }
            // not all uppercase
            if (uppers != end - start - 1)
            {
                int lastUpper = start;

                string word = "";
                for (int i = start + 1; i < end; i++)
                {
                    char c = chars[i];
                    if (char.IsUpper(c))
                    {
                        found = true;
                        word = new string(chars, lastUpper, i - lastUpper).ToLowerInvariant().Trim();
                        AddDictionary(dic, word);
                        lastUpper = i;
                    }
                }
                if (lastUpper > start)
                {
                    string last = new string(chars, lastUpper, end - lastUpper).ToLowerInvariant().Trim();
                    if (word != last)
                        AddDictionary(dic, last);
                }
            }
            if (found == false)
            {
                string s = new string(chars, start, end - start).ToLowerInvariant().Trim();
                AddDictionary(dic, s);
            }
        }

        private static void AddDictionary(Dictionary<string, int> dic, string word)
        {
            if (word == null)
                return;

            int l = word.Length;
            // too long
            if (l > Global.DefaultStringKeySize)
                return;

            // too short
            if (l < 2)
                return;

            addword(dic, word);
        }

        private static void addword(Dictionary<string, int> dic, string word)
        {
            int cc = 0;
            if (dic.TryGetValue(word, out cc))
                dic[word] = ++cc;
            else
                dic.Add(word, 1);
        }
    }
}
