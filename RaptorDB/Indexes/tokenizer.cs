using System;
using System.Collections.Generic;

namespace RaptorDB
{
    public class tokenizer
    {
        public static Dictionary<string, int> GenerateWordFreq(string text)
        {
            Dictionary<string, int> dic = new Dictionary<string, int>(500);

            char[] chars = text.ToCharArray();
            int index = 0;
            int run = -1;
            int count = chars.Length;
            while (index < count)
            {
                char c = chars[index++];
                if (!(char.IsLetterOrDigit(c) || c == '.' || c == ','))
                {
                    if (run != -1)
                    {
                        ParseString(dic, chars, index, run);
                        run = -1;
                    }
                }
                else
                    if (run == -1)
                    run = index - 1;
            }

            if (run != -1)
            {
                ParseString(dic, chars, index, run);
                run = -1;
            }

            return dic;
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

            // trim end non letter/digit    
            int x = 1;
            while (x < l)
            {
                if (char.IsLetterOrDigit(word[l - x]) == false)
                    x++;
                else
                    break;
            }
            if (l - x + 1 > 0)
                word = new string(word.ToCharArray(), 0, l - x + 1);
            else
                word = "";
            l = word.Length;

            // trim start non letter/digit
            x = 0;
            while (l > 0 && x < l)
            {
                if (char.IsLetterOrDigit(word[x]) == false)
                    x++;
                else
                    break;
            }
            if (l - x > 0)
                word = new string(word.ToCharArray(), x, l - x);
            else
                word = "";
            l = word.Length;

            // too short
            if (l < 2)
                return;

            //  split a.b.c words and not numbers and dots > 1
            if (char.IsDigit(word[0]) == false || CountWordDots(word)>1)
            {
                foreach (var s in word.Split('.'))
                {
                    if (s.Length > 2)
                        addword(dic, s);
                }
            }
            else
                addword(dic, word);
        }

        private static int CountWordDots(string word)
        {
            int c = 0;
            foreach(var s in word.ToCharArray())
                if (s == '.')
                    c++;
            return c;
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
