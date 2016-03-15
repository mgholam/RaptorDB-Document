using System.Xml.Serialization;

namespace RaptorDB
{
    public enum OPERATION
    {
        AND,
        OR,
        ANDNOT
    }

    public class Document
    {
        public Document()
        {
            DocNumber = -1;
        }
        public Document(string filename, string text)
        {
            FileName = filename;
            Text = text;
            DocNumber = -1;
        }
        public int DocNumber { get; set; }
        [XmlIgnore]
        public string Text { get; set; }
        public string FileName { get; set; }

        public override string ToString()
        {
            return FileName;
        }
    }
}
