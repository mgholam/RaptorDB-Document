using fastJSON;
using System;
using System.Collections.Generic;

namespace fastBinaryJSON
{
    internal sealed class BJsonParser
    {
        readonly byte[] _json;
        int _index;
        bool _useUTC = true;
        bool _v1_4TA = false;

        internal BJsonParser(byte[] json, bool useUTC, bool v1_4TA)
        {
            this._json = json;
            _v1_4TA = v1_4TA;
            _useUTC = useUTC;
        }

        public object Decode()
        {
            bool b = false;
            return ParseValue(out b);
        }

        private Dictionary<string, object> ParseObject()
        {
            Dictionary<string, object> dic = new Dictionary<string, object>(10);
            bool breakparse = false;
            while (!breakparse)
            {
                byte t = GetToken();
                if (t == TOKENS.COMMA)
                    continue;
                if (t == TOKENS.DOC_END)
                    break;
                if (t == TOKENS.TYPES_POINTER)
                {
                    // save curr index position
                    int savedindex = _index;
                    // set index = pointer 
                    _index = ParseInt();
                    t = GetToken();
                    // read $types
                    breakparse = readkeyvalue(dic, ref t);
                    // set index = saved + 4
                    _index = savedindex + 4;
                }
                else
                    breakparse = readkeyvalue(dic, ref t);
            }
            return dic;
        }

        private bool readkeyvalue(Dictionary<string, object> dic, ref byte t)
        {
            bool breakparse;
            string key = "";
            //if (t != TOKENS.NAME)
            if (t == TOKENS.NAME)
                key = ParseName();
            else if (t == TOKENS.NAME_UNI)
                key = ParseName2();
            else
                throw new Exception("excpecting a name field");

            t = GetToken();
            if (t != TOKENS.COLON)
                throw new Exception("expecting a colon");
            object val = ParseValue(out breakparse);

            if (breakparse == false)
                dic.Add(key, val);

            return breakparse;
        }

        private string ParseName2() // unicode byte len string -> <128 len chars
        {
            byte c = _json[_index++];
            string s = Reflection.UnicodeGetString(_json, _index, c);
            _index += c;
            return s;
        }

        private string ParseName()
        {
            byte c = _json[_index++];
            string s = Reflection.UTF8GetString(_json, _index, c);
            _index += c;
            return s;
        }

        private List<object> ParseArray()
        {
            List<object> array = new List<object>();

            bool breakparse = false;
            while (!breakparse)
            {
                object o = ParseValue(out breakparse);
                byte t = 0;
                if (breakparse == false)
                {
                    array.Add(o);
                    t = GetToken();
                }
                else t = (byte)o;

                if (t == TOKENS.COMMA)
                    continue;
                if (t == TOKENS.ARRAY_END)
                    break;
            }
            return array;
        }

        private object ParseValue(out bool breakparse)
        {
            byte t = GetToken();
            breakparse = false;
            switch (t)
            {
                case TOKENS.BYTE:
                    return ParseByte();
                case TOKENS.BYTEARRAY:
                    return ParseByteArray();
                case TOKENS.CHAR:
                    return ParseChar();
                case TOKENS.DATETIME:
                    return ParseDateTime();
                case TOKENS.DECIMAL:
                    return ParseDecimal();
                case TOKENS.DOUBLE:
                    return ParseDouble();
                case TOKENS.FLOAT:
                    return ParseFloat();
                case TOKENS.GUID:
                    return ParseGuid();
                case TOKENS.INT:
                    return ParseInt();
                case TOKENS.LONG:
                    return ParseLong();
                case TOKENS.SHORT:
                    return ParseShort();
                case TOKENS.UINT:
                    return ParseUint();
                case TOKENS.ULONG:
                    return ParseULong();
                case TOKENS.USHORT:
                    return ParseUShort();
                case TOKENS.UNICODE_STRING:
                    return ParseUnicodeString();
                case TOKENS.STRING:
                    return ParseString();
                case TOKENS.DOC_START:
                    return ParseObject();
                case TOKENS.ARRAY_START:
                    return ParseArray();
                case TOKENS.TRUE:
                    return true;
                case TOKENS.FALSE:
                    return false;
                case TOKENS.NULL:
                    return null;
                case TOKENS.ARRAY_END:
                    breakparse = true;
                    return TOKENS.ARRAY_END;
                case TOKENS.DOC_END:
                    breakparse = true;
                    return TOKENS.DOC_END;
                case TOKENS.COMMA:
                    breakparse = true;
                    return TOKENS.COMMA;
                case TOKENS.ARRAY_TYPED:
                case TOKENS.ARRAY_TYPED_LONG:
                    return ParseTypedArray(t);
                case TOKENS.TIMESPAN:
                    return ParsTimeSpan();
            }

            throw new Exception("Unrecognized token at index = " + _index);
        }

        private TimeSpan ParsTimeSpan()
        {
            long l = Helper.ToInt64(_json, _index);
            _index += 8;

            TimeSpan dt = new TimeSpan(l);

            return dt;
        }

        private object ParseTypedArray(byte token)
        {
            typedarray ar = new typedarray();
            if (token == TOKENS.ARRAY_TYPED)
            {
                if (_v1_4TA)
                    ar.typename = ParseName(); 
                else
                    ar.typename = ParseName2();
            }
            else
                ar.typename = ParseNameLong();

            ar.count = ParseInt();

            bool breakparse = false;
            while (!breakparse)
            {
                object o = ParseValue(out breakparse);
                byte b = 0;
                if (breakparse == false)
                {
                    ar.data.Add(o);
                    b = GetToken();
                }
                else b = (byte)o;

                if (b == TOKENS.COMMA)
                    continue;
                if (b == TOKENS.ARRAY_END)
                    break;
            }
            return ar;
        }

        private string ParseNameLong() // unicode short len string -> <32k chars
        {
            short c = Helper.ToInt16(_json, _index);
            _index += 2;
            string s = Reflection.UnicodeGetString(_json, _index, c);
            _index += c;
            return s;
        }

        private object ParseChar()
        {
            short u = Helper.ToInt16(_json, _index);
            _index += 2;
            return u;
        }

        private Guid ParseGuid()
        {
            byte[] b = new byte[16];
            Buffer.BlockCopy(_json, _index, b, 0, 16);
            _index += 16;
            return new Guid(b);
        }

        private float ParseFloat()
        {
            float f = BitConverter.ToSingle(_json, _index);
            _index += 4;
            return f;
        }

        private ushort ParseUShort()
        {
            ushort u = (ushort)Helper.ToInt16(_json, _index);
            _index += 2;
            return u;
        }

        private ulong ParseULong()
        {
            ulong u = (ulong)Helper.ToInt64(_json, _index);
            _index += 8;
            return u;
        }

        private uint ParseUint()
        {
            uint u = (uint)Helper.ToInt32(_json, _index);
            _index += 4;
            return u;
        }

        private short ParseShort()
        {
            short u = (short)Helper.ToInt16(_json, _index);
            _index += 2;
            return u;
        }

        private long ParseLong()
        {
            long u = (long)Helper.ToInt64(_json, _index);
            _index += 8;
            return u;
        }

        private int ParseInt()
        {
            int u = (int)Helper.ToInt32(_json, _index);
            _index += 4;
            return u;
        }

        private double ParseDouble()
        {
            double d = BitConverter.ToDouble(_json, _index);
            _index += 8;
            return d;
        }

        private object ParseUnicodeString()
        {
            int c = Helper.ToInt32(_json, _index);
            _index += 4;

            string s = Reflection.UnicodeGetString(_json, _index, c);
            _index += c;
            return s;
        }

        private string ParseString()
        {
            int c = Helper.ToInt32(_json, _index);
            _index += 4;

            string s = Reflection.UTF8GetString(_json, _index, c);
            _index += c;
            return s;
        }

        private decimal ParseDecimal()
        {
            int[] i = new int[4];
            i[0] = Helper.ToInt32(_json, _index);
            _index += 4;
            i[1] = Helper.ToInt32(_json, _index);
            _index += 4;
            i[2] = Helper.ToInt32(_json, _index);
            _index += 4;
            i[3] = Helper.ToInt32(_json, _index);
            _index += 4;

            return new decimal(i);
        }

        private DateTime ParseDateTime()
        {
            long l = Helper.ToInt64(_json, _index);
            _index += 8;

            DateTime dt = new DateTime(l);
            if (_useUTC)
                dt = dt.ToLocalTime(); // to local time

            return dt;
        }

        private byte[] ParseByteArray()
        {
            int c = Helper.ToInt32(_json, _index);
            _index += 4;
            byte[] b = new byte[c];
            Buffer.BlockCopy(_json, _index, b, 0, c);
            _index += c;
            return b;
        }

        private byte ParseByte()
        {
            return _json[_index++];
        }

        private byte GetToken()
        {
            byte b = _json[_index++];
            return b;
        }
    }
}
