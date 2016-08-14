using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace RaptorDB.Views
{
    delegate WAHBitArray QueryFromTo(string colname, object from, object to);
    delegate WAHBitArray QueryExpression(string colname, RDBExpression exp, object from);

    internal class QueryVisitor : ExpressionVisitor
    {
        public QueryVisitor(QueryExpression express, QueryFromTo fromto)
        {
            qexpression = express;
            qfromto = fromto;
        }
        public Stack<object> _stack = new Stack<object>();
        public Stack<object> _bitmap = new Stack<object>();
        QueryFromTo qfromto;
        QueryExpression qexpression;
        private bool _leftmode = true;

        protected override Expression VisitBinary(BinaryExpression b)
        {
            _leftmode = true;
            var m = this.Visit(b.Left);
            if (m == null) // VB.net sty;e linq for string compare
                return b.Right;
            ExpressionType t = b.NodeType;

            if (t == ExpressionType.Equal || t == ExpressionType.NotEqual ||
                t == ExpressionType.LessThan || t == ExpressionType.LessThanOrEqual ||
                t == ExpressionType.GreaterThan || t == ExpressionType.GreaterThanOrEqual)
                _stack.Push(b.NodeType);

            _leftmode = false;
            this.Visit(b.Right);
            t = b.NodeType;
            if (t == ExpressionType.Equal || t == ExpressionType.NotEqual ||
                t == ExpressionType.LessThanOrEqual || t == ExpressionType.LessThan ||
                t == ExpressionType.GreaterThanOrEqual || t == ExpressionType.GreaterThan
                )
            {
                // binary expression 
                object lval = _stack.Pop();
                ExpressionType lop = (ExpressionType)_stack.Pop();
                string lname = (string)_stack.Pop();
                if (_stack.Count > 0)
                {
                    lname += "_" + (string)_stack.Pop();
                }
                RDBExpression exp = RDBExpression.Equal;
                if (lop == ExpressionType.LessThan) exp = RDBExpression.Less;
                else if (lop == ExpressionType.LessThanOrEqual) exp = RDBExpression.LessEqual;
                else if (lop == ExpressionType.GreaterThan) exp = RDBExpression.Greater;
                else if (lop == ExpressionType.GreaterThanOrEqual) exp = RDBExpression.GreaterEqual;
                else if (lop == ExpressionType.NotEqual) exp = RDBExpression.NotEqual;

                _bitmap.Push(qexpression(lname, exp, lval));
            }

            if (t == ExpressionType.And || t == ExpressionType.AndAlso ||
                t == ExpressionType.Or || t == ExpressionType.OrElse)
            {
                if (_bitmap.Count > 1)
                {
                    // do bitmap operations 
                    WAHBitArray right = (WAHBitArray)_bitmap.Pop();
                    WAHBitArray left = (WAHBitArray)_bitmap.Pop();

                    if (t == ExpressionType.And || t == ExpressionType.AndAlso)
                        _bitmap.Push(right.And(left));
                    if (t == ExpressionType.Or || t == ExpressionType.OrElse)
                        _bitmap.Push(right.Or(left));
                }
                else
                {
                    // single bitmap operation
                }
            }
            return b;
        }

        protected override Expression VisitMethodCall(MethodCallExpression m)
        {
            string s = m.ToString();
            // FEATURE : add contains , startswith

            // VB.net : e.g. CompareString(x.NoCase, "Me 4", False)
            if (s.StartsWith("CompareString"))
            {
                var left = m.Arguments[0];
                // Removes dot if any
                var leftStr = left.ToString().Substring(left.ToString().IndexOf('.') + 1);
                var right = m.Arguments[1].ToString().Replace("\"", String.Empty);
                RDBExpression exp = RDBExpression.Equal;
                _bitmap.Push(qexpression("" + leftStr, exp, right));
                return null;
            }
            string mc = s.Substring(s.IndexOf('.') + 1);
            if (mc.Contains("Between")) // FIX : add more between here
            {
                Type datatype = m.Arguments[0].Type;
                string name = m.Arguments[0].ToString().Split('.')[1];

                if (datatype == typeof(DateTime))
                {
                    DateTime from = DateTime.Now;
                    DateTime to = DateTime.Now;
                    if (m.Arguments[1].Type == typeof(string))
                    {
                        from = DateTime.Parse((string)GetValueForMember(m.Arguments[1]));
                        to = DateTime.Parse((string)GetValueForMember(m.Arguments[2]));
                    }
                    else
                    {
                        from = (DateTime)GetValueForMember(m.Arguments[1]);
                        to = (DateTime)GetValueForMember(m.Arguments[2]);
                    }
                    _bitmap.Push(qfromto(name, from, to));
                }
                else if(datatype == typeof(int))
                {
                    var from = (int)GetValueForMember(m.Arguments[1]);
                    var to = (int)GetValueForMember(m.Arguments[2]);
                    _bitmap.Push(qfromto(name, from, to));
                }
                else if (datatype == typeof(long))
                {
                    var from = (long)GetValueForMember(m.Arguments[1]);
                    var to = (long)GetValueForMember(m.Arguments[2]);
                    _bitmap.Push(qfromto(name, from, to));
                }
                else if (datatype == typeof(decimal))
                {
                    var from = (decimal)GetValueForMember(m.Arguments[1]);
                    var to = (decimal)GetValueForMember(m.Arguments[2]);
                    _bitmap.Push(qfromto(name, from, to));
                }
            }
            else
                _stack.Push(mc);

            return m;
        }

        private object GetValueForMember(object m)
        {
            object val = null;
            var f = m as ConstantExpression;
            if (f != null)
                return f.Value;

            var mm = m as MemberExpression;
            if (mm.NodeType == ExpressionType.MemberAccess)
            {
                Type tt = mm.Expression.Type;
                val = tt.InvokeMember(mm.Member.Name, BindingFlags.GetField |
                    BindingFlags.GetProperty |
                    BindingFlags.Public |
                    BindingFlags.NonPublic |
                    BindingFlags.Instance |
                    BindingFlags.Static, null, (mm.Expression as ConstantExpression).Value, null);
            }
            return val;
        }

        protected override Expression VisitMember(MemberExpression m)
        {
            var n = m.Member.Name;
            if (n != null && m.Expression.Type == typeof(DateTime))
            {
                _stack.Push("$" + n);
            }
            var e = base.VisitMember(m);
            var c = m.Expression as ConstantExpression;
            if (c != null)
            {
                Type t = c.Value.GetType();
                var x = t.InvokeMember(m.Member.Name, BindingFlags.GetField |
                    BindingFlags.GetProperty |
                    BindingFlags.Public |
                    BindingFlags.NonPublic |
                    BindingFlags.Instance |
                    BindingFlags.Static, null, c.Value, null);
                _stack.Push(x);
            }

            if (m.Expression != null)
            {
                if (m.Expression.NodeType == ExpressionType.Parameter) // property
                    _stack.Push(m.Member.Name);
                else if (m.Expression.NodeType == ExpressionType.MemberAccess) // obj.property
                {
                    if (_leftmode == false)
                    {
                        Type t = m.Expression.Type;
                        var val = t.InvokeMember(m.Member.Name, BindingFlags.GetField |
                            BindingFlags.GetProperty |
                            BindingFlags.Public |
                            BindingFlags.NonPublic |
                            BindingFlags.Instance |
                            BindingFlags.Static, null, _stack.Pop(), null);
                        _stack.Push(val);
                    }
                }
            }

            return e;
        }

        protected override Expression VisitConstant(ConstantExpression c)
        {
            IQueryable q = c.Value as IQueryable;
            if (q != null)
            {
                _stack.Push(q.ElementType.Name);
            }
            else if (c.Value == null)
            {
                _stack.Push(null);
            }
            else
            {
                Type t = c.Value.GetType();
                if (t.IsValueType || t == typeof(string))
                    _stack.Push(c.Value);
            }
            return c;
        }
    }
}
