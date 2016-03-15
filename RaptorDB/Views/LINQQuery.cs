using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace RaptorDB.Views
{
    //FEATURE : handle Contains, StartsWith, Between predicates

    delegate WAHBitArray QueryFromTo(string colname, object from, object to);
    delegate WAHBitArray QueryExpression(string colname, RDBExpression exp, object from);

    internal class QueryVisitor : ExpressionVisitor
    {
        public QueryVisitor(QueryExpression express)
        {
            qexpression = express;
        }
        public Stack<object> _stack = new Stack<object>();
        public Stack<object> _bitmap = new Stack<object>();
        //QueryFromTo qfromto;
        QueryExpression qexpression;

        protected override Expression VisitBinary(BinaryExpression b)
        {           
            var m = this.Visit(b.Left);
            if (m == null) // VB.net sty;e linq for string compare
                return b.Right;
            ExpressionType t = b.NodeType;

            if (t == ExpressionType.Equal || t == ExpressionType.NotEqual ||
                t == ExpressionType.LessThan || t == ExpressionType.LessThanOrEqual ||
                t == ExpressionType.GreaterThan || t == ExpressionType.GreaterThanOrEqual)
                _stack.Push(b.NodeType);


            this.Visit(b.Right);
            t = b.NodeType;
            if (t == ExpressionType.Equal || t == ExpressionType.NotEqual ||
                t == ExpressionType.LessThanOrEqual || t == ExpressionType.LessThan ||
                t == ExpressionType.GreaterThanOrEqual || t == ExpressionType.GreaterThan
                )
            {
                // binary expression 
                object lv = _stack.Pop();
                ExpressionType lo = (ExpressionType)_stack.Pop();
                object ln = _stack.Pop();
                RDBExpression exp = RDBExpression.Equal;
                // FEATURE : add contains , between, startswith
                if (lo == ExpressionType.LessThan) exp = RDBExpression.Less;
                else if (lo == ExpressionType.LessThanOrEqual) exp = RDBExpression.LessEqual;
                else if (lo == ExpressionType.GreaterThan) exp = RDBExpression.Greater;
                else if (lo == ExpressionType.GreaterThanOrEqual) exp = RDBExpression.GreaterEqual;
                else if (lo == ExpressionType.NotEqual) exp = RDBExpression.NotEqual;

                _bitmap.Push(qexpression("" + ln, exp, lv));
            }

            if (t == ExpressionType.And || t == ExpressionType.AndAlso ||
                t == ExpressionType.Or || t == ExpressionType.OrElse)
            {
                if (_bitmap.Count > 1)
                {
                    // do bitmap operations 
                    WAHBitArray r = (WAHBitArray)_bitmap.Pop();
                    WAHBitArray l = (WAHBitArray)_bitmap.Pop();

                    if (t == ExpressionType.And || t == ExpressionType.AndAlso)
                        _bitmap.Push(r.And(l));
                    if (t == ExpressionType.Or || t == ExpressionType.OrElse)
                        _bitmap.Push(r.Or(l));
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
            // VB.net : e.g. CompareString(x.NoCase, "Me 4", False)
            if(s.StartsWith("CompareString"))
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
            if (mc.Contains("Between"))
            {
                // TODO : add code for between parsing here

                string name = m.Arguments[0].ToString().Split('.')[1];
                object from = GetValueForMember(m.Arguments[1]);
                object to = GetValueForMember(m.Arguments[2]);
                //var bits = qfromto(name, from, to);
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
