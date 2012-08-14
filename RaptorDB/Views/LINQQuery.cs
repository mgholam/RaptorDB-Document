using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Linq.Expressions;
using System.Reflection;

namespace RaptorDB.Views
{
    //FEATURE : handle Contains, StartsWith, Between predicates

    //delegate WAHBitArray QueryFromTo(string colname, object from, object to);
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
            this.Visit(b.Left);
            ExpressionType t = b.NodeType;

            if (t == ExpressionType.Equal || t == ExpressionType.LessThan || t == ExpressionType.LessThanOrEqual ||
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

                _bitmap.Push(qexpression("" + ln, exp, lv));
            }

            if (t == ExpressionType.And || t == ExpressionType.AndAlso ||
                t == ExpressionType.Or || t == ExpressionType.OrElse)
            {
                // do bitmap operations 
                WAHBitArray r = (WAHBitArray)_bitmap.Pop();
                WAHBitArray l = (WAHBitArray)_bitmap.Pop();

                if (t == ExpressionType.And || t == ExpressionType.AndAlso)
                    _bitmap.Push(r.And(l));
                if (t == ExpressionType.Or || t == ExpressionType.OrElse)
                    _bitmap.Push(r.Or(l));
            }
            return b;
        }

        protected override Expression VisitMethodCall(MethodCallExpression m)
        {
            string s = m.ToString();
            _stack.Push(s.Substring(s.IndexOf('.') + 1));
            return m;
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
