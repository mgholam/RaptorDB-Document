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
        //public StringBuilder sb = new StringBuilder();
        public Stack<object> _stack = new Stack<object>();
        public Stack<object> _bitmap = new Stack<object>();
        //QueryFromTo qfromto;
        QueryExpression qexpression;

        protected override Expression VisitBinary(BinaryExpression b)
        {
            //sb.Append("(");
            this.Visit(b.Left);
            ExpressionType t = b.NodeType;

            if (t == ExpressionType.Equal || t == ExpressionType.LessThan || t == ExpressionType.LessThanOrEqual ||
               t == ExpressionType.GreaterThan || t == ExpressionType.GreaterThanOrEqual)
                _stack.Push(b.NodeType);

            #region [  stringbuilder  ]
            //switch (b.NodeType)
            //{
            //    //case ExpressionType.Not:
            //    //    sb.Append(" NOT ");
            //    //    break;
            //    //case ExpressionType.AndAlso:
            //    //case ExpressionType.And:
            //    //    sb.Append(" AND ");
            //    //    break;
            //    //case ExpressionType.OrElse:
            //    //case ExpressionType.Or:
            //    //    sb.Append(" OR ");
            //    //    break;
            //    case ExpressionType.Equal:
            //        //sb.Append(" = ");
            //        _stack.Push(b.NodeType);
            //        break;
            //    //case ExpressionType.NotEqual:
            //    //    sb.Append(" <> ");
            //    //    break;
            //    case ExpressionType.LessThan:
            //        //sb.Append(" < ");
            //        _stack.Push(b.NodeType);
            //        break;
            //    case ExpressionType.LessThanOrEqual:
            //        //sb.Append(" <= ");
            //        _stack.Push(b.NodeType);
            //        break;
            //    case ExpressionType.GreaterThan:
            //        //sb.Append(" > ");
            //        _stack.Push(b.NodeType);
            //        break;
            //    case ExpressionType.GreaterThanOrEqual:
            //        //sb.Append(" >= ");
            //        _stack.Push(b.NodeType);
            //        break;
            //} 
            #endregion

            this.Visit(b.Right);
            //sb.Append(")");
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
            //sb.Append(s.Substring(s.IndexOf('.') + 1));
            return m;
        }

        protected override Expression VisitMember(MemberExpression m)
        {
            var e = base.VisitMember(m);
            var c = m.Expression as ConstantExpression;
            if (c != null)
            {
                Type t = c.Value.GetType();
                var x = t.InvokeMember(m.Member.Name, BindingFlags.GetField, null, c.Value, null);
                _stack.Push(x);
                //sb.Append(x);
            }
            if (m.Expression != null && m.Expression.NodeType == ExpressionType.Parameter)
            {
                //sb.Append(m.Member.Name);
                _stack.Push(m.Member.Name);
                return e;
            }
            return e;
        }

        protected override Expression VisitConstant(ConstantExpression c)
        {
            IQueryable q = c.Value as IQueryable;
            if (q != null)
            {
                _stack.Push(q.ElementType.Name);
                //sb.Append(q.ElementType.Name);
            }
            else if (c.Value == null)
            {
                _stack.Push(null);
                //sb.Append("NULL");
            }
            else
            {
                _stack.Push(c.Value);
                if (Type.GetTypeCode(c.Value.GetType()) == TypeCode.Object)
                    _stack.Pop();

                #region [  stringbuilder  ]
                //switch (Type.GetTypeCode(c.Value.GetType()))
                //{
                //    case TypeCode.Boolean:
                //        sb.Append(((bool)c.Value) ? 1 : 0);
                //        break;
                //    case TypeCode.String:
                //        sb.Append("'");
                //        sb.Append(c.Value);
                //        sb.Append("'");
                //        break;
                //    case TypeCode.Object:
                //        _stack.Pop();
                //        break;
                //    default:
                //        sb.Append(c.Value);
                //        break;
                //} 
                #endregion
            }
            return c;
        }
    }
}
