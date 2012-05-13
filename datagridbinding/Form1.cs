using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Windows.Forms;
using System.Reflection;

namespace datagridbinding
{
	public partial class Form1 : Form
	{
		public Form1()
		{
			InitializeComponent();
		}


		RaptorDB.RaptorDB rap;
		
		private void Form1_Load(object sender, EventArgs e)
		{
			rap = RaptorDB.RaptorDB.Open(@"..\..\..\RaptorDBdata");

			rap.RegisterView(new SalesInvoiceView());
			rap.RegisterView(new SalesItemRowsView());
			rap.RegisterView(new newview());

            Query();
		}
		
		void TextBox1KeyPress(object sender, KeyPressEventArgs e)
		{
			if(e.KeyChar == (char)Keys.Return)
                Query();
		}

        private void Query()
        {
            string[] s = textBox1.Text.Split(',');

            try
            {
                DateTime dt = RaptorDB.FastDateTime.Now;
                var q = rap.Query(s[0].Trim(), s[1].Trim());
                toolStripStatusLabel2.Text = "Query time (sec) = " + RaptorDB.FastDateTime.Now.Subtract(dt).TotalSeconds;
                dataGridView1.DataSource = q.Rows;
                toolStripStatusLabel1.Text = "Count = " + q.Count.ToString("#,0");
            }
            catch { }
        }
	}
}