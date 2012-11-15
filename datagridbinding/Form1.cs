using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Windows.Forms;
using System.Reflection;
using RaptorDB;
using RaptorDB.Common;
using SampleViews;

namespace datagridbinding
{
    public partial class Form1 : Form
    {
        public Form1()
        {
            InitializeComponent();
        }


        IRaptorDB rap;

        private void Form1_Load(object sender, EventArgs e)
        {
            dataGridView1.DoubleBuffered(true);
            Form2 f = new Form2();
            if (f.ShowDialog() == System.Windows.Forms.DialogResult.OK)
            {
                rap = f._rap;

                Query();
            }
        }

        void TextBox1KeyPress(object sender, KeyPressEventArgs e)
        {
            if (e.KeyChar == (char)Keys.Return)
                Query();
        }

        private void Query()
        {
            string[] s = textBox1.Text.Split(',');

            try
            {
                DateTime dt = FastDateTime.Now;
                var q = rap.Query(s[0].Trim(), s[1].Trim());
                toolStripStatusLabel2.Text = "Query time (sec) = " + FastDateTime.Now.Subtract(dt).TotalSeconds;
                dataGridView1.DataSource = q.Rows;
                toolStripStatusLabel1.Text = "Count = " + q.Count.ToString("#,0");
            }
            catch { }
        }

        private void sumQueryToolStripMenuItem_Click(object sender, EventArgs e)
        {
            int c = rap.Count("SalesItemRows", "product = \"prod 1\"");

            DateTime dt = FastDateTime.Now;
            var q = //rap.Query(typeof(SalesItemRowsView), (LineItem l) => (l.Product == "prod 1" || l.Product == "prod 3"));
                rap.Query<SalesItemRowsView.RowSchema>(x => x.Product == "prod 1" || x.Product == "prod 3");
            //List<SalesItemRowsView.RowSchema> list = q.Rows.Cast<SalesItemRowsView.RowSchema>().ToList();
            var res = from item in q.Rows//list
                      group item by item.Product into grouped
                      select new
                      {
                          Product = grouped.Key,
                          TotalPrice = grouped.Sum(product => product.Price),
                          TotalQTY = grouped.Sum(product => product.QTY)
                      };

            var reslist = res.ToList();
            dataGridView1.DataSource = reslist;
            toolStripStatusLabel2.Text = "Query time (sec) = " + FastDateTime.Now.Subtract(dt).TotalSeconds;
            toolStripStatusLabel1.Text = "Count = " + q.Count.ToString("#,0");
        }

        private void exitToolStripMenuItem_Click(object sender, EventArgs e)
        {
            if (rap != null)
                rap.Shutdown();
            this.Close();
        }

        private object _lock = new object();
        private void insert100000DocumentsToolStripMenuItem_Click(object sender, EventArgs e)
        {
            lock (_lock)
            {
                DialogResult dr = MessageBox.Show("Do you want to insert?", "Continue?", MessageBoxButtons.OKCancel, MessageBoxIcon.Stop, MessageBoxDefaultButton.Button2);
                if (dr == System.Windows.Forms.DialogResult.Cancel)
                    return;
                toolStripProgressBar1.Value = 0;
                DateTime dt = FastDateTime.Now;
                int count = 100000;
                int step = 5000;
                toolStripProgressBar1.Maximum = (count / step) + 1;
                Random r = new Random();
                for (int i = 0; i < count; i++)
                {
                    var inv = new SalesInvoice()
                    {
                        Date = FastDateTime.Now.AddMinutes(r.Next(60)),
                        Serial = i % 10000,
                        CustomerName = "me " + i % 10,
                        Status = (byte)(i % 4),
                        Address = "df asd sdf asdf asdf"
                    };
                    inv.Items = new List<LineItem>();
                    for (int k = 0; k < 5; k++)
                        inv.Items.Add(new LineItem() { Product = "prod " + k, Discount = 0, Price = 10 + k, QTY = 1 + k });
                    if (i % step == 0)
                        toolStripProgressBar1.Value++;
                    rap.Save(inv.ID, inv);
                }
                MessageBox.Show("Insert done in (sec) : " + FastDateTime.Now.Subtract(dt).TotalSeconds);
                toolStripProgressBar1.Value = 0;
            }
        }

        private void backupToolStripMenuItem_Click(object sender, EventArgs e)
        {
            bool b = rap.Backup();
            MessageBox.Show("Backup done");
        }

        private void restoreToolStripMenuItem_Click(object sender, EventArgs e)
        {
            rap.Restore();
        }

        public class objclass
        {
            public string val;
        }
        string prod3 = "prod 3";
        private void serverSideSumQueryToolStripMenuItem_Click(object sender, EventArgs e)
        {
            string prod1 = "prod 1";
            objclass c = new objclass() { val = "prod 3" };
            decimal i = 20;

            //var q = rap.Count(typeof(SalesItemRowsView), 
            //    (LineItem l) => (l.Product == prod1 || l.Product == prod3) && l.Price.Between(10,i)
            //    );
            
            DateTime dt = FastDateTime.Now;
           
            var qq = rap.ServerSide<LineItem>(Views.ServerSide.Sum_Products_based_on_filter,
                //"product = \"prod 1\""
                //(LineItem l) => (l.Product == c.val || l.Product == prod3 ) 
                x=> x.Product == c.val || x.Product == prod3
                ).ToList();
            dataGridView1.DataSource = qq;
            toolStripStatusLabel2.Text = "Query time (sec) = " + FastDateTime.Now.Subtract(dt).TotalSeconds;
            toolStripStatusLabel1.Text = "Count = " + qq.Count.ToString("#,0");
        }

        private void testToolStripMenuItem_Click(object sender, EventArgs e)
        {
            int c = rap.Count<SalesInvoiceView.RowSchema>(x => x.Serial < 100);
            var q = rap.Query<SalesInvoiceView.RowSchema>(x => x.Serial < 100, 0, 10);
            q= rap.Query<SalesInvoiceView.RowSchema>("serial <100");
            string s = q.Rows[0].CustomerName;
        }
    }
}