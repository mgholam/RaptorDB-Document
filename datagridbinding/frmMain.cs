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
using System.Linq.Expressions;
using System.IO;

namespace datagridbinding
{
    public partial class frmMain : Form
    {
        public frmMain()
        {
            InitializeComponent();
        }


        IRaptorDB rap;

        private void Form1_Load(object sender, EventArgs e)
        {
            dataGridView1.DoubleBuffered(true);
            frmStartup f = new frmStartup();
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
                stsError.Text = "";
            }
            catch (Exception ex)
            {
                stsError.Text = ex.Message;
                dataGridView1.DataSource = null;
                toolStripStatusLabel1.Text = "Count = 0";
                toolStripStatusLabel2.Text = "Query time (sec) = 0";
            }
        }

        private void sumQueryToolStripMenuItem_Click(object sender, EventArgs e)
        {
            int c = rap.Count("SalesItemRows", "product = \"prod 1\"");

            DateTime dt = FastDateTime.Now;
            var q = //rap.Query(typeof(SalesItemRowsView), (LineItem l) => (l.Product == "prod 1" || l.Product == "prod 3"));
                rap.Query<SalesItemRowsViewRowSchema>(x => x.Product == "prod 1" || x.Product == "prod 3");
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
            //RaptorDB.Global.SplitStorageFilesMegaBytes = 50;
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
                    var inv = CreateInvoice(i);
                    if (i % step == 0)
                        toolStripProgressBar1.Value++;
                    rap.Save(inv.ID, inv);
                }
                MessageBox.Show("Insert done in (sec) : " + FastDateTime.Now.Subtract(dt).TotalSeconds);
                toolStripProgressBar1.Value = 0;
            }
        }

        private static SalesInvoice CreateInvoice(int i)
        {
            var inv = new SalesInvoice()
            {
                Date = Faker.DateTimeFaker.BirthDay(),// FastDateTime.Now.AddMinutes(r.Next(60)),
                Serial = i % 10000,
                CustomerName = Faker.NameFaker.Name(),// "Me " + i % 10,
                NoCase = "Me " + i % 10,
                Status = (byte)(i % 4),
                Address = Faker.LocationFaker.Street(), //"df asd sdf asdf asdf",
                Approved = i % 100 == 0 ? true : false
            };
            inv.Items = new List<LineItem>();
            for (int k = 0; k < 5; k++)
                inv.Items.Add(new LineItem() { Product = "prod " + k, Discount = 0, Price = 10 + k, QTY = 1 + k });
            return inv;
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
            //string prod1 = "prod 1";
            objclass c = new objclass() { val = "prod 3" };
            //decimal i = 20;

            //var q = rap.Count(typeof(SalesItemRowsView), 
            //    (LineItem l) => (l.Product == prod1 || l.Product == prod3) && l.Price.Between(10,i)
            //    );

            DateTime dt = FastDateTime.Now;

            var qq = rap.ServerSide<LineItem>(Views.ServerSide.Sum_Products_based_on_filter,
                //"product = \"prod 1\""
                //(LineItem l) => (l.Product == c.val || l.Product == prod3 ) 
                x => x.Product == c.val || x.Product == prod3
                ).ToList();
            dataGridView1.DataSource = qq;
            toolStripStatusLabel2.Text = "Query time (sec) = " + FastDateTime.Now.Subtract(dt).TotalSeconds;
            toolStripStatusLabel1.Text = "Count = " + qq.Count.ToString("#,0");
        }

        private void KVHFtest()
        {
            //var r = (rap as RaptorDB.RaptorDB);
            var kv = rap.GetKVHF();
            
            DateTime dt = DateTime.Now;
            for (int i = 0; i < 100000; i++)
            {
                var o = CreateInvoice(i);
                kv.SetObjectHF(i.ToString(), o);// new byte[100000]);
            }
            MessageBox.Show("time = " + DateTime.Now.Subtract(dt).TotalSeconds);

            var g = kv.GetObjectHF("1009");

            for (int i = 0; i < 100000; i++)
                kv.DeleteKeyHF(i.ToString());
            
            g = kv.GetObjectHF("1009");
            MessageBox.Show(""+kv.CountHF());

            foreach (var f in Directory.GetFiles("d:\\pp", "*.*"))
            {
                kv.SetObjectHF(f, File.ReadAllBytes(f));
            }
            
            kv.CompactStorageHF();

            foreach (var f in Directory.GetFiles("d:\\pp", "*.*"))
            {
                var o = kv.GetObjectHF(f);
                File.WriteAllBytes(f.Replace("\\pp\\", "\\ppp\\"), o as byte[]);
            }
            bool b = kv.ContainsHF("aa");
            var keys = kv.GetKeysHF();
            //foreach(var o in r.KVHF.EnumerateObjects())
            //{
            //    string s = o.GetType().ToString();
            //}
        }

        private void testToolStripMenuItem_Click(object sender, EventArgs e)
        {
            GC.Collect(2);
            //KVHFtest();


            int c = rap.Count<SalesInvoiceViewRowSchema>(x => x.Serial < 100);
            c = rap.Count<SalesInvoiceViewRowSchema>(x => x.Serial != 100);
            c = rap.Count("SalesInvoice", "serial != 100");
            var q = rap.Query<SalesInvoiceViewRowSchema>(x => x.Serial < 100, 0, 10, "serial desc");
            //var p = rap.Query("SalesInvoice");
            //var pp = rap.Query(typeof(SalesInvoiceView));
            //var ppp = rap.Query(typeof(SalesItemRowsView.RowSchema));
            //var pppp = rap.Query(typeof(SalesInvoiceView), (SalesInvoiceView.RowSchema r) => r.Serial < 10);
            //var ppppp = rap.Query(typeof(SalesInvoiceView.RowSchema), (SalesInvoiceView.RowSchema r) => r.Serial < 10);
            //var pppppp = rap.Query<SalesInvoiceView.RowSchema>("serial <10");
            //Guid g = new Guid("82997e60-f8f4-4b37-ae35-02d033512673");
            var qq = rap.Query<SalesInvoiceViewRowSchema>(x => x.docid == new Guid("82997e60-f8f4-4b37-ae35-02d033512673"));
            dataGridView1.DataSource = q.Rows;

            //int i = rap.ViewDelete<SalesInvoiceViewRowSchema>(x => x.Serial == 0);

            //var qqq= rap.Query<SalesInvoiceViewRowSchema>(x => );
            //SalesInvoiceViewRowSchema s = new SalesInvoiceViewRowSchema();
            //s.docid = Guid.NewGuid();
            //s.CustomerName = "hello";
            //rap.ViewInsert<SalesInvoiceViewRowSchema>(s.docid, s);
            //q= rap.Query<SalesInvoiceView.RowSchema>("serial <100");
            //string s = q.Rows[0].CustomerName;

            //perftest();
        }

        //private void perftest()
        //{
        //    DateTime dt = DateTime.Now;

        //    for (int i = 0; i < 100000; i++)
        //    {
        //        var s = new SalesInvoiceViewRowSchema();
        //        s.docid = Guid.NewGuid();
        //        s.Address = Faker.LocationFaker.Street();
        //        s.CustomerName = Faker.NameFaker.Name();
        //        s.Date = Faker.DateTimeFaker.BirthDay();
        //        s.Serial = i % 1000;
        //        s.Status = (byte)(i % 5);
        //        rap.ViewInsert(s.docid, s);
        //    }
        //    MessageBox.Show("time = " + DateTime.Now.Subtract(dt).TotalSeconds);
        //}
    }
}