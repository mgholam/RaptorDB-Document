using System;
using System.Windows.Forms;
using RaptorDB.Common;
using SampleViews;

namespace datagridbinding
{
    public partial class frmStartup : Form
    {
        public frmStartup()
        {
            InitializeComponent();
        }

        public IRaptorDB _rap;

        private void button2_Click(object sender, EventArgs e)
        {
            this.Close();
            Application.Exit();
        }

        private void button1_Click(object sender, EventArgs e)
        {
            if (radioButton1.Checked)
            {
                var p = RaptorDB.RaptorDB.Open(txtFolder.Text);
                p.RegisterView(new SalesInvoiceView());
                p.RegisterView(new SalesItemRowsView());
                p.RegisterView(new newview());
                _rap = p;
            }
            else
            {
                _rap = new RaptorDB.RaptorDBClient(txtServer.Text, int.Parse(txtPort.Text), txtUser.Text, txtPassword.Text);
            }
        }

        private void radioButton1_CheckedChanged(object sender, EventArgs e)
        {
            if (radioButton1.Checked)
            {
                groupBox1.Visible = true;
                groupBox2.Visible = false;
            }
            else
            {
                groupBox1.Visible = false;
                groupBox2.Visible = true;
            }
        }
    }
}
