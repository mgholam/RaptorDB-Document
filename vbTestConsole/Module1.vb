Module Module1

    Sub Main()
        Dim rdb As RaptorDB.RaptorDB = RaptorDB.RaptorDB.Open("..\..\..\RaptorDBdata")
        rdb.RegisterView(New SampleViews.SalesInvoiceView())
        Dim r = rdb.Query(Of SampleViews.SalesInvoiceViewRowSchema)(Function(x) x.NoCase = "Me 4" And x.Serial < 10)
        Console.WriteLine(fastJSON.JSON.ToNiceJSON(r.Rows, New fastJSON.JSONParameters With {.UseExtensions = False}))
    End Sub

End Module
