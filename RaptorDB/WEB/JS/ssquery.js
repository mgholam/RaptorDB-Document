
//------------------------------------------------------------------------------------------------------------------------
//----------------------------------------------- ssquery ----------------------------------------------------------------
/*
var ssquery = {
    createDataTable: function (url, tabname) {
        var startTime = new Date().getTime();
        var r = $('#' + tabname + ' #data').nodes[0];
        var m = models[tabname];

        $.ajax(url,
            function () {
                var table = "<table id = 'results'  >";
                m.rows = data.TotalCount;
                m.pages = Math.ceil(m.rows / m.count);
                if (m.pages == 0) m.pages = 1;
                $('#' + tabname + ' #pageof').each(function (i) { this.innerHTML = "" + m.page + " of " + m.pages });

                if (data.Rows.length > 0) {
                    for (var i = 0; i < data.Rows.length; i++) {
                        var it = data.Rows[i];
                        if (i == 0)
                            for (var p in it)
                                table += '<th><a onclick="query.sortcol(\'' + p + '\',\'' + tabname + '\')">' + p + '</a></th>';
                        table += "<tr>";
                        for (var p in it)
                            table += "<td>" + it[p] + "</td>";
                        table += "</tr>";
                    }
                    table += "</table>";
                    r.innerHTML = table;
                    $("#" + tabname + " .pager").show();
                }
                else {
                    $("#" + tabname + " .pager").hide();
                    r.innerHTML = "<font color='red'>No rows returned</font>";
                }
                var endTime = new Date().getTime();
                $('#' + tabname + ' pre').nodes[0].innerHTML = "Total Count = " + data.TotalCount + ", Count = " + data.Count + ", time (+render) = " + (endTime - startTime) + " ms";
            }
            , function (request) {
                $("#" + tabname + " .pager").hide();
                r.innerHTML = "<font color='red'>" + request.responseText + "</font>";
            });
    },

    rowsperpage: function (c, element) {
        var tabname = element.parentElement.parentElement.id;
        models[tabname].count = c;
        models[tabname].page = 1;
        this.doquery(tabname);
    },

    page: function (i, element) {
        var tabname = element.parentElement.parentElement.id;
        var m = models[tabname];
        m.page += i;
        if (m.page == 0) m.page = 1;
        if (m.page > m.pages) m.page = m.pages;
        this.doquery(tabname);
        document.location = "#";
    },

    schemachanged: function (element) {
        var tabname = element.parentElement.id;
        this.schemaselected(tabname);
    },

    schemaselected: function (tabname) {
        var x = $("#" + tabname + " #viewsel").nodes[0].value;
        models[tabname].vname = x;
        // fill schema
        $.ajax(server + 'RaptorDB/GetSchema?view=' + x,
            function () {
                var sel = $('#' + tabname + ' #schemasel').nodes[0];
                sel.options.length = 0;
                for (var i = 0; i < data.Rows.length; i++) {
                    var opt = document.createElement('option');
                    var it = data.Rows[i];
                    opt.innerHTML = it.ColumnName;
                    opt.value = opt.innerHTML;
                    sel.appendChild(opt);
                }
            });
    },

    run: function (element) {
        var tabname = element.parentElement.id;
        models[tabname].page = 1;
        models[tabname].sortcol = "";
        this.doquery(tabname);
    },

    doquery: function (tabname) {
        // if sort col exists -> query with sort
        var m = models[tabname];
        var sort = "";
        if (m.sortcol != "") sort = "?orderby=" + m.sortcol + (m.sort == "+" ? " " : " desc");
        var start = m.page - 1;
        if (start < 0) start = 0;
        start = start * m.count;
        var s = server + 'RaptorDB/Views/' + m.vname + '?' + $('#' + tabname + ' #filter').nodes[0].value + '?count=' + m.count + '?start=' + start + sort;
        this.createDataTable(s, tabname);
    },

    hookup: function (tabname) {
        var q = this;
        $.ajax(server + 'RaptorDB/GetViews',
        function () {
            console.log(tabname);
            var sel = $('#' + tabname + ' #viewsel').nodes[0];
            sel.options.length = 0;
            for (var i = 0; i < data.Rows.length; i++) {
                var opt = document.createElement('option');
                var it = data.Rows[i];
                opt.innerHTML = it.Name;
                opt.value = opt.innerHTML;
                sel.appendChild(opt);
                if (i == 0)
                    models[tabname].vname = it.Name;
            }
            q.schemaselected(tabname);
        });
        $("#" + tabname + " .pager").hide();
    },

    schemadbl: function (element) {
        var tabname = element.parentElement.id;
        var x = $("#" + tabname + " #schemasel").nodes[0].value;
        var f = $("#" + tabname + " #filter").nodes[0];
        f.value += " " + x + " ";
    },

    sortcol: function (colname, tabname) {
        var m = models[tabname];
        if (m.sortcol != colname) {
            m.sortcol = colname;
            m.sort = "+";
        }
        else {
            if (m.sort == "+") m.sort = "-";
            else m.sort = "+";
        }
        this.doquery(tabname);
    }
};
*/
