//-----------------------------------------------------------------------------------------------------------
//------------------------------------------ query  ---------------------------------------------------------
var query = {
    createDataTable: function (url, tabname) {
        var startTime = new Date().getTime();
        var r = $('#' + tabname + ' #data').first();
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
                        for (var p in it) {
                            table += "<td>";
                            if (p == "docid")
                                table += "<a href='#' onclick='docview.show(\"" + it[p] + "\"); event.preventDefault();'>" + it[p] + "</a>";
                            else
                                table += it[p];
                            table += "</td>";
                        }
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
                $('#' + tabname + ' pre').first().innerHTML = "Total Count = " + data.TotalCount + ", Count = " + data.Count + ", time (+render) = " + (endTime - startTime) + " ms";
            }
            , function (request) {
                $("#" + tabname + " .pager").hide();
                r.innerHTML = "<font color='red'>" + request.responseText + "</font>";
            });
    },

    rowsperpage: function (c, tabname) {
        models[tabname].count = c;
        models[tabname].page = 1;
        this.doquery(tabname);
    },

    page: function (i, tabname) {
        var m = models[tabname];
        m.page += i;
        if (m.page == 0) m.page = 1;
        if (m.page > m.pages) m.page = m.pages;
        this.doquery(tabname);
    },

    schemachanged: function (element) {
        var tabname = element.parentElement.id;
        this.schemaselected(tabname);
    },

    schemaselected: function (tabname) {
        var x = $("#" + tabname + " #viewsel").first().value;
        models[tabname].vname = x;
        // fill schema
        $.ajax(server + 'RaptorDB/GetSchema?view=' + x,
            function () {
                var d = $('#' + tabname + ' #columns').first();
                d.innerHTML = "";
                for (var i = 0; i < data.Rows.length; i++) {
                    var it = data.Rows[i];
                    var dv = document.createElement("a");
                    dv.innerHTML = '<a href="" style="display:block" onclick="query.columnadd(\'' + tabname.trim() +
                        '\',\'' + it.ColumnName + '\'); event.preventDefault();">' + it.ColumnName + '</a>';
                    d.appendChild(dv);
                }
            });
    },

    run: function (element) {
        var tabname = element.parentElement.id;
        models[tabname].page = 1;
        models[tabname].sortcol = "";
        this.doquery(tabname);
    },

    excel: function (element) {
        var tabname = element.parentElement.id;
        models[tabname].page = 1;
        models[tabname].sortcol = "";
        var m = models[tabname];
        var s = server + 'RaptorDB/ExcelExport/' + m.vname + '?' + $('#' + tabname + ' #filter').first().value;
        window.open(s);
    },

    doquery: function (tabname) {
        // if sort col exists -> query with sort
        var m = models[tabname];
        var sort = "";
        if (m.sortcol != "") sort = "?orderby=" + m.sortcol + (m.sort == "+" ? " " : " desc");
        var start = m.page - 1;
        if (start < 0) start = 0;
        start = start * m.count;
        var s = server + 'RaptorDB/Views/' + m.vname + '?' + $('#' + tabname + ' #filter').first().value + '?count=' + m.count + '?start=' + start + sort;
        this.createDataTable(s, tabname);
    },

    hookup: function (tabname) {
        $.ajax(server + 'RaptorDB/GetViews',
            function () {
                //console.log(tabname);
                var sel = $('#' + tabname + ' #viewsel').first();
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
                query.schemaselected(tabname);
                //query.doquery(tabname);
            });
        $("#" + tabname + " .pager").hide();
    },

    columnadd: function (tabname, col) {
        var f = $("#" + tabname + " #filter").first();
        f.value += " " + col + " ";
        f.value = f.value.trim();
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


