// -----------------------------------  jquery like ----------------------------------------------
$ = function (selector) {
    if (!(this instanceof $))
        return new $(selector);
    var doc = document;
    this.nodes = doc.getElementById(selector);
    if (this.nodes == null)
        this.nodes = doc.querySelectorAll(selector);
    else
        return this.nodes;
};

$.prototype = {
    each: function (callback) {
        if (this.nodes != null) {
            for (var i = 0; i < this.nodes.length; ++i) {
                callback.call(this.nodes[i], i);
            }
        }
        else
            callback.call(this, 0);
        return this; // to allow chaining like jQuery does
    },
    hide: function () {
        this.each(function (i) { this.style.display = "none"; })
        return this;
    },
    show: function () {
        this.each(function (i) { this.style.display = ""; })
        return this;
    },
    first: function () {
        if (this.nodes != null)
            return this.nodes[0];
    }
}

$.ajax = function (url, callback, error) {
    request = new XMLHttpRequest();
    request.open('GET', url);

    request.onreadystatechange = function () {
        if (request.readyState == 1) { return }
        else if (request.readyState == 4) {
            if (request.status < 400) {
                if (request.responseText != "")
                    data = JSON.parse(request.responseText);
                else
                    data = "";
                callback(data);
            }
            else if (error != null)
                error(request);
        }
    };
    request.onerror = function () {
        $.showDialog("Sorry", "<p>Connection Failed!</p>");
    }
    request.send();
}

$.load = function (url, callback, error) {
    request = new XMLHttpRequest();
    request.open('GET', url);

    request.onreadystatechange = function () {
        if (request.readyState == 1) { return }
        else if (request.readyState == 4) {
            if (request.status < 400) {
                callback(request.responseText);
            }
            else if (error != null)
                error(request);
        }
    };
    request.onerror = function () {
        $.showDialog("Sorry", "<p>Connection Failed!</p>");
    }
    request.send();
}

$.showDialog = function (caption, innerhtml) {
    var body = document.body;
    var d = document.createElement('div');
    d.id = "openModal";
    d.className = "modalDialog";
    d.innerHTML = '<div><a onclick="$.closemodal()" title="Close" class="close">X</a><h2>' + caption + '</h2>' + innerhtml + '</div>';
    body.appendChild(d);
    document.location = "#openModal";
    document.onkeypress = function () {
        $.closemodal();
    };
}

$.closemodal = function () {
    var d = $("openModal");
    document.body.removeChild(d);
    document.onkeypress = null;
}





//-----------------------------------------------------------------------------------------------------------
//------------------------------------------ nav menu -------------------------------------------------------
//-----------------------------------------------------------------------------------------------------------
var nav = {
    tabmenuclick: function (item, a) {
        //console.log("tab click : " + item);
        $(".tab-content").hide();
        $(".tab-links li").each(function (i) { this.className = ""; });
        $('.tab-content#' + item).first().style.display = "block";
        var p = a.parentElement;
        p.className = "active";
    },

    navclick: function (a) {
        var id = a.id;
        if (id !== "") {
            console.log(id);
            var t = $(".tab-links li#" + id);

            if (t.nodes.length > 0) { // activate existing
                nav.tabmenuclick(id, t.first().childNodes[0]);
            }
            else {
                nav.loadtab(id);
            }
        }
    },

    loadtab: function (name, func) {
        switch (name) {
            case "query":
                $.load(server + "query.html",
                    function (res) {
                        var tname = "query" + Math.uuid(4, 16);
                        models[tname] = new modelobj();
                        nav.createTab(tname, "Query", res.replace(/\$tabname/g, tname));
                        query.hookup(tname);
                        if (func != null)
                            func(tname);
                    });
                break;

            case "sysinfo":
                $.load("systeminfo.html",
                    function (res) {
                        var tname = "info" + Math.uuid(4, 16);
                        nav.createTab(tname, "System Information", res.replace(/\$tabname/g, tname));
                        system.run(tname);
                        if (func != null)
                            func(tname);
                    });
                break;

            case "sysconfig":
                $.load("configs.html",
                    function (res) {
                        var tname = "conf" + Math.uuid(4, 16);
                        nav.createTab(tname, "System Configs", res.replace(/\$tabname/g, tname));
                        system.configs(tname);
                        if (func != null)
                            func(tname);
                    });
                break;

            case "ssquery":
                this.createTab("tab" + Math.uuid(4, 16), "Server Side", "<p>" + new Date() + "</p>");
                break;

            case "docview":
                $.load("docview.html",
                    function (res) {
                        var tname = "docv" + Math.uuid(4, 16);
                        nav.createTab(tname, "Document View", res.replace(/\$tabname/g, tname));
                        if (func != null)
                            func(tname);
                    });
                break;

            case "dochistory":
                $.load("dochistory.html",
                    function (res) {
                        var tname = "doch" + Math.uuid(4, 16);
                        nav.createTab(tname, "Document History", res.replace(/\$tabname/g, tname));
                        if (func != null)
                            func(tname);
                    });
                break;

            case "docsearch":
                $.load("docsearch.html",
                    function (res) {
                        var tname = "doch" + Math.uuid(4, 16);
                        models[tname] = new modelobj();
                        var m = models[tname];
                        m.count = 20;
                        m.start = 0;
                        m.page = 1;
                        m.pages = 1;
                        nav.createTab(tname, "Document Search", res.replace(/\$tabname/g, tname));
                        if (func != null)
                            func(tname);
                    });
                break;

            case "hfview":
                $.load("hfbrowse.html",
                    function (res) {
                        var tname = "hfv" + Math.uuid(4, 16);
                        models[tname] = new modelobj();
                        var m = models[tname];
                        m.count = 20;
                        m.start = 0;
                        m.page = 1;
                        m.pages = 1;
                        nav.createTab(tname, "HF Browse", res.replace(/\$tabname/g, tname));
                        if (func != null)
                            func(tname);
                    });
                break;

            case "fileview":
                $.load("fileview.html",
                    function (res) {
                        var tname = "filev" + Math.uuid(4, 16);
                        nav.createTab(tname, "File View", res.replace(/\$tabname/g, tname));
                        if (func != null)
                            func(tname);
                    });
                break;

            default:
                $.showDialog("Sorry!", "<p>Not implemented yet.</p>");
                break;
        }
    },

    createTab: function (name, text, inner) {
        var tl = $(".tab-links").first();
        var li = document.createElement("li");
        li.innerHTML = '<div onclick="nav.tabmenuclick(\'' + name + '\', this)"><a>' + text + '</a><a id="close" onclick="nav.closetab(\'' + name + '\',event)">X</a></div>';
        li.id = name;
        tl.appendChild(li);

        var md = $(".Middle").first();
        var d = document.createElement("div");
        d.id = name;
        d.className = "tab-content";
        d.innerHTML = inner;
        md.appendChild(d);
        nav.tabmenuclick(name, li.childNodes[0]);
    },

    closetab: function (name, event) {
        $(".tab-content").hide(); // hide all content
        var n = $(".tab-content#" + name).first();
        var p = n.parentElement;
        p.removeChild(n); // remove selected content
        $(".tab-links li").each(function (i) { this.className = ""; }); // deactivate all tabs
        n = $(".tab-links li#" + name).first();
        var prev = n.previousElementSibling; // prev tab 
        p = n.parentElement;
        p.removeChild(n); // remove selected tab
        prev.className = "active"; // show prev tab
        $(".tab-content#" + prev.id).first().style.display = "block"; // show prev tab 
        event.stopPropagation();
    }
};



//-------------------------------------------------------------------------------------------------------------
//--------------------------------------------- start up ------------------------------------------------------
var server = "";

(function () {
    if (document.addEventListener)   // IE9+ only
        document.addEventListener('DOMContentLoaded', onLoad);
    else {
        // must be IE
        document.attachEvent("onreadystatechange", readyStateChange);
        window.attachEvent("onload", onLoad);
    }

    function readyStateChange() {
        if (document.readyState === "complete") {
            onLoad();
        }
    }

    function onLoad() {
        var s = document.URL;
        if (s.indexOf("#", 0) > 0)
            s = s.substring(0, s.indexOf("#"));

        if (s.indexOf("/", s.length - 1) === -1)
            s += "/";
        server = s;
        startup();
    }

    function startup() {
        $.load("nav.html", function (res) {
            $(".Left").first().innerHTML = res;
            // hook up nav link clicks
            $(".panel a").each(function (i) { this.onclick = function (e) { nav.navclick(this); } });
            $(".accordion").each(function (i) {
                this.onclick = function (e) {
                    $(".accordion").each(function (i) {
                        if (this.classList.contains("active")) {
                            this.classList.toggle("active");
                            this.nextElementSibling.classList.toggle("show");
                        }
                    });
                    this.classList.toggle("active");
                    this.nextElementSibling.classList.toggle("show");
                }
            });
        });
        $(".tab-content#help").first().style.display = "block";
        $(".tab-links li").first().className = "active";

        $("javascriptmsg").style.display = "none";
        $(".Container").first().style.display = "block";
    }
})();

var models = {};

function modelobj() {
    this.count = 10;
    this.page = 1;
    this.pages = 1;
    this.vname = "aa";
    this.sortcol = "";
    this.sort = "asc";
    this.rows = 0;
}



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
        var q = this;
        $.ajax(server + 'RaptorDB/GetViews',
        function () {
            console.log(tabname);
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
            q.schemaselected(tabname);
        });
        $("#" + tabname + " .pager").hide();
    },

    columnadd: function(tabname, col){
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


//------------------------------------------------------------------------------------------------------------------------
//----------------------------------------------- system -----------------------------------------------------------------
//------------------------------------------------------------------------------------------------------------------------
var system = {
    run: function (tabname) {
        $.ajax("/raptordb/systeminfo", function () {
            weld($(".tab-content#" + tabname + " .data").first(), data);
        });
    },

    configs: function (tabname) {
        $.load("/raptordb/action?getconfigs", function (res) {
            $(".tab-content#" + tabname + " .rdbconfig").first().innerText = res;
        });
    },

    action: function (tabname, cmd) {
        $.ajax("/raptordb/action?" + cmd, function () {
            system.run(tabname);
        });
    }
};


//---------------------------------------------------------------------
//----------------------------docview----------------------------------
var docview = {

    get: function (tabname) {
        var docid = $('#' + tabname + ' #docid').first().value;
        $.load("/raptordb/docget?" + docid,
            function (res) {
                var err = $("#" + tabname + " #err").first();
                err.innerText = "";
                $("#" + tabname + " #data").first().innerText = res;
                var n = $("#" + tabname + " .disp").first();
                if (res == "") {
                    n.style.display = "none";
                    err.innerText = "docid not found."
                }
                else {
                    n.style.display = "";
                    $.ajax("/raptordb/dochistory?" + docid,
                        function () {
                            $("#" + tabname + " #revisions").first().innerText = data.length;
                        });
                }
            });
    },

    getversion: function (tabname, el) {
        var ver = el.id;
        $.load("/raptordb/docversion?" + ver,
            function (res) {
                $("#" + tabname + " #data").first().innerText = res;
            });
    },

    show: function (docid) {
        nav.loadtab("docview", function (tabname) {
            $('#' + tabname + ' #docid').first().value = docid;
            docview.get(tabname);
        });
    },

    fillhistory: function (name) {
        var docid = $('#' + name + ' #docid').first().value;
        $.ajax("/raptordb/dochistory?" + docid,
           function () {
               var n = $("#" + name + " #versions").first();
               n.innerHTML = "";
               var err = $("#" + name + " #err").first();
               err.innerText = "";
               if (data != "") {
                   $("#" + name + " #revisions").first().innerText = data.length;

                   for (var i = 0; i < data.length; i++) {
                       var dv = document.createElement("a");
                       dv.innerHTML = '<a href="" style="display:block" onclick="docview.getversion(\'' + name.trim() +
                           '\',this); event.preventDefault();" id="' + data[i].Version +
                           '">Version = ' + data[i].Version +
                           ', Date = ' + data[i].ChangeDate.replace("T", " ").replace("Z", "") +
                           '</a>';
                       n.appendChild(dv);
                   }
               }
               else {
                   err.innerText = "docid not found."
                   $("#" + name + " #data").first().innerText = "";
                   $("#" + name + " #revisions").first().innerText = "";
               }
           });
    },

    search: function (name) {
        var docid = $('#' + name + ' #docid').first().value;
        var m = models[name];
        if (m.pages == 0) m.pages = 1;
        $('#' + name + ' #pageof').each(function (i) { this.innerHTML = "" + m.page + " of " + m.pages });
        var s = m.page - 1;
        if (s < 0) s = 0;
        s = s * m.count;
        $.ajax("/raptordb/docsearch?" + docid + "&start=" + s + "&count=" + m.count,
           function () {
               m.rows = data.TotalCount;
               m.pages = Math.ceil(m.rows / m.count);
               if (m.pages == 0) m.pages = 1;
               $('#' + name + ' #pageof').each(function (i) { this.innerHTML = "" + m.page + " of " + m.pages });
               var s = m.page - 1;
               if (s < 0) s = 0;
               s = s * m.count;
               var n = $("#" + name + " #versions").first();
               n.innerHTML = "";
               var err = $("#" + name + " #err").first();
               err.innerText = "";
               if (data != "") {
                   $("#" + name + " #revisions").first().innerText = data.TotalCount;

                   for (var i = 0; i < data.Items.length; i++) {
                       var dv = document.createElement("a");
                       dv.innerHTML = '<a href="" style="display:block" onclick="docview.getversion(\'' + name.trim() +
                           '\',this); event.preventDefault();" id="' + data.Items[i] +
                           '">Document number = ' + data.Items[i] +
                           '</a>';
                       n.appendChild(dv);
                   }
               }
               else {
                   err.innerText = "Documents not found."
                   $("#" + name + " #data").first().innerText = "";
                   $("#" + name + " #revisions").first().innerText = "";
               }
           });
    },

    page: function (i, tabname) {
        var m = models[tabname];
        m.page += i;
        if (m.page == 0) m.page = 1;
        if (m.page > m.pages) m.page = m.pages;
        docview.search(tabname);
    },

    history: function (tabname) {
        var docid = $('#' + tabname + ' #docid').first().value;
        // open new tab page
        nav.loadtab("dochistory", function (name) {
            $('#' + name + ' #docid').first().value = docid;
            docview.fillhistory(name);
        });
    }
};

//---------------------------------------------------------------------
//----------------------------hfview----------------------------------
var hfview = {

    search: function (name) {
        var docid = $('#' + name + ' #docid').first().value;
        var m = models[name];
        if (m.pages == 0) m.pages = 1;
        $('#' + name + ' #pageof').each(function (i) { this.innerHTML = "" + m.page + " of " + m.pages });
        var s = m.page - 1;
        if (s < 0) s = 0;
        s = s * m.count;
        $.ajax("/raptordb/hfkeys?" + docid + "&start=" + s + "&count=" + m.count,
           function () {
               m.rows = data.TotalCount;
               m.pages = Math.ceil(m.rows / m.count);
               if (m.pages == 0) m.pages = 1;
               $('#' + name + ' #pageof').each(function (i) { this.innerHTML = "" + m.page + " of " + m.pages });
               var s = m.page - 1;
               if (s < 0) s = 0;
               s = s * m.count;
               var n = $("#" + name + " #versions").first();
               n.innerHTML = "";
               var err = $("#" + name + " #err").first();
               err.innerText = "";
               if (data != "") {
                   $("#" + name + " #revisions").first().innerText = data.TotalCount;

                   for (var i = 0; i < data.Items.length; i++) {
                       var dv = document.createElement("a");
                       dv.innerHTML = '<a href="" style="display:block" onclick="hfview.get(\'' + name.trim() +
                           '\',this); event.preventDefault();" id="' + data.Items[i] +
                           '">Key = ' + data.Items[i] +
                           '</a>';
                       n.appendChild(dv);
                   }
               }
               else {
                   err.innerText = "Documents not found."
                   $("#" + name + " #data").first().innerText = "";
                   $("#" + name + " #revisions").first().innerText = "";
               }
           });
    },

    page: function (i, tabname) {
        var m = models[tabname];
        m.page += i;
        if (m.page == 0) m.page = 1;
        if (m.page > m.pages) m.page = m.pages;
        hfview.search(tabname);
    },

    get: function (tabname, el) {
        var key = el.id;
        $.load("/raptordb/hfget?" + key,
            function (res) {
                $("#" + tabname + " #data").first().innerText = res;
            });
    },

    getkey: function (tabname) {
        var docid = $('#' + tabname + ' #docid').first().value;
        $.load("/raptordb/hfget?" + docid,
            function (res) {
                $("#" + tabname + " #data").first().innerText = res;
            });
    }

};


//---------------------------------------------------------------------
//----------------------------fileview----------------------------------
var fileview = {

    get: function (tabname) {
        var docid = $('#' + tabname + ' #docid').first().value;
        $.load("/raptordb/fileget?" + docid,
            function (res) {
                var err = $("#" + tabname + " #err").first();
                err.innerText = "";
                $("#" + tabname + " #data").first().innerText = res;
                var n = $("#" + tabname + " .disp").first();
                if (res == "") {
                    n.style.display = "none";
                    err.innerText = "fileid not found."
                }
                else {
                    n.style.display = "";
                    $.ajax("/raptordb/filehistory?" + docid,
                        function () {
                            $("#" + tabname + " #revisions").first().innerText = data.length;
                        });
                }
            });
    },

    getversion: function (tabname, el) {
        var ver = el.id;
        $.load("/raptordb/fileversion?" + ver,
            function (res) {
                $("#" + tabname + " #data").first().innerText = res;
            });
    },

    show: function (docid) {
        nav.loadtab("fileview", function (tabname) {
            $('#' + tabname + ' #docid').first().value = docid;
            fileview.get(tabname);
        });
    },

    fillhistory: function (name) {
        var docid = $('#' + name + ' #docid').first().value;
        $.ajax("/raptordb/filehistory?" + docid,
           function () {
               var n = $("#" + name + " #versions").first();
               n.innerHTML = "";
               var err = $("#" + name + " #err").first();
               err.innerText = "";
               if (data != "") {
                   $("#" + name + " #revisions").first().innerText = data.length;

                   for (var i = 0; i < data.length; i++) {
                       var dv = document.createElement("a");
                       dv.innerHTML = '<a href="" style="display:block" onclick="docview.getversion(\'' + name.trim() +
                           '\',this); event.preventDefault();" id="' + data[i].Version +
                           '">Id = ' + data[i].Version +
                           ', Date = ' + data[i].ChangeDate.replace("T", " ").replace("Z", "") +
                           '</a>';
                       n.appendChild(dv);
                   }
               }
               else {
                   err.innerText = "fileid not found."
                   $("#" + name + " #data").first().innerText = "";
                   $("#" + name + " #revisions").first().innerText = "";
               }
           });
    },

    history: function (tabname) {
        var docid = $('#' + tabname + ' #docid').first().value;
        // open new tab page
        nav.loadtab("filehistory", function (name) {
            $('#' + name + ' #docid').first().value = docid;
            fileview.fillhistory(name);
        });
    }

};

//------------------------------------------------------------------------------------------------------------------------
//----------------------------------------------- extensions -------------------------------------------------------------
(function () {
    $.murmur = function (str) {
        return murmurhash2_32_gc(str, 0xc58f1a7b);
    }

    function murmurhash2_32_gc(str, seed) {
        var
          l = str.length,
          h = seed ^ l,
          i = 0,
          k;

        while (l >= 4) {
            k =
              ((str.charCodeAt(i) & 0xff)) |
              ((str.charCodeAt(++i) & 0xff) << 8) |
              ((str.charCodeAt(++i) & 0xff) << 16) |
              ((str.charCodeAt(++i) & 0xff) << 24);

            k = (((k & 0xffff) * 0x5bd1e995) + ((((k >>> 16) * 0x5bd1e995) & 0xffff) << 16));
            k ^= k >>> 24;
            k = (((k & 0xffff) * 0x5bd1e995) + ((((k >>> 16) * 0x5bd1e995) & 0xffff) << 16));

            h = (((h & 0xffff) * 0x5bd1e995) + ((((h >>> 16) * 0x5bd1e995) & 0xffff) << 16)) ^ k;

            l -= 4;
            ++i;
        }

        switch (l) {
            case 3: h ^= (str.charCodeAt(i + 2) & 0xff) << 16;
            case 2: h ^= (str.charCodeAt(i + 1) & 0xff) << 8;
            case 1: h ^= (str.charCodeAt(i) & 0xff);
                h = (((h & 0xffff) * 0x5bd1e995) + ((((h >>> 16) * 0x5bd1e995) & 0xffff) << 16));
        }

        h ^= h >>> 13;
        h = (((h & 0xffff) * 0x5bd1e995) + ((((h >>> 16) * 0x5bd1e995) & 0xffff) << 16));
        h ^= h >>> 15;

        return h >>> 0;
    }
})();

(function () {
    // Private array of chars to use
    var CHARS = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz'.split('');

    Math.uuid = function (len, radix) {
        var chars = CHARS, uuid = [], i;
        radix = radix || chars.length;

        if (len) {
            // Compact form
            for (i = 0; i < len; i++) uuid[i] = chars[0 | Math.random() * radix];
        } else {
            // rfc4122, version 4 form
            var r;

            // rfc4122 requires these characters
            uuid[8] = uuid[13] = uuid[18] = uuid[23] = '-';
            uuid[14] = '4';

            // Fill in random data.  At i==19 set the high bits of clock sequence as
            // per rfc4122, sec. 4.1.5
            for (i = 0; i < 36; i++) {
                if (!uuid[i]) {
                    r = 0 | Math.random() * 16;
                    uuid[i] = chars[(i == 19) ? (r & 0x3) | 0x8 : r];
                }
            }
        }

        return uuid.join('');
    };

    // A more performant, but slightly bulkier, RFC4122v4 solution.  We boost performance
    // by minimizing calls to random()
    Math.uuidFast = function () {
        var chars = CHARS, uuid = new Array(36), rnd = 0, r;
        for (var i = 0; i < 36; i++) {
            if (i == 8 || i == 13 || i == 18 || i == 23) {
                uuid[i] = '-';
            } else if (i == 14) {
                uuid[i] = '4';
            } else {
                if (rnd <= 0x02) rnd = 0x2000000 + (Math.random() * 0x1000000) | 0;
                r = rnd & 0xf;
                rnd = rnd >> 4;
                uuid[i] = chars[(i == 19) ? (r & 0x3) | 0x8 : r];
            }
        }
        return uuid.join('');
    };

    // A more compact, but less performant, RFC4122v4 solution:
    Math.uuidCompact = function () {
        return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function (c) {
            var r = Math.random() * 16 | 0, v = c == 'x' ? r : (r & 0x3 | 0x8);
            return v.toString(16);
        });
    };
})();

var _tmplCache = {}
this.parseTemplate = function (str, data) {
    //    <script id="StockListTemplate" type="text/html">
    //      <# for(var i=0; i < stocks.length; i++)     
    //         {         
    //           var stock = stocks[i]; #>
    //           <div>
    //             <div><#= stock.company  #> ( <#= stock.symbol #>)</div>
    //             <div><#= stock.lastprice.formatNumber("c") #></div>
    //           </div>
    //      <# } #>
    //    </script>    
    //
    //     <script id="ItemTemplate" type="text/html">
    //         <div>
    //             <div><#= name #></div>
    //             <div><#= address.street #></div>
    //         </div>
    //     </script>
    // 
    //  usage : parseTemplate($("ItemTemplate").innerHTML, { name: "rick", address: { street: "32 kaiea", city: "paia"} } );
    // 
    var err = "";
    try {
        var func = _tmplCache[str];
        if (!func) {
            var strFunc =
            "var p=[],print=function(){p.push.apply(p,arguments);};" +
                        "with(obj){p.push('" +
            str.replace(/[\r\t\n]/g, " ")
               .replace(/'(?=[^#]*#>)/g, "\t")
               .split("'").join("\\'")
               .split("\t").join("'")
               .replace(/<#=(.+?)#>/g, "',$1,'")
               .split("<#").join("');")
               .split("#>").join("p.push('")
               + "');}return p.join('');";

            func = new Function("obj", strFunc);
            _tmplCache[str] = func;
        }
        return func(data);
    } catch (e) { err = e.message; }
    return "< # ERROR: " + err.htmlEncode() + " # >";
}



//------------------------------------------------------------------------------------------------------------------------
//----------------------------------------------- weldjs     -------------------------------------------------------------
//------------------------------------------------------------------------------------------------------------------------

; (function (exports) {
    // shim out Object.keys
    // ES5 15.2.3.14
    // http://whattheheadsaid.com/2010/10/a-safer-object-keys-compatibility-implementation
    if (!Object.keys) {

        var hasDontEnumBug = true,
        dontEnums = [
          'toString',
          'toLocaleString',
          'valueOf',
          'hasOwnProperty',
          'isPrototypeOf',
          'propertyIsEnumerable',
          'constructor'
        ],
        dontEnumsLength = dontEnums.length;

        for (var key in { "toString": null }) {
            hasDontEnumBug = false;
        }
        Object.keys = function keys(object) {

            if (typeof object !== "object" &&
                typeof object !== "function" ||
                object === null) {
                throw new TypeError("Object.keys called on a non-object");
            }

            var keys = [];
            for (var name in object) {
                if (object.hasOwnProperty(name)) {
                    keys.push(name);
                }
            }

            if (hasDontEnumBug) {
                for (var i = 0, ii = dontEnumsLength; i < ii; i++) {
                    var dontEnum = dontEnums[i];
                    if (object.hasOwnProperty(dontEnum)) {
                        keys.push(dontEnum);
                    }
                }
            }
            return keys;
        };
    }


    /*  Let us play nice with IE
     *  https://developer.mozilla.org/en/JavaScript/Reference/Global_Objects/Array/indexOf#Compatibility
     */
    if (!Array.prototype.indexOf) {
        Array.prototype.indexOf = function (searchElement /*, fromIndex */) {
            "use strict";
            if (this == null) {
                throw new TypeError();
            }
            var t = Object(this);
            var len = t.length >>> 0;
            if (len === 0) {
                return -1;
            }
            var n = 0;
            if (arguments.length > 0) {
                n = Number(arguments[1]);
                if (n != n) { // shortcut for verifying if it's NaN
                    n = 0;
                } else if (n != 0 && n != Infinity && n != -Infinity) {
                    n = (n > 0 || -1) * Math.floor(Math.abs(n));
                }
            }
            if (n >= len) {
                return -1;
            }
            var k = n >= 0 ? n : Math.max(len - Math.abs(n), 0);
            for (; k < len; k++) {
                if (k in t && t[k] === searchElement) {
                    return k;
                }
            }
            return -1;
        }
    }


    /*  // Start: DEBUGGING
     // ----------------
   
     /*  Since weld runs browser/server, ensure there is a console implementation.
      */

    var logger = (typeof console === 'undefined') ? { log: function () { } } : console;
    var nodejs = false;

    if (typeof process !== 'undefined' && process.title) {
        nodejs = true;
    }

    var color = {
        gray: '\033[37m',
        darkgray: '\033[40;30m',
        red: '\033[31m',
        green: '\033[32m',
        yellow: '\033[33m',
        lightblue: '\033[1;34m',
        cyan: '\033[36m',
        white: '\033[1;37m'
    };

    var inputRegex = /input|select|option|button/i;
    var imageRegex = /img/i;
    var textareaRegex = /textarea/i;
    var truthyRegex = /yes|true|1|ok/i;
    var depth = 0;  // The current depth of the traversal, used for debugging.
    var successIndicator = nodejs ? (color.green + ' ?' + color.gray) : ' Success';
    var failureIndicator = nodejs ? (color.red + ' ?' + color.gray) : ' Fail';

    var debuggable = function debuggable(name, operation) {

        var label = name.toUpperCase();

        // All of the ops have the same signature, so this is sane.
        return function (parent, element, key, value) {
            logger.log(
              pad(),
              ((nodejs ? (color.gray + '+ ') : '+ ') + label + ' -'),
              'parent:', colorize(parent) + ',',
              'element:', colorize(element) + ',',
              'key:', colorize(key) + ',',
              'value:', colorize(value)
            );

            depth += 1;

            if (operation) {
                var res = operation(parent, element, key, value);
                depth -= 1;
                logger.log(pad(), (nodejs ? '+ ' : '+ ') + element + '' + (res !== false ? successIndicator : failureIndicator));
                return res;
            }

            depth -= 1;
            d('- OPERATION NOT FOUND: ', label);
        };
    };

    /*  Generates padding used for indenting debugger statements.
     */

    var pad = function pad() {
        var l = depth, ret = '';
        while (l--) {
            ret += nodejs ? ' ¦   ' : ' |   ';
        }
        return ret;
    };

    /*  Debugger statement, terse, accepts any number of arguments
     *  that are passed to a logger.log statement.
     */

    var d = function d() {
        var args = Array.prototype.slice.call(arguments);

        // This is done because on the browser you cannot call console.log.apply
        logger.log(pad(), args.join(' '));
    };

    var colorize = function colorize(val) {
        var sval = val + '', u = 'undefined';
        if (nodejs) {
            if (sval === 'false' || sval === 'null' || sval === '' || sval === u || typeof val === u || val === false) {
                if (sval === '') { sval = '(empty string)' };
                return color.red + sval + color.gray;
            }
            else {
                return color.yellow + sval + color.gray;
            }
        }
        return sval;
    };

    // End: DEBUGGING
    // -------------- */


    /*  Weld!
     *  @param {HTMLElement} DOMTarget
     *    The target html node that will be used as the subject of data binding.
     *  @param {Object|Array} data
     *    The data that will be used.
     *  @param {Object} pconfig
     *    The configuration object.
     */
    exports.weld = function weld(DOMTarget, data, pconfig) {

        var parent = DOMTarget.parentNode;
        var currentOpKey, p, fn, debug;

        /*
         *  Configuration Object.
         *  @member {Object}
         *    Contains an explicit mapping of data-keys to element name/id/classes
         *  @member {Boolean}
         *    Determines if debugging will be enabled.
         *  @method {Boolean|Function}
         *    Determines the method of insertion, can be a functon or false.
         */

        var config = {
            alias: {},
            debug: false,
            insert: false // Default to append
        };

        // Merge the user configuration over the existing config
        if (pconfig) {
            for (p in pconfig) {
                if (pconfig.hasOwnProperty(p)) {
                    config[p] = pconfig[p];
                }
            }
        }

        debug = config.debug;

        /*  An interface to the interal operations, implements common
         *  debugging output based on a standard set of parameters.
         *
         *  @param {Function} operation
         *    The function to call in "debug mode"
         */

        var ops = {
            siblings: function siblings(parent, element, key, value) {
                var remove = [],
                sibling,
                classes,
                cc,
                match,
                siblings = parent.children;
                cs = siblings.length; // Current Sibling
                element.weld = {
                    parent: parent,
                    classes: element.className.split(' ')
                };

                // Find all siblings that match the exact classes that exist in the originally
                // matched element node
                while (cs--) {
                    sibling = siblings[cs];

                    if (sibling === element) {
                        // If this is not the last item in the list, store where new items should be inserted
                        if (cs < siblings.length) {
                            element.weld.insertBefore = siblings[cs + 1];
                        }

                        // remove the element here because siblings is a live list.
                        // which means, if you remove it before hand, the length will mismatch and cause problems
                        if (debug) {
                            d('- REMOVE - element:', colorize(element), 'class:', colorize(element.className), 'id:', colorize(element.id));
                        }

                        parent.removeChild(element);

                        // Check for the same class
                    } else {
                        classes = sibling.className.split(' ');
                        cc = classes.length;
                        match = true;
                        while (cc--) {
                            // TODO: optimize
                            if (element.weld.classes.indexOf(classes[cc]) < 0) {
                                match = false;
                                break;
                            }
                        }

                        // This element matched, you win a prize! DIE.
                        if (match) {
                            if (debug) {
                                d('- REMOVE - element:', colorize(sibling), 'class:', colorize(sibling.className), 'id:', colorize(sibling.id));
                            }
                            parent.removeChild(sibling);
                        }
                    }
                }
            },
            traverse: function traverse(parent, element, key, value, row) {

                var type, target, i, keys, l, obj;
                var template = element;
                var templateParent = element.parentNode;

                // LEAF

                if (~({}).toString.call(value).indexOf('Date')) {
                    value = value.toString();
                }

                if (value.nodeType || typeof value !== 'object') {
                    ops.set(parent, element, key, value, row);

                    // ARRAY / NodeList
                } else if (value.length && value[0]) {
                    if (templateParent) {
                        ops.siblings(templateParent, template, key, value);
                    } else if (template.weld && template.weld.parent) {
                        templateParent = template.weld.parent;
                    }

                    l = value.length;
                    for (i = 0; i < l; i++) {
                        if (debug) {
                            d('- CLONE - element:', colorize(element), 'class:', colorize(element.className), 'id:', colorize(element.id));
                        }
                        target = element.cloneNode(true);
                        target.weld = {};

                        // Clone weld params
                        if (element.weld) {
                            var keys = Object.keys(element.weld), currentKey = keys.length, weldParam;
                            while (currentKey--) {
                                weldParam = keys[currentKey];
                                target.weld[weldParam] = element.weld[weldParam];
                            }
                        }
                        ops.traverse(templateParent, target, i, value[i], value[i]);
                        ops.insert(templateParent, target, i, value[i]);
                    }

                    // OBJECT
                } else {
                    var keys = Object.keys(value), current = keys.length, obj;
                    while (current--) {
                        var lkey = keys[current];
                        obj = value[lkey];
                        target = ops.match(template, element, lkey, obj);

                        if (target) {
                            ops.traverse(template, target, lkey, obj, row);

                            // Handle the case where a parent data key doesn't
                            // match a dom node, but the child data object may.
                            // don't continue traversing if the child data object
                            // is not an array/object
                        }
                        // mg error here --------------------------------------------------------------------------------------------

                        //else if (target !== false &&
                        //           typeof obj === 'object' &&
                        //           Object.keys(obj).length > 0) // TODO: optimize
                        //{
                        //  ops.traverse(templateParent, template, lkey, obj,row);
                        //}
                    }
                }
            },
            elementType: function elementType(parent, element, key, value) {
                if (element) {
                    var nodeName = element.nodeName;

                    if (typeof nodeName === "string") {
                        if (inputRegex.test(nodeName)) {
                            return 'input';
                        }

                        if (imageRegex.test(nodeName)) {
                            return 'image';
                        }

                        if (textareaRegex.test(nodeName)) {
                            return 'textarea';
                        }
                    }
                }
            },
            map: false, // this is a user-defined operation
            insert: function (parent, element) {
                // Insert the template back into document
                if (element.weld && element.weld.insertBefore) {
                    parent.insertBefore(element, element.weld.insertBefore);
                } else {
                    parent.appendChild(element);
                }
            },
            set: function set(parent, element, key, value, row) {

                if (ops.map && ops.map(parent, element, key, value, row) === false) {
                    return false;
                }

                if (debug) {
                    d('- SET: value is', value.tagName);
                }

                var type = ops.elementType(parent, element, key, value), res = false;

                if (value && value.nodeType) { // imports.

                    if (element.ownerDocument !== value.ownerDocument) {
                        value = element.ownerDocument.importNode(value, true);
                    } else if (value.parentNode) {
                        value.parentNode.removeChild(value);
                    }

                    while (element.firstChild) { // clean first.
                        element.removeChild(element.firstChild);
                    }

                    element.appendChild(value);
                    res = true;
                }
                else if (type === 'input') { // special cases.
                    if (element.tagName.toLowerCase() === 'select') {
                        element.value = value;
                    } else if (element.type.toLowerCase() === 'checkbox') {
                        if (truthyRegex.test(value)) {
                            element.setAttribute('checked', true);
                        } else if (element.hasAttribute('checked')) {
                            element.removeAttribute('checked');
                        }
                    } else {
                        element.setAttribute('value', value);
                    }
                    res = true;
                }
                else if (type === 'image') {
                    element.setAttribute('src', value);
                    res = true;
                }
                else if (type === 'textarea') {
                    element.textContent = value;
                    if (element.value !== value) {
                        // Here's looking at you Opera.
                        element.value = value;
                    }
                    res = true;
                }
                else { // simple text assignment.
                    if (!isNaN(value))
                        element.textContent = value.toLocaleString();
                    else
                        element.textContent = value;
                    res = true;
                }
                return res;
            },
            match: function match(parent, element, key, value) {
                if (typeof config.alias[key] !== 'undefined') {
                    if (typeof config.alias[key] === 'function') {
                        key = config.alias[key](parent, element, key, value) || key;
                    }
                    else if (config.alias[key] === false) {
                        return false;
                    }
                    else {
                        key = config.alias[key];
                    }
                }

                // Alias can be a node, for explicit binding.
                // Alias can also be a method that returns a string or DOMElement
                if (key && key.nodeType) {
                    return key;
                }

                if (element) {
                    if (element.querySelector) {
                        return element.querySelector('.' + key + ',#' + key + ',[name="' + key + '"]');
                    }
                    else {
                        var els = element.getElementsByTagName('*'), l = els.length, e, i;
                        // find the _first_ best match
                        for (i = 0; i < l; i++) {
                            e = els[i];
                            if (e.id === key || e.name === key || e.className.split(' ').indexOf(key) > -1) {
                                return e;
                            }
                        }
                    }
                }
            }
        };


        // Allow the caller to overwrite the internals of weld
        for (currentOpKey in ops) {
            if (ops.hasOwnProperty(currentOpKey)) {
                currentOp = ops[currentOpKey];
                fn = config[currentOpKey] || ops[currentOpKey];

                if (debug) {
                    fn = debuggable(currentOpKey, fn);
                }

                ops[currentOpKey] = fn;
            }
        }

        // Kick it off
        ops.traverse(null, DOMTarget, null, data);

        if (config.debug) {
            logger.log(DOMTarget.outerHTML);
        }
    };

    if (typeof exports.define !== "undefined" && exports.define.amd) {
        define('weld', [], function () { return window.weld });
    }

}(typeof process !== 'undefined' && typeof process.title !== 'undefined' ? exports : window));