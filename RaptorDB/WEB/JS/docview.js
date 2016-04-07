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
                $("#" + tabname + " #docid").first().value = ver;
            });
    },

    getsearch: function (tabname, el) {
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
        var docid = $('#' + name + ' #search').first().value;
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
                       dv.innerHTML = '<a href="" style="display:block" onclick="docview.getsearch(\'' + name.trim() +
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