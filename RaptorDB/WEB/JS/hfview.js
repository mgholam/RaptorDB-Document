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
                //$("#" + tabname + " #docid").first().value = key;
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