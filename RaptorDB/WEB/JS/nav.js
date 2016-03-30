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
            //console.log(id);
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
                $.load(server + "pages/query.html",
                    function (res) {
                        var tname = "query" + $.uuid(4, 16);
                        models[tname] = new modelobj();
                        nav.createTab(tname, "Query", res.replace(/\$tabname/g, tname));
                        query.hookup(tname);
                        if (func != null)
                            func(tname);
                    });
                break;

            case "sysinfo":
                $.load("pages/systeminfo.html",
                    function (res) {
                        var tname = "info" + $.uuid(4, 16);
                        nav.createTab(tname, "System Information", res.replace(/\$tabname/g, tname));
                        system.run(tname);
                        if (func != null)
                            func(tname);
                    });
                break;

            case "sysconfig":
                $.load("pages/configs.html",
                    function (res) {
                        var tname = "conf" + $.uuid(4, 16);
                        nav.createTab(tname, "System Configs", res.replace(/\$tabname/g, tname));
                        system.configs(tname);
                        if (func != null)
                            func(tname);
                    });
                break;

            case "ssquery":
                this.createTab("tab" + $.uuid(4, 16), "Server Side", "<p>" + new Date() + "</p>");
                break;

            case "docview":
                $.load("pages/docview.html",
                    function (res) {
                        var tname = "docv" + $.uuid(4, 16);
                        nav.createTab(tname, "Document View", res.replace(/\$tabname/g, tname));
                        if (func != null)
                            func(tname);
                    });
                break;

            case "dochistory":
                $.load("pages/dochistory.html",
                    function (res) {
                        var tname = "doch" + $.uuid(4, 16);
                        nav.createTab(tname, "Document History", res.replace(/\$tabname/g, tname));
                        if (func != null)
                            func(tname);
                    });
                break;

            case "docsearch":
                $.load("pages/docsearch.html",
                    function (res) {
                        var tname = "doch" + $.uuid(4, 16);
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
                $.load("pages/hfbrowse.html",
                    function (res) {
                        var tname = "hfv" + $.uuid(4, 16);
                        models[tname] = new modelobj();
                        var m = models[tname];
                        m.count = 20;
                        m.start = 0;
                        m.page = 1;
                        m.pages = 1;
                        nav.createTab(tname, "HF Browse", res.replace(/\$tabname/g, tname));
                        hfview.search(tname);
                        if (func != null)
                            func(tname);
                    });
                break;

            case "fileview":
                $.load("pages/fileview.html",
                    function (res) {
                        var tname = "filev" + $.uuid(4, 16);
                        nav.createTab(tname, "File View", res.replace(/\$tabname/g, tname));
                        if (func != null)
                            func(tname);
                    });
                break;

            case "schema":
                $.load("pages/schema.html",
                    function (res) {
                        var tname = "schm" + $.uuid(4, 16);
                        models[tname] = new modelobj();
                        nav.createTab(tname, "Schema", res.replace(/\$tabname/g, tname));
                        schema.hookup(tname);
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
