//-----------------------------------------------------------------------------------------------------------
//------------------------------------------ query  ---------------------------------------------------------
var schema = {
    schemachanged: function (element) {
        var tabname = element.parentElement.id;
        this.schemaselected(tabname);
    },

    schemaselected: function (tabname) {
        var x = $("#" + tabname + " #viewsel").first().value;
        models[tabname].vname = x;
        $.load(server + 'pages/schemadata.html', function (res) {
            $(".tab-content#" + tabname + " #schema").first().innerHTML = res.replace(/\$tabname/g, tabname);
            // fill schema
            $.ajax(server + 'RaptorDB/viewinfo?' + x,
                function () {
                    weld($(".tab-content#" + tabname + " .View").first(), data.View);
                    var def = $(".tab-content#" + tabname + " .schema_def").first();
                    var str = "<th>Column Name</th><th>Type</th>";
                    for (var p in data.Schema) {
                        str += "<tr><td>" + p + "</td><td>" + data.Schema[p] + "</td></tr>";
                    }
                    def.innerHTML = str;
                });
        });

    },

    hookup: function (tabname) {
        var q = this;
        $.ajax(server + 'RaptorDB/GetViews',
            function () {
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
                };
                schema.schemaselected(tabname);
            });
    },
};


