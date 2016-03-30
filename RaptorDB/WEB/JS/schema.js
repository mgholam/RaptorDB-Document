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

    hookup: function (tabname) {
        var q = this;
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
        });
    },
};


