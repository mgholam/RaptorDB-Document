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
