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

