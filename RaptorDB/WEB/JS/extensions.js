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
};

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
};

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
};

$.closemodal = function () {
    var d = $("openModal");
    if (d != null) {
        document.body.removeChild(d);
        document.onkeypress = null;
        document.location = "#";
    }
};

$.showDialog = function (caption, innerhtml) {
    var body = document.body;
    var d = document.createElement('div');
    d.id = "openModal";
    d.className = "modalDialog";
    d.innerHTML = '<div><a onclick="$.closemodal(); event.preventDefault();" title="Close" class="close">X</a><h2>' + caption + '</h2>' + innerhtml + '</div>';
    body.appendChild(d);
    document.location = "#openModal";
    document.onkeypress = function () {
        $.closemodal();
    };
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
}());

(function () {
    // Private array of chars to use
    var CHARS = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz'.split('');
    $.uuid = function (len, radix) {
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
    $.uuidFast = function () {
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
    $.uuidCompact = function () {
        return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function (c) {
            var r = Math.random() * 16 | 0, v = c == 'x' ? r : (r & 0x3 | 0x8);
            return v.toString(16);
        });
    };
}());

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
            "var p=[],print=function(){p.push.apply(p,arguments);};with(obj){p.push('" +
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
